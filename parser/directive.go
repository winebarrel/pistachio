package parser

import (
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

var renameDirectivePattern = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pist:rename-from[ \t]+(.+?)[ \t]*$`)

// QualifyRenameFrom qualifies a rename-from value with the default schema
// if it does not already contain a schema (dot separator).
func QualifyRenameFrom(value, defaultSchema string) string {
	if strings.Contains(value, ".") {
		return value
	}
	return defaultSchema + "." + value
}

// unquoteIdent strips surrounding double quotes from a SQL identifier and
// unescapes doubled double-quotes ("" → "). Returns the input unchanged
// if it is not quoted.
func unquoteIdent(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		inner := s[1 : len(s)-1]
		return strings.ReplaceAll(inner, `""`, `"`)
	}
	return s
}

// normalizeDirectiveValue normalizes a rename-from directive value by
// unquoting each part of a potentially schema-qualified name.
// e.g. `"My Schema"."Old Name"` → `My Schema.Old Name`
//
//	`public.old_name`       → `public.old_name` (unchanged)
//	`"Old Name"`            → `Old Name`
func normalizeDirectiveValue(s string) string {
	// Split on dots, but respect quoted identifiers
	parts := splitQualifiedName(s)
	for i, p := range parts {
		parts[i] = unquoteIdent(p)
	}
	return strings.Join(parts, ".")
}

// splitQualifiedName splits a potentially schema-qualified name into parts,
// respecting quoted identifiers. e.g. `"My Schema"."Old Name"` → [`"My Schema"`, `"Old Name"`]
func splitQualifiedName(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '"' {
			if inQuote && i+1 < len(s) && s[i+1] == '"' {
				// Escaped double quote
				current.WriteByte('"')
				current.WriteByte('"')
				i++
			} else {
				inQuote = !inQuote
				current.WriteByte(ch)
			}
		} else if ch == '.' && !inQuote {
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

// ExtractStmtDirectives scans raw SQL for `-- pist:rename-from <name>` comments
// that appear in each statement's raw text region (including leading comments).
// pg_query includes leading comments in StmtLocation/StmtLen, so we scan the
// raw text of each statement for the directive.
// Returns a map from StmtLocation to the old name string.
func ExtractStmtDirectives(rawSQL string, stmts []*pg_query.RawStmt) map[int32]string {
	directives := make(map[int32]string)

	for _, stmt := range stmts {
		loc := stmt.StmtLocation
		end := loc + stmt.StmtLen
		if end > int32(len(rawSQL)) {
			end = int32(len(rawSQL))
		}

		region := rawSQL[loc:end]

		// Only scan the leading comment block before the actual SQL keyword.
		// Find where the first non-comment, non-whitespace content starts.
		leadingEnd := findLeadingCommentEnd(region)
		leading := region[:leadingEnd]

		matches := renameDirectivePattern.FindAllStringSubmatch(leading, -1)
		if len(matches) > 0 {
			// Use the last match (closest to the actual SQL statement)
			directives[loc] = normalizeDirectiveValue(matches[len(matches)-1][1])
		}
	}

	return directives
}

// findLeadingCommentEnd returns the byte offset where leading comments/whitespace
// end and the actual SQL statement begins.
func findLeadingCommentEnd(s string) int {
	lines := strings.Split(s, "\n")
	offset := 0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			offset += len(line) + 1 // +1 for \n
		} else {
			break
		}
	}
	if offset > len(s) {
		offset = len(s)
	}
	return offset
}

// InlineDirectives holds rename directives for columns and constraints within a CREATE TABLE.
type InlineDirectives struct {
	Columns     map[string]string // new column name → old column name
	Constraints map[string]string // new constraint name → old constraint name
}

// ExtractInlineDirectives scans the raw text of a CREATE TABLE statement for
// `-- pist:rename-from <old_name>` directives that appear on lines immediately
// before column or constraint definitions.
func ExtractInlineDirectives(rawCreateTableSQL string) *InlineDirectives {
	result := &InlineDirectives{
		Columns:     make(map[string]string),
		Constraints: make(map[string]string),
	}
	lines := strings.Split(rawCreateTableSQL, "\n")

	var pendingRename string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if m := renameDirectivePattern.FindStringSubmatch(line); m != nil {
			pendingRename = normalizeDirectiveValue(m[1])
			continue
		}

		if pendingRename != "" && trimmed != "" && !strings.HasPrefix(trimmed, "--") {
			upper := strings.ToUpper(trimmed)
			if strings.HasPrefix(upper, "CONSTRAINT ") {
				conName := extractConstraintName(trimmed)
				if conName != "" {
					result.Constraints[conName] = pendingRename
				}
			} else {
				colName := extractColumnName(trimmed)
				if colName != "" {
					result.Columns[colName] = pendingRename
				}
			}
			pendingRename = ""
		} else if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			// Skip blank lines and other comments, keep pending
		} else {
			pendingRename = ""
		}
	}

	return result
}

// ExtractColumnDirectives is a convenience wrapper for backward compatibility.
func ExtractColumnDirectives(rawCreateTableSQL string) map[string]string {
	return ExtractInlineDirectives(rawCreateTableSQL).Columns
}

// extractConstraintName extracts the constraint name from a CONSTRAINT line.
// e.g. "CONSTRAINT users_pkey PRIMARY KEY (id)" → "users_pkey"
func extractConstraintName(line string) string {
	line = strings.TrimSpace(line)
	upper := strings.ToUpper(line)
	if !strings.HasPrefix(upper, "CONSTRAINT ") {
		return ""
	}
	rest := strings.TrimSpace(line[len("CONSTRAINT "):])
	if strings.HasPrefix(rest, `"`) {
		end := strings.Index(rest[1:], `"`)
		if end >= 0 {
			return rest[1 : end+1]
		}
		return ""
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

// extractColumnName extracts the column name from a column definition line.
// Handles both unquoted identifiers and quoted identifiers ("My Column").
func extractColumnName(line string) string {
	line = strings.TrimSpace(line)

	// Skip CONSTRAINT lines
	upper := strings.ToUpper(line)
	if strings.HasPrefix(upper, "CONSTRAINT ") {
		return ""
	}

	if strings.HasPrefix(line, `"`) {
		// Quoted identifier
		end := strings.Index(line[1:], `"`)
		if end >= 0 {
			return line[1 : end+1]
		}
		return ""
	}

	// Unquoted identifier: first word
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ""
	}

	name := fields[0]
	// Remove trailing comma if present
	name = strings.TrimSuffix(name, ",")
	return name
}
