package parser

import (
	"fmt"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/pistachio/model"
)

var (
	renameDirectivePattern       = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:renamed-from[ \t]+(.+?)[ \t]*$`)
	executeDirectivePattern      = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:execute(?:[ \t]+(.+?))?[ \t]*$`)
	concurrentlyDirectivePattern = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:concurrently[ \t]*$`)
	// Matches -- pista:concurrently with trailing content (invalid usage).
	concurrentlyWithArgsPattern = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:concurrently[ \t]+\S`)
	bulkAlterDirectivePattern   = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:bulk-alter[ \t]*$`)
	// Matches -- pista:bulk-alter with trailing content (invalid usage).
	bulkAlterWithArgsPattern = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:bulk-alter[ \t]+\S`)
	ignoreDirectivePattern   = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:ignore[ \t]*$`)
	// Matches -- pista:ignore with trailing content (invalid usage).
	ignoreWithArgsPattern = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:ignore[ \t]+\S`)
	// Matches any -- pista: directive, capturing the name (if any) after the colon.
	anyDirectivePattern = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:[ \t]*(\S*)`)
)

// knownDirectives lists all recognized directive names.
var knownDirectives = map[string]bool{
	"renamed-from": true,
	"execute":      true,
	"concurrently": true,
	"bulk-alter":   true,
	"ignore":       true,
}

// validateDirectives checks for unknown -- pista: directives in the raw SQL
// and returns an error if any are found.
func validateDirectives(rawSQL string) error {
	matches := anyDirectivePattern.FindAllStringSubmatch(rawSQL, -1)
	for _, m := range matches {
		name := strings.TrimSpace(m[1])
		if name == "" {
			return fmt.Errorf("invalid directive: -- pista: (missing directive name)")
		}
		if !knownDirectives[name] {
			return fmt.Errorf("unknown directive: -- pista:%s", name)
		}
	}

	if concurrentlyWithArgsPattern.MatchString(rawSQL) {
		return fmt.Errorf("-- pista:concurrently does not accept arguments")
	}

	if bulkAlterWithArgsPattern.MatchString(rawSQL) {
		return fmt.Errorf("-- pista:bulk-alter does not accept arguments")
	}

	if ignoreWithArgsPattern.MatchString(rawSQL) {
		return fmt.Errorf("-- pista:ignore does not accept arguments")
	}

	return nil
}

// ExecuteStmt represents an arbitrary SQL statement marked with -- pista:execute.
type ExecuteStmt struct {
	SQL      string // The SQL statement to execute
	CheckSQL string // Optional condition check SQL (empty = always execute)
}

// extractExecuteDirectives scans raw SQL for `-- pista:execute [<check SQL>]`
// comments and pairs them with the following SQL statement.
// Returns the execute statements and a set of statement locations to skip
// during normal parsing.
func extractExecuteDirectives(rawSQL string, stmts []*pg_query.RawStmt) ([]*ExecuteStmt, map[int32]bool, error) {
	var executeStmts []*ExecuteStmt
	skipLocations := make(map[int32]bool)

	for _, stmt := range stmts {
		loc := stmt.StmtLocation
		end := loc + stmt.StmtLen
		if end > int32(len(rawSQL)) {
			end = int32(len(rawSQL))
		}

		region := rawSQL[loc:end]
		leadingEnd := findLeadingCommentEnd(region)
		leading := region[:leadingEnd]

		matches := executeDirectivePattern.FindAllStringSubmatch(leading, -1)
		if len(matches) == 0 {
			continue
		}

		// Deparse the statement to get canonical SQL
		deparsed, err := pg_query.Deparse(&pg_query.ParseResult{
			Stmts: []*pg_query.RawStmt{stmt},
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to deparse execute statement: %w", err)
		}

		// Use the last match (closest to the actual SQL statement)
		lastMatch := matches[len(matches)-1]
		checkSQL := ""
		if len(lastMatch) > 1 {
			checkSQL = strings.TrimSpace(lastMatch[1])
			// Remove trailing semicolons; pgx extended protocol doesn't allow them
			checkSQL = strings.TrimRight(checkSQL, ";")
			checkSQL = strings.TrimSpace(checkSQL)
		}

		executeStmts = append(executeStmts, &ExecuteStmt{
			SQL:      deparsed,
			CheckSQL: checkSQL,
		})
		skipLocations[loc] = true
	}

	return executeStmts, skipLocations, nil
}

// FormatExecuteStmt formats an ExecuteStmt as SQL with the directive comment.
func FormatExecuteStmt(es *ExecuteStmt) string {
	directive := "-- pista:execute"
	if es.CheckSQL != "" {
		directive += " " + es.CheckSQL
	}
	sql := strings.TrimRight(es.SQL, " \t\r\n")
	if !strings.HasSuffix(sql, ";") {
		sql += ";"
	}
	return fmt.Sprintf("%s\n%s", directive, sql)
}

// qualifyRenameFrom qualifies a renamed-from value with the default schema
// if it does not already contain a schema. Quoted identifiers containing
// dots (e.g. `"a.b"`) are treated as a single identifier.
func qualifyRenameFrom(value, defaultSchema string) string {
	parts := splitQualifiedName(value)
	for i, p := range parts {
		parts[i] = unquoteIdent(p)
	}
	if len(parts) >= 2 {
		return model.Ident(parts...)
	}
	return model.Ident(defaultSchema, parts[0])
}

// unquoteIdent strips surrounding double quotes from a SQL identifier and
// unescapes doubled double-quotes ("" -> "). For unquoted identifiers,
// folds to lowercase to match PostgreSQL's behavior.
func unquoteIdent(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		inner := s[1 : len(s)-1]
		return strings.ReplaceAll(inner, `""`, `"`)
	}
	return strings.ToLower(s)
}

// normalizeUnqualifiedDirective normalizes a renamed-from directive value
// for unqualified names (columns, constraints, indexes, foreign keys)
// by unquoting the identifier. If a schema-qualified name is provided
// (e.g. "public.old_idx"), only the last part is used.
// The result matches the unquoted name used as orderedmap keys by the parser.
func normalizeUnqualifiedDirective(s string) string {
	parts := splitQualifiedName(s)
	// Use the last part (the actual name, ignoring any schema prefix)
	last := parts[len(parts)-1]
	return unquoteIdent(last)
}

// splitQualifiedName splits a potentially schema-qualified name into parts,
// respecting quoted identifiers. e.g. `"My Schema"."Old Name"` -> [`"My Schema"`, `"Old Name"`]
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
			parts = append(parts, strings.TrimSpace(current.String()))
			current.Reset()
		} else {
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}
	return parts
}

// extractStmtDirectives scans raw SQL for `-- pista:renamed-from <name>` comments
// that appear in each statement's raw text region (including leading comments).
// pg_query includes leading comments in StmtLocation/StmtLen, so we scan the
// raw text of each statement for the directive.
// Returns a map from StmtLocation to the old name string.
func extractStmtDirectives(rawSQL string, stmts []*pg_query.RawStmt) map[int32]string {
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
			renameFrom := strings.TrimSpace(matches[len(matches)-1][1])
			if renameFrom != "" {
				directives[loc] = renameFrom
			}
		}
	}

	return directives
}

// extractConcurrentlyDirectives scans raw SQL for `-- pista:concurrently` comments
// that appear in each statement's leading comment region.
// Returns a set of StmtLocations that have the directive.
func extractConcurrentlyDirectives(rawSQL string, stmts []*pg_query.RawStmt) map[int32]bool {
	return extractFlagDirectives(concurrentlyDirectivePattern, rawSQL, stmts)
}

// extractBulkAlterDirectives scans raw SQL for `-- pista:bulk-alter` comments
// that appear in each statement's leading comment region.
// Returns a set of StmtLocations that have the directive.
func extractBulkAlterDirectives(rawSQL string, stmts []*pg_query.RawStmt) map[int32]bool {
	return extractFlagDirectives(bulkAlterDirectivePattern, rawSQL, stmts)
}

// extractIgnoreDirectives scans raw SQL for `-- pista:ignore` comments that
// appear in each statement's leading comment region.
// Returns a set of StmtLocations that have the directive.
func extractIgnoreDirectives(rawSQL string, stmts []*pg_query.RawStmt) map[int32]bool {
	return extractFlagDirectives(ignoreDirectivePattern, rawSQL, stmts)
}

// extractFlagDirectives scans raw SQL for argument-less directive comments
// matching pattern in each statement's leading comment region.
// Returns a set of StmtLocations that have the directive.
func extractFlagDirectives(pattern *regexp.Regexp, rawSQL string, stmts []*pg_query.RawStmt) map[int32]bool {
	directives := make(map[int32]bool)

	for _, stmt := range stmts {
		loc := stmt.StmtLocation
		end := loc + stmt.StmtLen
		if end > int32(len(rawSQL)) {
			end = int32(len(rawSQL))
		}

		region := rawSQL[loc:end]
		leadingEnd := findLeadingCommentEnd(region)
		leading := region[:leadingEnd]

		if pattern.MatchString(leading) {
			directives[loc] = true
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

// inlineDirectives holds rename directives for columns and constraints within a CREATE TABLE.
type inlineDirectives struct {
	Columns     map[string]string // new column name -> old column name
	Constraints map[string]string // new constraint name -> old constraint name
}

// extractInlineDirectives scans the raw text of a CREATE TABLE statement for
// `-- pista:renamed-from <old_name>` directives that appear on lines immediately
// before column or constraint definitions.
func extractInlineDirectives(rawCreateTableSQL string) *inlineDirectives {
	result := &inlineDirectives{
		Columns:     make(map[string]string),
		Constraints: make(map[string]string),
	}

	// Only scan lines inside the column/constraint list (after the opening parenthesis)
	parenIdx := strings.Index(rawCreateTableSQL, "(")
	if parenIdx < 0 {
		return result
	}
	body := rawCreateTableSQL[parenIdx:]
	lines := strings.Split(body, "\n")

	var pendingRename string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if m := renameDirectivePattern.FindStringSubmatch(line); m != nil {
			pendingRename = normalizeUnqualifiedDirective(m[1])
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

// extractEnumValueDirectives scans a CREATE TYPE ... AS ENUM statement for
// `-- pista:renamed-from <old_value>` directives on the line before a value
// literal. The statement is tokenized with the pg_query lexer, so any literal
// form (escapes, dollar quoting, multi-line) is recognized. The old value may
// be written as a quoted literal or bare. Returns a map from value index to
// old value.
func extractEnumValueDirectives(rawCreateEnumSQL string) (map[int]string, error) {
	result := make(map[int]string)

	scan, err := pg_query.Scan(rawCreateEnumSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to scan CREATE TYPE statement: %w", err)
	}

	inValueList := false
	pending := ""
	hasPending := false
	valueIdx := 0

	for _, tok := range scan.Tokens {
		switch tok.Token {
		case pg_query.Token_ASCII_40: // "("
			// Directives before the value list rename the type, not a value.
			inValueList = true
		case pg_query.Token_SQL_COMMENT:
			if !inValueList || !atLineStart(rawCreateEnumSQL, int(tok.Start)) {
				continue
			}
			text := rawCreateEnumSQL[tok.Start:tok.End]
			if m := renameDirectivePattern.FindStringSubmatch(text); m != nil {
				pending = unquoteEnumLiteral(strings.TrimSpace(m[1]))
				hasPending = true
			}
		case pg_query.Token_SCONST, pg_query.Token_USCONST:
			if hasPending {
				result[valueIdx] = pending
				hasPending = false
			}
			valueIdx++
		}
	}

	return result, nil
}

// atLineStart reports whether only whitespace precedes pos on its line.
func atLineStart(s string, pos int) bool {
	for i := pos - 1; i >= 0; i-- {
		switch s[i] {
		case '\n':
			return true
		case ' ', '\t', '\r':
			continue
		default:
			return false
		}
	}
	return true
}

// unquoteEnumLiteral strips surrounding single quotes from an enum value and
// unescapes doubled quotes (two quotes become one). Bare values are returned
// as-is; enum values are literals, so no case folding is applied.
func unquoteEnumLiteral(s string) string {
	if val, ok := scanEnumLiteral(s); ok {
		return val
	}
	return s
}

// scanEnumLiteral scans a single-quoted literal from the start of s, handling
// doubled-quote escape sequences. Returns the unquoted value and true if
// successful.
func scanEnumLiteral(s string) (string, bool) {
	if len(s) == 0 || s[0] != '\'' {
		return "", false
	}
	var val strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] == '\'' {
			if i+1 < len(s) && s[i+1] == '\'' {
				// Escaped single quote
				val.WriteByte('\'')
				i++
			} else {
				// End of literal
				return val.String(), true
			}
		} else {
			val.WriteByte(s[i])
		}
	}
	return "", false
}

// scanQuotedIdent scans a quoted identifier from the start of s, handling ""
// escape sequences. Returns the unquoted name and true if successful.
func scanQuotedIdent(s string) (string, bool) {
	if len(s) == 0 || s[0] != '"' {
		return "", false
	}
	var name strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] == '"' {
			if i+1 < len(s) && s[i+1] == '"' {
				// Escaped double quote
				name.WriteByte('"')
				i++
			} else {
				// End of quoted identifier
				return name.String(), true
			}
		} else {
			name.WriteByte(s[i])
		}
	}
	return "", false
}

// extractConstraintName extracts the constraint name from a CONSTRAINT line.
// e.g. "CONSTRAINT users_pkey PRIMARY KEY (id)" -> "users_pkey"
func extractConstraintName(line string) string {
	line = strings.TrimSpace(line)
	upper := strings.ToUpper(line)
	if !strings.HasPrefix(upper, "CONSTRAINT ") {
		return ""
	}
	rest := strings.TrimSpace(line[len("CONSTRAINT "):])
	if strings.HasPrefix(rest, `"`) {
		name, ok := scanQuotedIdent(rest)
		if ok {
			return name
		}
		return ""
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return ""
	}
	return strings.ToLower(fields[0])
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
		name, ok := scanQuotedIdent(line)
		if ok {
			return name
		}
		return ""
	}

	// Unquoted identifier: first word, folded to lowercase per PostgreSQL behavior
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ""
	}

	name := fields[0]
	// Remove trailing comma if present
	name = strings.TrimSuffix(name, ",")
	return strings.ToLower(name)
}
