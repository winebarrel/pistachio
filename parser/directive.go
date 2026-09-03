package parser

import (
	"fmt"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/pistachio/model"
)

var (
	renameDirectivePattern = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:renamed-from[ \t]+(.+?)[ \t]*$`)
	// execute-first shares a prefix with execute. The execute pattern accepts
	// only whitespace or end-of-line after the name, so an execute-first
	// comment never matches it.
	executeDirectivePattern      = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:execute(?:[ \t]+(.+?))?[ \t]*$`)
	executeFirstDirectivePattern = regexp.MustCompile(`(?m)^[ \t]*--[ \t]*pista:execute-first(?:[ \t]+(.+?))?[ \t]*$`)
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
	"renamed-from":  true,
	"execute":       true,
	"execute-first": true,
	"concurrently":  true,
	"bulk-alter":    true,
	"ignore":        true,
}

// validateDirectives checks for unknown -- pista: directives in the raw SQL
// and returns an error if any are found.
func validateDirectives(rawSQL string) error {
	matches := anyDirectivePattern.FindAllStringSubmatchIndex(rawSQL, -1)
	for _, m := range matches {
		// m[0] is the match start, m[2]:m[3] the name group.
		name := strings.TrimSpace(rawSQL[m[2]:m[3]])
		if name == "" {
			return &locatedError{msg: "invalid directive: -- pista: (missing directive name)", offset: m[0]}
		}
		if !knownDirectives[name] {
			return &locatedError{msg: fmt.Sprintf("unknown directive: -- pista:%s", name), offset: m[2]}
		}
	}

	if m := concurrentlyWithArgsPattern.FindStringIndex(rawSQL); m != nil {
		return &locatedError{msg: "-- pista:concurrently does not accept arguments", offset: m[0]}
	}

	if m := bulkAlterWithArgsPattern.FindStringIndex(rawSQL); m != nil {
		return &locatedError{msg: "-- pista:bulk-alter does not accept arguments", offset: m[0]}
	}

	if m := ignoreWithArgsPattern.FindStringIndex(rawSQL); m != nil {
		return &locatedError{msg: "-- pista:ignore does not accept arguments", offset: m[0]}
	}

	return nil
}

// ExecuteStmt represents an arbitrary SQL statement marked with
// -- pista:execute or -- pista:execute-first.
type ExecuteStmt struct {
	SQL      string // The SQL statement to execute
	CheckSQL string // Optional condition check SQL (empty = always execute)
	// First reports whether the statement came from -- pista:execute-first,
	// which runs before the managed DDL instead of after it. Its check SQL is
	// therefore evaluated against the pre-change schema.
	First bool
}

// extractExecuteDirectives scans raw SQL for `-- pista:execute [<check SQL>]`
// and `-- pista:execute-first [<check SQL>]` comments and pairs them with the
// following SQL statement.
// Returns the execute statements and a set of statement locations to skip
// during normal parsing.
func extractExecuteDirectives(rawSQL string, stmts []*pg_query.RawStmt) ([]*ExecuteStmt, map[int32]bool, error) {
	var executeStmts []*ExecuteStmt
	skipLocations := make(map[int32]bool)

	for _, stmt := range stmts {
		loc := stmt.StmtLocation
		leading := leadingDirectiveText(stmtRegion(rawSQL, stmt))

		// The two directives put the statement on opposite sides of the
		// managed DDL, so carrying both is a contradiction rather than a
		// preference. Repeating one directive still takes the last match.
		matches := executeDirectivePattern.FindAllStringSubmatchIndex(leading, -1)
		firstMatches := executeFirstDirectivePattern.FindAllStringSubmatchIndex(leading, -1)
		if len(matches) > 0 && len(firstMatches) > 0 {
			return nil, nil, fmt.Errorf("-- pista:execute and -- pista:execute-first cannot both apply to one statement")
		}
		first := len(firstMatches) > 0
		if first {
			matches = firstMatches
		}
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
		if lastMatch[2] >= 0 {
			checkSQL = strings.TrimSpace(leading[lastMatch[2]:lastMatch[3]])
			// Remove trailing semicolons; pgx extended protocol doesn't allow them
			checkSQL = strings.TrimRight(checkSQL, ";")
			checkSQL = strings.TrimSpace(checkSQL)
		}

		executeStmts = append(executeStmts, &ExecuteStmt{
			SQL:      deparsed,
			CheckSQL: checkSQL,
			First:    first,
		})
		skipLocations[loc] = true
	}

	return executeStmts, skipLocations, nil
}

// FormatExecuteStmt formats an ExecuteStmt as SQL with the directive comment.
func FormatExecuteStmt(es *ExecuteStmt) string {
	return FormatExecuteStmtWithNote(es, "")
}

// FormatExecuteStmtWithNote formats an ExecuteStmt with an extra comment line
// between the directive and the SQL. note is used by plan to record that the
// check SQL could not be evaluated, so the statement is shown without a
// verdict. Newlines in note are folded to keep the comment on one line.
func FormatExecuteStmtWithNote(es *ExecuteStmt, note string) string {
	directive := "-- pista:execute"
	if es.First {
		directive += "-first"
	}
	if es.CheckSQL != "" {
		directive += " " + es.CheckSQL
	}
	if note != "" {
		directive += "\n-- " + strings.Join(strings.Fields(note), " ")
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
	parts := model.SplitQualifiedName(value)
	for i, p := range parts {
		parts[i] = model.UnquoteIdent(p)
	}
	if len(parts) >= 2 {
		return model.Ident(parts...)
	}
	return model.Ident(defaultSchema, parts[0])
}

// normalizeUnqualifiedDirective normalizes a renamed-from directive value
// for unqualified names (columns, constraints, indexes, foreign keys)
// by unquoting the identifier. If a schema-qualified name is provided
// (e.g. "public.old_idx"), only the last part is used.
// The result matches the unquoted name used as orderedmap keys by the parser.
func normalizeUnqualifiedDirective(s string) string {
	parts := model.SplitQualifiedName(s)
	// Use the last part (the actual name, ignoring any schema prefix)
	last := parts[len(parts)-1]
	return model.UnquoteIdent(last)
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
		leading := leadingDirectiveText(stmtRegion(rawSQL, stmt))

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
		leading := leadingDirectiveText(stmtRegion(rawSQL, stmt))

		if pattern.MatchString(leading) {
			directives[loc] = true
		}
	}

	return directives
}

// findLeadingCommentEnd returns the byte offset where the whitespace and
// comments before a statement end and the statement begins. Both comment forms
// count, so a directive written after a `/* ... */` block is still part of the
// leading text. A string of nothing but comments returns its length, so the
// offset always stays within the string.
func findLeadingCommentEnd(s string) int {
	offset := 0
	for offset < len(s) {
		switch {
		case isSQLSpace(s[offset]):
			offset++
		case strings.HasPrefix(s[offset:], "--"):
			i := strings.IndexByte(s[offset:], '\n')
			if i < 0 {
				return len(s)
			}
			offset += i + 1
		case strings.HasPrefix(s[offset:], "/*"):
			end := blockCommentEnd(s, offset)
			if end < 0 {
				return len(s)
			}
			offset = end
		default:
			return offset
		}
	}
	return offset
}

func isSQLSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// leadingDirectiveText returns the whitespace and comments before a statement,
// with the body of every block comment blanked out. A directive inside a block
// comment is commented out like anything else, so a scan must not see it, while
// blanking rather than removing keeps the offsets extractExecuteDirectives
// slices a check SQL out of.
func leadingDirectiveText(region string) string {
	return blankBlockComments(region[:findLeadingCommentEnd(region)])
}

// blankBlockComments replaces every byte of a block comment, its delimiters
// included, with a space, leaving newlines in place so line-anchored patterns
// still see the same lines.
func blankBlockComments(s string) string {
	if !strings.Contains(s, "/*") {
		return s
	}

	out := []byte(s)
	for i := 0; i < len(s); {
		switch {
		case strings.HasPrefix(s[i:], "--"):
			// A line comment runs to the end of the line, so a `/*` in it
			// opens nothing.
			j := strings.IndexByte(s[i:], '\n')
			if j < 0 {
				i = len(s)
			} else {
				i += j + 1
			}
		case strings.HasPrefix(s[i:], "/*"):
			end := blockCommentEnd(s, i)
			if end < 0 {
				end = len(s)
			}
			for k := i; k < end; k++ {
				if out[k] != '\n' {
					out[k] = ' '
				}
			}
			i = end
		default:
			i++
		}
	}
	return string(out)
}

// blockCommentEnd returns the offset just past the block comment that starts at
// pos, or -1 when it is never closed. PostgreSQL nests block comments, so an
// inner `/*` takes a `*/` of its own to close.
func blockCommentEnd(s string, pos int) int {
	depth := 0
	for i := pos; i+1 < len(s); {
		switch {
		case s[i] == '/' && s[i+1] == '*':
			depth++
			i += 2
		case s[i] == '*' && s[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return -1
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
