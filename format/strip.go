package format

import (
	"fmt"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

var renamedFromPattern = regexp.MustCompile(`^--[ \t]*pista:renamed-from(?:[ \t\r]|$)`)

// StripRenamedFrom removes every "-- pista:renamed-from" line from sql. Only a
// comment on a line of its own is a directive, so a comment after code stays,
// and so does text inside a string or a routine body, which the scanner reads
// as part of that token.
func StripRenamedFrom(sql string) (string, error) {
	result, err := pg_query.Scan(sql)
	if err != nil {
		return "", fmt.Errorf("failed to scan SQL: %w", err)
	}

	var b strings.Builder
	prev := 0

	for _, t := range result.Tokens {
		if t.Token != pg_query.Token_SQL_COMMENT {
			continue
		}

		start, end := int(t.Start), int(t.End)
		if !renamedFromPattern.MatchString(sql[start:end]) {
			continue
		}

		lineStart := strings.LastIndex(sql[:start], "\n") + 1
		if strings.Trim(sql[lineStart:start], " \t") != "" {
			continue
		}

		lineEnd := end
		for lineEnd < len(sql) && (sql[lineEnd] == '\r' || sql[lineEnd] == ' ' || sql[lineEnd] == '\t') {
			lineEnd++
		}
		if lineEnd < len(sql) && sql[lineEnd] == '\n' {
			lineEnd++
		}

		b.WriteString(sql[prev:lineStart])
		prev = lineEnd
	}

	b.WriteString(sql[prev:])

	return b.String(), nil
}
