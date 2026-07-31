package model

import (
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

var safeIdentifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func Ident(names ...string) string {
	var idents []string

	for _, n := range names {
		if n == "" {
			continue
		}
		idents = append(idents, quoteIdent(n))
	}

	return strings.Join(idents, ".")
}

func quoteIdent(name string) string {
	if name == "" {
		return `""`
	}

	if !safeIdentifierPattern.MatchString(name) {
		return quote(name)
	}

	result, err := pg_query.Scan(name)
	if err != nil || len(result.Tokens) != 1 {
		return quote(name)
	}

	// Identifiers are emitted in ColId positions, which accept a bare
	// identifier, an unreserved keyword, or a col_name keyword. Reserved and
	// type_func_name keywords need quotes. The list is an allow list so a
	// category added upstream is quoted until it is reviewed.
	switch result.Tokens[0].KeywordKind {
	case pg_query.KeywordKind_NO_KEYWORD,
		pg_query.KeywordKind_UNRESERVED_KEYWORD,
		pg_query.KeywordKind_COL_NAME_KEYWORD:
		return name
	default:
		return quote(name)
	}
}

// SplitQualifiedName splits a qualified name into its parts, respecting
// quoting: `public."my.coll"` -> [`public`, `"my.coll"`].
func SplitQualifiedName(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false

	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '"':
			if inQuote && i+1 < len(s) && s[i+1] == '"' {
				current.WriteString(`""`)
				i++
			} else {
				inQuote = !inQuote
				current.WriteByte(ch)
			}
		case ch == '.' && !inQuote:
			parts = append(parts, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}
	return parts
}

// UnquoteIdent is the inverse of quoteIdent: it strips the surrounding double
// quotes and unescapes doubled ones. An unquoted identifier is folded to lower
// case, the way PostgreSQL reads it.
func UnquoteIdent(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	return strings.ToLower(s)
}

func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func QuoteLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}
