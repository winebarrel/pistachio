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

	// Every identifier pistachio emits sits in a ColId position, which the
	// grammar defines as a bare IDENT, an unreserved keyword, or a col_name
	// keyword. The two remaining categories need quotes: reserved keywords,
	// and type_func_name keywords -- listed in the manual as "reserved (can
	// be function or type name)", so usable as a type or function name but
	// not as a table, column, index, constraint, or policy name.
	//
	// The check is an allow list rather than a deny list so that a keyword
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

func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func QuoteLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}
