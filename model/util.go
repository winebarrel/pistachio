package model

import (
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	pgquery "github.com/wasilibs/go-pgquery"
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

	result, err := pgquery.Scan(name)
	if err != nil || len(result.Tokens) != 1 {
		return quote(name)
	}

	switch result.Tokens[0].KeywordKind {
	case pg_query.KeywordKind_RESERVED_KEYWORD:
		return quote(name)
	default:
		return name
	}
}

func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func QuoteLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}
