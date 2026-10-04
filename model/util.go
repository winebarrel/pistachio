package model

import (
	"regexp"
	"strings"
	"sync"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
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

	// A plan quotes the same names many times, and the keyword check is a
	// scan through cgo, so the answer is kept per name.
	if v, ok := quotedIdents.Load(name); ok {
		return v.(string)
	}

	v := quoteIdentUncached(name)
	quotedIdents.Store(name, v)

	return v
}

var quotedIdents sync.Map

func quoteIdentUncached(name string) string {
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

// StripTypeSchema removes a redundant "<schema>." qualifier from a type name,
// preserving any trailing "[]". The catalog reports a type in the search path
// unqualified (via format_type), while desired SQL may write it schema-
// qualified. Stripping the owning object's schema makes the two forms compare
// equal. schema is the quoted form Ident writes, since that is how a type name
// spells it. A type in a different search-path schema than its owner is not
// covered.
func StripTypeSchema(typeName, schema string) string {
	if schema == "" {
		return typeName
	}
	return strings.TrimPrefix(typeName, schema+".")
}

// joinSQL renders each value of m with f and joins the results with a blank
// line.
func joinSQL[V any](m *orderedmap.Map[string, V], f func(V) string) string {
	return strings.Join(
		m.TransformSlice(func(_ string, v V) string {
			return f(v)
		}),
		"\n\n",
	)
}
