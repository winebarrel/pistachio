package model

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdent_single(t *testing.T) {
	assert.Equal(t, "users", Ident("users"))
}

func TestIdent_schemaAndTable(t *testing.T) {
	assert.Equal(t, "public.users", Ident("public", "users"))
}

func TestIdent_reservedKeyword(t *testing.T) {
	assert.Equal(t, `"select"`, Ident("select"))
}

func TestIdent_uppercase(t *testing.T) {
	assert.Equal(t, `"Users"`, Ident("Users"))
}

func TestIdent_empty(t *testing.T) {
	assert.Equal(t, "", Ident(""))
}

func TestIdent_emptySchema(t *testing.T) {
	assert.Equal(t, "users", Ident("", "users"))
}

func TestIdent_withSpecialChars(t *testing.T) {
	assert.Equal(t, `"my-table"`, Ident("my-table"))
}

func TestIdent_withDoubleQuote(t *testing.T) {
	assert.Equal(t, `"my""table"`, Ident(`my"table`))
}

func TestIdent_unreservedKeyword(t *testing.T) {
	// "name" is unreserved in PostgreSQL, should not be quoted
	assert.Equal(t, "name", Ident("name"))
}

func TestIdent_multipleTokens(t *testing.T) {
	// A string that scans to multiple tokens should be quoted
	assert.Equal(t, `"a b"`, Ident("a b"))
}

func TestQuoteIdent_empty(t *testing.T) {
	// quoteIdent with empty string returns quoted empty
	assert.Equal(t, `""`, quoteIdent(""))
}

func TestQuoteLiteral(t *testing.T) {
	assert.Equal(t, "'hello'", QuoteLiteral("hello"))
}

func TestQuoteLiteral_withSingleQuote(t *testing.T) {
	assert.Equal(t, "'it''s'", QuoteLiteral("it's"))
}

// Identifier quoting follows PostgreSQL's keyword categories (kwlist.h).
// ColId, the grammar rule behind every identifier pistachio emits, accepts a
// bare IDENT, an unreserved keyword, or a col_name keyword. The remaining two
// categories -- type_func_name ("reserved (can be function or type name)" in
// the manual) and reserved -- must be quoted.
func TestIdent_keywordKinds(t *testing.T) {
	tests := []struct {
		name     string
		kind     pg_query.KeywordKind
		expected string
	}{
		{"status", pg_query.KeywordKind_NO_KEYWORD, "status"},
		{"name", pg_query.KeywordKind_UNRESERVED_KEYWORD, "name"},
		{"int", pg_query.KeywordKind_COL_NAME_KEYWORD, "int"},
		{"left", pg_query.KeywordKind_TYPE_FUNC_NAME_KEYWORD, `"left"`},
		{"select", pg_query.KeywordKind_RESERVED_KEYWORD, `"select"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.kind, keywordKindOf(t, tt.name), "keyword category changed upstream")
			assert.Equal(t, tt.expected, Ident(tt.name))
		})
	}
}

// The full type_func_name category. These parse as a type or function name but
// not as a table, column, or index name, so all of them need quoting.
func TestIdent_typeFuncNameKeywords(t *testing.T) {
	keywords := []string{
		"authorization", "binary", "collation", "concurrently", "cross",
		"current_schema", "freeze", "full", "ilike", "inner", "is", "isnull",
		"join", "left", "like", "natural", "notnull", "outer", "overlaps",
		"right", "similar", "tablesample", "verbose",
	}

	for _, kw := range keywords {
		t.Run(kw, func(t *testing.T) {
			assert.Equal(t, pg_query.KeywordKind_TYPE_FUNC_NAME_KEYWORD, keywordKindOf(t, kw))
			assert.Equal(t, `"`+kw+`"`, Ident(kw))
		})
	}
}

// col_name keywords are usable as a table or column name unquoted, so quoting
// them would only add noise.
func TestIdent_colNameKeywords(t *testing.T) {
	for _, kw := range []string{"int", "char", "numeric", "position", "values"} {
		t.Run(kw, func(t *testing.T) {
			assert.Equal(t, pg_query.KeywordKind_COL_NAME_KEYWORD, keywordKindOf(t, kw))
			assert.Equal(t, kw, Ident(kw))
		})
	}
}

// identPositions covers every syntactic position in which pistachio emits an
// identifier through Ident. Each template takes one already-quoted identifier.
var identPositions = []string{
	"CREATE TABLE %s (id integer)",
	"CREATE TABLE public.t (%s integer)",
	"CREATE TABLE public.t (id integer COLLATE %s)",
	"CREATE TABLE public.t (id integer) TABLESPACE %s",
	"ALTER TABLE public.t ADD COLUMN %s integer",
	"ALTER TABLE public.t RENAME COLUMN a TO %s",
	"ALTER TABLE ONLY public.t ADD CONSTRAINT %s CHECK (id > 0)",
	"CREATE INDEX %s ON public.t (id)",
	"CREATE TYPE %s AS ENUM ('a')",
	"CREATE TYPE %s AS (a text)",
	"CREATE DOMAIN %s AS text",
	"CREATE SEQUENCE %s",
	"CREATE VIEW %s AS SELECT 1",
	"CREATE POLICY %s ON public.t",
	"COMMENT ON TABLE %s IS 'x'",
}

// rolePosition is kept apart from identPositions because of unusableRoleName.
const rolePosition = "CREATE POLICY p ON public.t TO %s"

// PostgreSQL reserves "none" as a role name outright: CREATE ROLE none is
// rejected quoted or not, so no policy can ever name it and quoting would not
// help. Every other keyword works as a role name once quoted.
const unusableRoleName = "none"

// qualifiedIdentPositions takes a schema-qualified identifier.
var qualifiedIdentPositions = []string{
	"CREATE TABLE %s (id integer)",
	"ALTER TABLE ONLY %s ADD CONSTRAINT c CHECK (id > 0)",
	"COMMENT ON TABLE %s IS 'x'",
}

// TestIdent_allKeywords is the exhaustive form of the rule: every PostgreSQL
// keyword, in every position pistachio emits an identifier, must survive a
// round trip through the parser. It also pins the quoting decision itself, so
// a category that stops needing quotes is caught as a regression rather than
// silently producing noisier SQL.
func TestIdent_allKeywords(t *testing.T) {
	keywords := loadKeywords(t)
	require.Greater(t, len(keywords), 400, "keyword corpus looks truncated")

	seen := map[pg_query.KeywordKind]int{}

	for _, kw := range keywords {
		kind := keywordKindOf(t, kw)
		seen[kind]++

		quoted := kind == pg_query.KeywordKind_TYPE_FUNC_NAME_KEYWORD ||
			kind == pg_query.KeywordKind_RESERVED_KEYWORD

		got := Ident(kw)
		if quoted {
			assert.Equal(t, `"`+kw+`"`, got, "%s (%s) must be quoted", kw, kind)
		} else {
			assert.Equal(t, kw, got, "%s (%s) must not be quoted", kw, kind)
		}

		assertParses(t, got, kw != unusableRoleName)
		assertQualifiedParses(t, Ident("public", kw))
	}

	for _, kind := range []pg_query.KeywordKind{
		pg_query.KeywordKind_UNRESERVED_KEYWORD,
		pg_query.KeywordKind_COL_NAME_KEYWORD,
		pg_query.KeywordKind_TYPE_FUNC_NAME_KEYWORD,
		pg_query.KeywordKind_RESERVED_KEYWORD,
	} {
		assert.NotZero(t, seen[kind], "corpus has no %s entry", kind)
	}
}

// Non-keyword identifiers are quoted for reasons unrelated to the keyword
// category (case, characters outside the safe set), and must still parse.
func TestIdent_nonKeywordsRoundTrip(t *testing.T) {
	for _, name := range []string{"users", "Users", "my-table", `my"table`, "a b", "a.b", "1st", "_x", "0"} {
		t.Run(name, func(t *testing.T) {
			got := Ident(name)
			assertParses(t, got, true)
			assertQualifiedParses(t, Ident("public", name))
		})
	}
}

// assertParses checks that a single quoted identifier is accepted in every
// position pistachio emits one.
func assertParses(t *testing.T, ident string, withRole bool) {
	t.Helper()
	positions := slices.Clone(identPositions)
	if withRole {
		positions = append(positions, rolePosition)
	}
	for _, tmpl := range positions {
		sql := fmt.Sprintf(tmpl, ident)
		if _, err := pg_query.Parse(sql); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
}

func assertQualifiedParses(t *testing.T, qualified string) {
	t.Helper()
	for _, tmpl := range qualifiedIdentPositions {
		sql := fmt.Sprintf(tmpl, qualified)
		if _, err := pg_query.Parse(sql); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
}

func keywordKindOf(t *testing.T, name string) pg_query.KeywordKind {
	t.Helper()
	result, err := pg_query.Scan(name)
	require.NoError(t, err)
	require.Len(t, result.Tokens, 1)
	return result.Tokens[0].KeywordKind
}

func loadKeywords(t *testing.T) []string {
	t.Helper()
	f, err := os.Open("testdata/keywords.txt")
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	var keywords []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		keywords = append(keywords, line)
	}
	require.NoError(t, scanner.Err())
	return keywords
}
