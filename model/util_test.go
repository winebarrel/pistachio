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

// Quoting follows the keyword categories in PostgreSQL's kwlist.h: a ColId
// accepts a bare identifier, an unreserved keyword, or a col_name keyword, and
// the other two categories need quotes.
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

// The full type_func_name category. Valid as a type or function name, not as a
// table, column, or index name.
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

// col_name keywords work as a table or column name unquoted.
func TestIdent_colNameKeywords(t *testing.T) {
	for _, kw := range []string{"int", "char", "numeric", "position", "values"} {
		t.Run(kw, func(t *testing.T) {
			assert.Equal(t, pg_query.KeywordKind_COL_NAME_KEYWORD, keywordKindOf(t, kw))
			assert.Equal(t, kw, Ident(kw))
		})
	}
}

// identPositions lists every position where a bare identifier is emitted,
// taken from the Ident call sites in model/, diff/, and apply.go.
var identPositions = []string{
	// model/table.go
	"CREATE TABLE %s (id integer)",
	"CREATE TABLE public.t (%s integer)",
	"CREATE TABLE public.t (id integer COLLATE %s)",
	"CREATE TABLE public.t (id integer CONSTRAINT %s NOT NULL)",
	"CREATE TABLE public.t (id integer, CONSTRAINT %s PRIMARY KEY (id))",
	"CREATE TABLE public.t (id integer) TABLESPACE %s",
	// diff/tables.go
	"ALTER TABLE public.t ADD COLUMN %s integer",
	"ALTER TABLE public.t ADD COLUMN c integer CONSTRAINT %s NOT NULL",
	"ALTER TABLE public.t ALTER COLUMN %s SET DATA TYPE text",
	"ALTER TABLE public.t ALTER COLUMN %s SET DEFAULT 1",
	"ALTER TABLE public.t DROP COLUMN %s",
	"ALTER TABLE ONLY public.t ADD CONSTRAINT %s CHECK (id > 0)",
	"ALTER TABLE public.t DROP CONSTRAINT %s",
	"ALTER TABLE public.t VALIDATE CONSTRAINT %s",
	"DROP INDEX public.%s",
	// diff/rename.go
	"ALTER TABLE public.t RENAME TO %s",
	"ALTER TABLE public.t RENAME COLUMN a TO %s",
	"ALTER TABLE public.t RENAME COLUMN %s TO b",
	"ALTER TABLE public.t RENAME CONSTRAINT %s TO c",
	"ALTER TABLE public.t RENAME CONSTRAINT c TO %s",
	"ALTER INDEX public.i RENAME TO %s",
	"ALTER TYPE public.e RENAME TO %s",
	"ALTER SEQUENCE public.s RENAME TO %s",
	"ALTER DOMAIN public.d RENAME TO %s",
	"ALTER VIEW public.v RENAME TO %s",
	"ALTER MATERIALIZED VIEW public.mv RENAME TO %s",
	// model/index.go, model/enum.go, model/composite_type.go
	"CREATE INDEX %s ON public.t (id)",
	"CREATE TYPE %s AS ENUM ('a')",
	"CREATE TYPE %s AS (a text)",
	"CREATE TYPE public.ct AS (%s text)",
	"CREATE TYPE public.ct AS (a text COLLATE %s)",
	// diff/composite_types.go
	"ALTER TYPE public.ct ADD ATTRIBUTE %s text",
	"ALTER TYPE public.ct DROP ATTRIBUTE %s",
	"ALTER TYPE public.ct ALTER ATTRIBUTE %s TYPE text",
	"ALTER TYPE public.ct RENAME ATTRIBUTE %s TO b",
	"ALTER TYPE public.ct RENAME ATTRIBUTE a TO %s",
	// model/domain.go, diff/domains.go
	"CREATE DOMAIN %s AS text",
	"CREATE DOMAIN public.d AS integer CONSTRAINT %s CHECK (VALUE > 0)",
	"ALTER DOMAIN public.d ADD CONSTRAINT %s CHECK (VALUE > 0)",
	"ALTER DOMAIN public.d DROP CONSTRAINT %s",
	"ALTER DOMAIN public.d VALIDATE CONSTRAINT %s",
	// model/sequence.go, model/view.go
	"CREATE SEQUENCE %s",
	"CREATE VIEW %s AS SELECT 1",
	"CREATE MATERIALIZED VIEW %s AS SELECT 1",
	// model/policy.go, diff/policies.go
	"CREATE POLICY %s ON public.t",
	"ALTER POLICY %s ON public.t RENAME TO p2",
	"ALTER POLICY p ON public.t RENAME TO %s",
	"DROP POLICY %s ON public.t",
	"COMMENT ON TABLE %s IS 'x'",
	// apply.go
	"SET search_path TO %s",
}

// The role position is separate because of unusableRoleName.
const rolePosition = "CREATE POLICY p ON public.t TO %s"

// PostgreSQL rejects CREATE ROLE none quoted or not, so no policy can name it.
const unusableRoleName = "none"

// qualifiedIdentPositions takes a schema-qualified identifier, the form the
// FQTN / FQEN / FQDN / FQCN / FQVN / FQN helpers return.
var qualifiedIdentPositions = []string{
	"CREATE TABLE %s (id integer)",
	"ALTER TABLE ONLY %s ADD CONSTRAINT c CHECK (id > 0)",
	"ALTER TABLE %s RENAME TO t2",
	"DROP TABLE %s",
	"CREATE INDEX i ON %s (id)",
	"DROP INDEX %s",
	"ALTER INDEX %s RENAME TO i2",
	"CREATE TYPE %s AS ENUM ('a')",
	"CREATE TYPE %s AS (a text)",
	"ALTER TYPE %s ADD ATTRIBUTE a text",
	"DROP TYPE %s",
	"CREATE DOMAIN %s AS text",
	"ALTER DOMAIN %s DROP CONSTRAINT c",
	"DROP DOMAIN %s",
	"CREATE SEQUENCE %s",
	"ALTER SEQUENCE %s RENAME TO s2",
	"DROP SEQUENCE %s",
	"CREATE VIEW %s AS SELECT 1",
	"CREATE MATERIALIZED VIEW %s AS SELECT 1",
	"DROP VIEW %s",
	"DROP MATERIALIZED VIEW %s",
	"CREATE POLICY p ON %s",
	"DROP POLICY p ON %s",
	"COMMENT ON TABLE %s IS 'x'",
	"COMMENT ON TYPE %s IS 'x'",
	"COMMENT ON DOMAIN %s IS 'x'",
	"COMMENT ON VIEW %s IS 'x'",
	"COMMENT ON MATERIALIZED VIEW %s IS 'x'",
	"COMMENT ON SEQUENCE %s IS 'x'",
}

// columnIdentPositions takes a schema.table.column identifier, used for column
// comments.
var columnIdentPositions = []string{
	"COMMENT ON COLUMN %s IS 'x'",
}

// Every PostgreSQL keyword, in every position, must parse after quoting. The
// quoting decision itself is checked too, so a keyword that stops needing
// quotes shows up as a failure instead of extra noise in the output.
func TestIdent_allKeywords(t *testing.T) {
	keywords := loadKeywords(t)
	// Pinned so that a pg_query_go upgrade adding keywords fails here instead
	// of silently leaving them untested.
	require.Len(t, keywords, 491, "keyword corpus is stale; run make keywords")

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
		assertParsesIn(t, qualifiedIdentPositions, Ident("public", kw))
		assertParsesIn(t, columnIdentPositions, Ident("public", "t", kw))
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

// Identifiers quoted for other reasons (case, characters outside the safe set)
// must parse as well.
func TestIdent_nonKeywordsRoundTrip(t *testing.T) {
	for _, name := range []string{"users", "Users", "my-table", `my"table`, "a b", "a.b", "1st", "_x", "0"} {
		t.Run(name, func(t *testing.T) {
			assertParses(t, Ident(name), true)
			assertParsesIn(t, qualifiedIdentPositions, Ident("public", name))
			assertParsesIn(t, columnIdentPositions, Ident("public", "t", name))
		})
	}
}

// assertParses checks a bare identifier in every position it is emitted.
func assertParses(t *testing.T, ident string, withRole bool) {
	t.Helper()
	positions := slices.Clone(identPositions)
	if withRole {
		positions = append(positions, rolePosition)
	}
	assertParsesIn(t, positions, ident)
}

func assertParsesIn(t *testing.T, positions []string, ident string) {
	t.Helper()
	for _, tmpl := range positions {
		sql := fmt.Sprintf(tmpl, ident)
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

func TestSplitQualifiedName(t *testing.T) {
	tests := []struct {
		name     string
		expected []string
	}{
		{"users", []string{"users"}},
		{"public.users", []string{"public", "users"}},
		{`public."my.coll"`, []string{"public", `"my.coll"`}},
		{`"C.utf8"`, []string{`"C.utf8"`}},
		{`"My Schema"."Old Name"`, []string{`"My Schema"`, `"Old Name"`}},
		{`public."a""b"`, []string{"public", `"a""b"`}},
		{`a.b.c`, []string{"a", "b", "c"}},
		{"public . users", []string{"public", "users"}},
		{"", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, SplitQualifiedName(tt.name))
		})
	}
}

func TestUnquoteIdent(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"users", "users"},
		{"Users", "users"},
		{`"Users"`, "Users"},
		{`"my.coll"`, "my.coll"},
		{`"a""b"`, `a"b`},
		{`""`, ""},
		{`"`, `"`},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, UnquoteIdent(tt.name))
		})
	}
}
