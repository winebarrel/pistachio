package format

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The checks below cannot be reached through Format with the layout rules as
// they are: a result that drops, adds or rewrites a token is a bug in the
// renderer. They are what stops such a bug from reaching a schema file, so
// they are exercised directly.
func TestVerify(t *testing.T) {
	toks, err := scan(`CREATE TABLE public.t (id integer);`)
	require.NoError(t, err)

	require.NoError(t, verify(toks, `CREATE TABLE public.t (id integer);`))
	require.ErrorContains(t, verify(toks, `CREATE TABLE public.t (id integer)`), "changed the token count")
	require.ErrorContains(t, verify(toks, `CREATE TABLE public.t (id bigint);`), "changed a token")
	require.ErrorContains(t, verify(toks, "CREATE TABLE public.t (id integer);\n'"), "failed to scan")
}

// A token that lost its quotes scans as the keyword it spells, so verify
// compares it by text alone.
func TestVerify_UnquotedTokenKeepsItsKind(t *testing.T) {
	toks, err := scan(`CREATE TABLE public.t ("name" text);`)
	require.NoError(t, err)

	for _, tok := range toks {
		if tok.orig == `"name"` {
			tok.out = "name"
		}
	}

	require.NoError(t, verify(toks, `CREATE TABLE public.t (name text);`))
}

func TestJoins(t *testing.T) {
	assert.True(t, joins("(", "id"))
	assert.True(t, joins("id", ")"))
	assert.False(t, joins("select", "name"), "two words read as one without a space")
	assert.False(t, joins("-", "-"), "two operators read as a comment without a space")
}

func TestScan_Empty(t *testing.T) {
	toks, err := scan("")
	require.NoError(t, err)
	assert.Empty(t, toks)
}

func TestSplit_StatementWithoutSemicolon(t *testing.T) {
	sql := "CREATE TABLE public.a (id int);\nCREATE TABLE public.b (id int)"

	stmts, err := split(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	assert.Equal(t, len(sql), stmts[1].end, "the last statement runs to the end of the input")
}

func TestBareIdent_Rejects(t *testing.T) {
	tests := map[string]string{
		"an upper-case letter": `"Value"`,
		"a reserved keyword":   `"select"`,
		"a type_func_name":     `"left"`,
		"a space":              `"a b"`,
		"an empty name":        `""`,
	}

	for name, text := range tests {
		t.Run(name, func(t *testing.T) {
			_, ok := bareIdent(&token{kind: pg_query.Token_IDENT, orig: text})
			assert.False(t, ok)
		})
	}

	inner, ok := bareIdent(&token{kind: pg_query.Token_IDENT, orig: `"items"`})
	require.True(t, ok)
	assert.Equal(t, "items", inner)
}
