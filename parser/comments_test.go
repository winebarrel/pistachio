package parser

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanComments_lineAndBlock(t *testing.T) {
	sql := "-- line1\n/* block */\nCREATE TABLE x (id integer);\n"
	cs, err := ScanComments(sql)
	require.NoError(t, err)
	require.Len(t, cs, 2)
	assert.Equal(t, "-- line1", cs[0].Text)
	assert.Equal(t, "/* block */", cs[1].Text)
}

func TestScanComments_filtersDirective(t *testing.T) {
	sql := "-- pist:execute SELECT 1\n-- regular comment\nCREATE TABLE x (id integer);\n"
	cs, err := ScanComments(sql)
	require.NoError(t, err)
	require.Len(t, cs, 1)
	assert.Equal(t, "-- regular comment", cs[0].Text)
}

func TestScanComments_invalidSQL(t *testing.T) {
	_, err := ScanComments("/* unterminated")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to scan SQL for comments")
}

func TestCodeStart_skipsLeadingCommentsAndWhitespace(t *testing.T) {
	sql := "-- header\n  /* note */\n\nCREATE TABLE x (id integer);"
	pos := CodeStart(sql, 0)
	assert.Equal(t, byte('C'), sql[pos])
}

func TestCodeStart_alreadyAtCode(t *testing.T) {
	sql := "CREATE TABLE x (id integer);"
	pos := CodeStart(sql, 0)
	assert.Equal(t, int32(0), pos)
}

func TestCodeStart_runsToEnd(t *testing.T) {
	sql := "-- only comments\n  /* and whitespace */\n"
	pos := CodeStart(sql, 0)
	assert.Equal(t, int32(len(sql)), pos)
}

func TestFilterTopLevelComments_dropsInline(t *testing.T) {
	sql := "-- top\nCREATE TABLE x (\n  id integer, -- inline\n  name text\n);\n"
	cs, err := ScanComments(sql)
	require.NoError(t, err)
	require.Len(t, cs, 2)

	parsed, err := pg_query.Parse(sql)
	require.NoError(t, err)
	filtered := filterTopLevelComments(sql, cs, parsed.Stmts)
	require.Len(t, filtered, 1)
	assert.Equal(t, "-- top", filtered[0].Text)
}

func TestFilterTopLevelComments_keepsBetweenStmts(t *testing.T) {
	sql := "CREATE TABLE a (id integer);\n-- between\nCREATE TABLE b (id integer);\n"
	cs, err := ScanComments(sql)
	require.NoError(t, err)
	parsed, err := pg_query.Parse(sql)
	require.NoError(t, err)
	filtered := filterTopLevelComments(sql, cs, parsed.Stmts)
	require.Len(t, filtered, 1)
	assert.Equal(t, "-- between", filtered[0].Text)
}

func TestCommentStmtTargetFQN(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
		ok   bool
	}{
		{"table", "COMMENT ON TABLE public.users IS 'x';", "public.users", true},
		{"table_unqualified", "COMMENT ON TABLE users IS 'x';", "public.users", true},
		{"view", "COMMENT ON VIEW public.v IS 'x';", "public.v", true},
		{"matview", "COMMENT ON MATERIALIZED VIEW public.mv IS 'x';", "public.mv", true},
		{"column", "COMMENT ON COLUMN public.users.name IS 'x';", "public.users", true},
		{"type", "COMMENT ON TYPE public.status IS 'x';", "public.status", true},
		{"domain", "COMMENT ON DOMAIN public.pos_int IS 'x';", "public.pos_int", true},
		{"unsupported_function", "COMMENT ON FUNCTION public.f() IS 'x';", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := pg_query.Parse(tc.sql)
			require.NoError(t, err)
			require.NotEmpty(t, parsed.Stmts)
			cs := parsed.Stmts[0].Stmt.GetCommentStmt()
			require.NotNil(t, cs)
			got, ok := commentStmtTargetFQN(cs, "public")
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}
