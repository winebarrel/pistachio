package parser_test

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/parser"
)

func TestFigureColname(t *testing.T) {
	// The names are the ones PostgreSQL 16 gave each target.
	tests := []struct {
		expr string
		want string
	}{
		{"t.m", "m"},
		{"count(*)", "count"},
		{"pg_catalog.lower(m)", "lower"},
		{"(a + n)::text", "text"},
		{"n::varchar", "n"},
		{"coalesce(m, 'z')", "coalesce"},
		{"nullif(a, 1)", "nullif"},
		{"greatest(a, n)", "greatest"},
		{"least(a, n)", "least"},
		{"ARRAY[a, n]", "array"},
		{"CASE WHEN a > 0 THEN m END", "case"},
		{"CASE WHEN a > 0 THEN m ELSE upper(m) END", "upper"},
		{`m COLLATE "C"`, "m"},
		{"(t.r).f", "f"},
		{"current_date", "current_date"},
		{"current_time(2)", "current_time"},
		{"localtimestamp", "localtimestamp"},
		{"current_user", "current_user"},
		{"ROW(a, n)", "row"},
		{"grouping(m)", "grouping"},
		{"EXISTS (SELECT 1)", "exists"},
		{"ARRAY(SELECT n FROM u)", "array"},
		{"(SELECT max(n) FROM u)", "max"},
		{"(SELECT n AS x FROM u)", "x"},
		{"(SELECT n FROM u UNION SELECT a FROM u)", "n"},
		{"(SELECT 1)", "?column?"},
		{"a IN (SELECT n FROM u)", "?column?"},
		{"a + n", "?column?"},
		{"'x'", "?column?"},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			result, err := pg_query.Parse("SELECT " + tc.expr)
			require.NoError(t, err)
			target := result.Stmts[0].Stmt.GetSelectStmt().TargetList[0].GetResTarget()
			assert.Equal(t, tc.want, parser.FigureColname(target.Val))
		})
	}
}
