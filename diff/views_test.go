package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func TestDiffViews_newView(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE OR REPLACE VIEW public.v1")
}

func TestDiffViews_dropView(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP VIEW public.v1;"}, result.DropStmts)
}

func TestDiffViews_dropView_denied(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()

	result, err := DiffViews(current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.CreateStmts)
	assert.Empty(t, result.DropStmts)
	assert.Equal(t, []string{"-- skipped: DROP VIEW public.v1;"}, result.DisallowedDropStmts)
}

func TestDiffViews_modifyView(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 2"})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE OR REPLACE VIEW public.v1")
}

func TestDiffViews_noChange(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.CreateStmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffViews_formattingDifferenceIgnored(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT   1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.CreateStmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffViews_commentAdd(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1", Comment: new("my view")})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 1)
	assert.Equal(t, "COMMENT ON VIEW public.v1 IS 'my view';", result.CreateStmts[0])
}

func TestDiffViews_commentDrop(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1", Comment: new("my view")})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 1)
	assert.Equal(t, "COMMENT ON VIEW public.v1 IS NULL;", result.CreateStmts[0])
}

func TestDiffViews_rename(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	oldName := "public.v1"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v2", &model.View{Schema: "public", Name: "v2", RenameFrom: &oldName, Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER VIEW public.v1 RENAME TO v2;"}, result.CreateStmts)
}

func TestDiffViews_rename_selfRename_skipped(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	oldName := "public.v1"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", RenameFrom: &oldName, Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.CreateStmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffViews_rename_alreadyApplied(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v2", &model.View{Schema: "public", Name: "v2", Definition: "SELECT 1"})

	oldName := "public.v1"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v2", &model.View{Schema: "public", Name: "v2", RenameFrom: &oldName, Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.CreateStmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffViews_rename_destinationExists_error(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	current.Set("public.v2", &model.View{Schema: "public", Name: "v2", Definition: "SELECT 2"})

	oldName := "public.v1"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v2", &model.View{Schema: "public", Name: "v2", RenameFrom: &oldName, Definition: "SELECT 1"})

	_, err := DiffViews(current, desired, allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffViews_rename_crossSchema_error(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	oldName := "public.v1"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("other.v2", &model.View{Schema: "other", Name: "v2", RenameFrom: &oldName, Definition: "SELECT 1"})

	_, err := DiffViews(current, desired, allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cross-schema rename")
}

func TestDiffViews_rename_sourceNotFound(t *testing.T) {
	current := orderedmap.New[string, *model.View]()

	oldName := "public.nonexistent"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v2", &model.View{Schema: "public", Name: "v2", RenameFrom: &oldName, Definition: "SELECT 1"})

	_, err := DiffViews(current, desired, allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source")
}

func TestEqualViewDef_same(t *testing.T) {
	assert.True(t, equalViewDef("SELECT 1", "SELECT 1"))
}

func TestEqualViewDef_formattingDifference(t *testing.T) {
	assert.True(t, equalViewDef("SELECT   1", "SELECT 1"))
}

func TestEqualViewDef_different(t *testing.T) {
	assert.False(t, equalViewDef("SELECT 1", "SELECT 2"))
}

func TestEqualViewDef_schemaQualificationDifference(t *testing.T) {
	// pg_get_viewdef omits schema, parser preserves it
	assert.True(t, equalViewDef(
		"SELECT id FROM users",
		"SELECT id FROM public.users",
	))
}

func TestEqualViewDef_columnQualificationDifference(t *testing.T) {
	// pg_get_viewdef adds table.column, parser doesn't
	assert.True(t, equalViewDef(
		"SELECT users.id, users.name FROM users",
		"SELECT id, name FROM public.users",
	))
}

func TestEqualViewDef_joinQualification(t *testing.T) {
	// Covers JoinExpr + A_Expr paths in stripQualifications
	assert.True(t, equalViewDef(
		"SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
		"SELECT u.id FROM public.users u JOIN public.orders o ON u.id = o.user_id",
	))
}

func TestEqualViewDef_whereSubquery(t *testing.T) {
	// Covers SubLink + BoolExpr paths in stripQualifications
	assert.True(t, equalViewDef(
		"SELECT users.id FROM users WHERE users.id > 0 AND EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
		"SELECT id FROM public.users WHERE id > 0 AND EXISTS (SELECT 1 FROM public.orders WHERE user_id = id)",
	))
}

func TestEqualViewDef_groupByOrderByLimit(t *testing.T) {
	// Covers GroupClause, SortClause, LimitCount paths
	assert.True(t, equalViewDef(
		"SELECT users.id, count(*) AS cnt FROM users GROUP BY users.id ORDER BY users.id LIMIT 10",
		"SELECT id, count(*) AS cnt FROM public.users GROUP BY id ORDER BY id LIMIT 10",
	))
}

func TestEqualViewDef_unionDifference(t *testing.T) {
	// Covers Larg/Rarg paths
	assert.True(t, equalViewDef(
		"SELECT users.id FROM users UNION SELECT admins.id FROM admins",
		"SELECT id FROM public.users UNION SELECT id FROM public.admins",
	))
}

func TestEqualViewDef_subselect(t *testing.T) {
	// Covers RangeSubselect path
	assert.True(t, equalViewDef(
		"SELECT sub.id FROM (SELECT users.id FROM users) sub",
		"SELECT sub.id FROM (SELECT id FROM public.users) sub",
	))
}

func TestEqualViewDef_funcCallArgs(t *testing.T) {
	// Covers FuncCall path
	assert.True(t, equalViewDef(
		"SELECT upper(users.name) FROM users",
		"SELECT upper(name) FROM public.users",
	))
}

func TestEqualViewDef_cte(t *testing.T) {
	// Covers WithClause/CTE path
	assert.True(t, equalViewDef(
		"WITH active AS (SELECT users.id FROM users) SELECT active.id FROM active",
		"WITH active AS (SELECT id FROM public.users) SELECT active.id FROM active",
	))
}

func TestEqualViewDef_inVsAnyArray(t *testing.T) {
	// pg_get_viewdef rewrites `IN ('a','b')` as `= ANY (ARRAY['a','b'])`.
	// Equality must hold across that rewrite for the WHERE clause.
	assert.True(t, equalViewDef(
		"SELECT id FROM t WHERE status = ANY (ARRAY['a', 'b'])",
		"SELECT id FROM t WHERE status IN ('a', 'b')",
	))
}

func TestEqualViewDef_inVsAnyArray_join(t *testing.T) {
	// Same rewrite, but on a JOIN ... ON expression.
	assert.True(t, equalViewDef(
		"SELECT u.id FROM users u JOIN orders o ON o.status = ANY (ARRAY['paid', 'shipped'])",
		"SELECT u.id FROM public.users u JOIN public.orders o ON o.status IN ('paid', 'shipped')",
	))
}

func TestEqualViewDef_currentOnlyTypeCast(t *testing.T) {
	// pg_get_viewdef adds a cast to the column's type on bare literals
	// (e.g. enum columns get `'x'::my_enum`). The desired SQL written
	// without the cast should still compare equal to the current form.
	// First arg is current (with cast), second is desired (without).
	assert.True(t, equalViewDef(
		"SELECT id FROM t WHERE status = 'published'::post_status",
		"SELECT id FROM t WHERE status = 'published'",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_inList(t *testing.T) {
	// Casts inside an IN list, after the ANY->IN rewrite.
	assert.True(t, equalViewDef(
		"SELECT id FROM t WHERE status = ANY (ARRAY['published'::post_status, 'pinned'::post_status])",
		"SELECT id FROM t WHERE status IN ('published', 'pinned')",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_notStrippedFromDesired(t *testing.T) {
	// alignCurrentCasts is asymmetric: a cast in desired that is missing
	// from current must surface as a difference (otherwise a user-requested
	// cast change would be silently hidden).
	assert.False(t, equalViewDef(
		"SELECT id FROM t WHERE x = 1",
		"SELECT id FROM t WHERE x = 1::bigint",
	))
}

// The tests below exercise each walker position in normalizeSelectExprs
// / alignSelectCasts beyond the already-covered WHERE and JOIN ON.

func TestEqualViewDef_inVsAnyArray_targetList(t *testing.T) {
	// Target-list expression: CASE inside SELECT.
	assert.True(t, equalViewDef(
		"SELECT CASE WHEN status = ANY (ARRAY['a', 'b']) THEN 1 ELSE 0 END AS hit FROM t",
		"SELECT CASE WHEN status IN ('a', 'b') THEN 1 ELSE 0 END AS hit FROM t",
	))
}

func TestEqualViewDef_inVsAnyArray_having(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT x, count(*) FROM t GROUP BY x HAVING x = ANY (ARRAY['a', 'b'])",
		"SELECT x, count(*) FROM t GROUP BY x HAVING x IN ('a', 'b')",
	))
}

func TestEqualViewDef_inVsAnyArray_groupBy(t *testing.T) {
	// GROUP BY can contain expressions (here a boolean test on status).
	// Covers the GroupClause walker position.
	assert.True(t, equalViewDef(
		"SELECT count(*) FROM t GROUP BY status = ANY (ARRAY['a', 'b'])",
		"SELECT count(*) FROM t GROUP BY status IN ('a', 'b')",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_groupBy(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT count(*) FROM t GROUP BY status = 'a'::e",
		"SELECT count(*) FROM t GROUP BY status = 'a'",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_join(t *testing.T) {
	// Covers the JoinExpr.Quals position in alignSelectCasts (the IN<->ANY
	// variant is already tested in inVsAnyArray_join).
	assert.True(t, equalViewDef(
		"SELECT u.id FROM users u JOIN orders o ON o.status = 'paid'::e",
		"SELECT u.id FROM public.users u JOIN public.orders o ON o.status = 'paid'",
	))
}

func TestEqualViewDef_inVsAnyArray_cte(t *testing.T) {
	assert.True(t, equalViewDef(
		"WITH active AS (SELECT id FROM t WHERE status = ANY (ARRAY['a', 'b'])) SELECT id FROM active",
		"WITH active AS (SELECT id FROM t WHERE status IN ('a', 'b')) SELECT id FROM active",
	))
}

func TestEqualViewDef_inVsAnyArray_union(t *testing.T) {
	// Both UNION arms (Larg / Rarg) must be normalized.
	assert.True(t, equalViewDef(
		"SELECT id FROM t WHERE x = ANY (ARRAY[1, 2]) UNION SELECT id FROM t WHERE y = ANY (ARRAY[3, 4])",
		"SELECT id FROM t WHERE x IN (1, 2) UNION SELECT id FROM t WHERE y IN (3, 4)",
	))
}

func TestEqualViewDef_inVsAnyArray_rangeSubselect(t *testing.T) {
	// Sub-SELECT in FROM (RangeSubselect path).
	assert.True(t, equalViewDef(
		"SELECT s.id FROM (SELECT id FROM t WHERE x = ANY (ARRAY[1, 2])) s",
		"SELECT s.id FROM (SELECT id FROM t WHERE x IN (1, 2)) s",
	))
}

func TestEqualViewDef_inVsAnyArray_sublink(t *testing.T) {
	// Sub-SELECT inside an EXISTS predicate (SubLink path).
	assert.True(t, equalViewDef(
		"SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.x = ANY (ARRAY[1, 2]))",
		"SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.x IN (1, 2))",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_targetList(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT CASE WHEN status = 'a'::e THEN 1 ELSE 0 END AS hit FROM t",
		"SELECT CASE WHEN status = 'a' THEN 1 ELSE 0 END AS hit FROM t",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_having(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT x, count(*) FROM t GROUP BY x HAVING x = 'a'::e",
		"SELECT x, count(*) FROM t GROUP BY x HAVING x = 'a'",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_cte(t *testing.T) {
	assert.True(t, equalViewDef(
		"WITH active AS (SELECT id FROM t WHERE status = 'a'::e) SELECT id FROM active",
		"WITH active AS (SELECT id FROM t WHERE status = 'a') SELECT id FROM active",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_union(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT id FROM t WHERE x = 'a'::e UNION SELECT id FROM t WHERE y = 'b'::e",
		"SELECT id FROM t WHERE x = 'a' UNION SELECT id FROM t WHERE y = 'b'",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_sublink(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.x = 'a'::e)",
		"SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.x = 'a')",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_rangeSubselect(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT s.id FROM (SELECT id FROM t WHERE status = 'a'::e) s",
		"SELECT s.id FROM (SELECT id FROM t WHERE status = 'a') s",
	))
}

func TestEqualViewDef_inVsAnyArray_orderBy(t *testing.T) {
	// ORDER BY can contain a comparison expression (sorts by the boolean
	// result). Covers the SortClause walker position.
	assert.True(t, equalViewDef(
		"SELECT id FROM t ORDER BY CASE WHEN status = ANY (ARRAY['a', 'b']) THEN 0 ELSE 1 END",
		"SELECT id FROM t ORDER BY CASE WHEN status IN ('a', 'b') THEN 0 ELSE 1 END",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_orderBy(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT id FROM t ORDER BY CASE WHEN status = 'a'::e THEN 0 ELSE 1 END",
		"SELECT id FROM t ORDER BY CASE WHEN status = 'a' THEN 0 ELSE 1 END",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_limit(t *testing.T) {
	// pg_get_viewdef can emit a LIMIT/OFFSET with a TypeCast attached to the
	// numeric literal. The desired SQL written as a bare integer must still
	// compare equal. Covers the LimitCount walker position.
	assert.True(t, equalViewDef(
		"SELECT id FROM t LIMIT 5::bigint",
		"SELECT id FROM t LIMIT 5",
	))
}

func TestEqualViewDef_currentOnlyTypeCast_offset(t *testing.T) {
	// Covers the LimitOffset walker position.
	assert.True(t, equalViewDef(
		"SELECT id FROM t OFFSET 0::bigint",
		"SELECT id FROM t OFFSET 0",
	))
}

func TestEqualViewDef_targetListTopLevelTextCast_added(t *testing.T) {
	// A user-added top-level cast on a target column changes the resulting
	// view column type. It must surface as a diff, even though
	// normalizeCheckExpr strips ::text/::varchar symmetrically elsewhere.
	assert.False(t, equalViewDef(
		"SELECT id FROM t",
		"SELECT id::text FROM t",
	))
}

func TestEqualViewDef_targetListTopLevelTextCast_removed(t *testing.T) {
	// A user-removed top-level cast (current has cast, desired doesn't)
	// also changes the view column type. alignCurrentCasts would normally
	// strip the current-only cast; at the top of a target list we must
	// not, so the diff still surfaces.
	assert.False(t, equalViewDef(
		"SELECT id::text FROM t",
		"SELECT id FROM t",
	))
}

func TestEqualViewDef_targetListTopLevelTextCast_bothPresent(t *testing.T) {
	// Both sides have the same top-level cast; no diff.
	assert.True(t, equalViewDef(
		"SELECT id::text FROM t",
		"SELECT id::text FROM t",
	))
}

func TestEqualViewDef_qualifiedColumnInsideTextCast(t *testing.T) {
	// A qualified ColumnRef nested under a text-like TypeCast (e.g.
	// `lower(users.name::text)`) must still be stripped to match the
	// unqualified bare form. stripQualifications must recurse into
	// TypeCast.Arg so the inner ColumnRef is reached before
	// normalizeSelectExprs collapses the surrounding cast.
	assert.True(t, equalViewDef(
		"SELECT lower(users.name::text) FROM users",
		"SELECT lower(name) FROM users",
	))
}

func TestEqualViewDef_inSubquery_testexprQualified(t *testing.T) {
	// `users.id IN (SELECT ...)` parses to SubLink{Testexpr: users.id, ...}.
	// stripQualifications must recurse into Testexpr or the table-qualified
	// LHS won't match the unqualified desired form.
	assert.True(t, equalViewDef(
		"SELECT id FROM users WHERE users.id IN (SELECT user_id FROM orders)",
		"SELECT id FROM public.users WHERE id IN (SELECT user_id FROM public.orders)",
	))
}

func TestEqualViewDef_realChangeStillDetected(t *testing.T) {
	// Regression guard: after all the normalizations, a genuinely different
	// view body must still surface as a difference.
	assert.False(t, equalViewDef(
		"SELECT id FROM t WHERE status = ANY (ARRAY['a', 'b'])",
		"SELECT id FROM t WHERE status IN ('a', 'b', 'c')",
	))
}

func TestDiffViews_newMatview(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n",
		Indexes:    orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE MATERIALIZED VIEW public.mv")
}

func TestDiffViews_newMatviewWithIndex(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	desired := orderedmap.New[string, *model.View]()
	mv := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n",
		Indexes:    orderedmap.New[string, *model.Index](),
	}
	mv.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition: "CREATE INDEX idx_mv_n ON public.mv USING btree (n)",
	})
	desired.Set("public.mv", mv)

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.CreateStmts, 2)
	assert.Contains(t, result.CreateStmts[0], "CREATE MATERIALIZED VIEW")
	assert.Contains(t, result.CreateStmts[1], "CREATE INDEX idx_mv_n")
}

func TestDiffViews_dropMatview(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.DropStmts, 1)
	assert.Contains(t, result.DropStmts[0], "DROP MATERIALIZED VIEW public.mv")
}

func TestDiffViews_dropMatview_denied(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()

	result, err := DiffViews(current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Equal(t, []string{"-- skipped: DROP MATERIALIZED VIEW public.mv;"}, result.DisallowedDropStmts)
}

func TestDiffViews_modifyMatview(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 2 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	// Materialized views must be dropped and recreated
	assert.Len(t, result.DropStmts, 1)
	assert.Contains(t, result.DropStmts[0], "DROP MATERIALIZED VIEW")
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE MATERIALIZED VIEW")
}

func TestDiffViews_modifyMatview_preservesComment(t *testing.T) {
	comment := "user statistics"
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Comment: &comment,
		Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 2 AS n", Comment: &comment,
		Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	// DROP+CREATE loses the comment, so it must be re-applied
	assert.Len(t, result.DropStmts, 1)
	require.Len(t, result.CreateStmts, 2)
	assert.Contains(t, result.CreateStmts[0], "CREATE MATERIALIZED VIEW")
	assert.Contains(t, result.CreateStmts[1], "COMMENT ON MATERIALIZED VIEW")
}

func TestDiffViews_modifyMatview_dropDenied(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 2 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, denyAllDrops{})
	require.NoError(t, err)
	// Drop denied: no executable DROP or CREATE; the suppressed recreation
	// is surfaced as a skipped comment so users can see what was blocked.
	assert.Empty(t, result.DropStmts)
	assert.Empty(t, result.CreateStmts)
	assert.Equal(t, []string{"-- skipped: DROP MATERIALIZED VIEW public.mv;"}, result.DisallowedDropStmts)
}

func TestDiffViews_modifyMatview_dropDenied_withCommentChange(t *testing.T) {
	// When a matview definition change is blocked by --allow-drop and the
	// desired matview also has a different comment, the comment change must
	// NOT be emitted: the matview on disk still has the old definition, so
	// only suppressing the recreation but updating the comment would be a
	// half-applied change. The whole recreation is skipped instead.
	oldComment := "old"
	newComment := "new"
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Comment: &oldComment,
		Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 2 AS n", Comment: &newComment,
		Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Empty(t, result.CreateStmts, "no executable DDL when recreation is denied, even if comment differs")
	assert.Equal(t, []string{"-- skipped: DROP MATERIALIZED VIEW public.mv;"}, result.DisallowedDropStmts)
}

func TestDiffViews_matviewIndexAdd(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	mv := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	}
	mv.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition: "CREATE INDEX idx_mv_n ON public.mv USING btree (n)",
	})
	desired.Set("public.mv", mv)

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE INDEX idx_mv_n")
}

func TestDiffViews_matviewIndexDrop(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	mv := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	}
	mv.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition: "CREATE INDEX idx_mv_n ON public.mv USING btree (n)",
	})
	current.Set("public.mv", mv)
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "DROP INDEX")
}

func TestDiffViews_matviewIndexAdd_concurrently_parseError(t *testing.T) {
	// createIndexSQL with concurrently=true parses the definition through
	// pg_query; an unparseable definition surfaces as a wrapped error from
	// diffViewIndexes.
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	mv := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	}
	mv.Indexes.Set("idx_bad", &model.Index{
		Schema: "public", Name: "idx_bad", Table: "mv",
		Definition:   "NOT VALID SQL {{{{",
		Concurrently: true,
	})
	desired.Set("public.mv", mv)

	_, err := DiffViews(current, desired, allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create index")
}

func TestDiffViews_matviewIndexDrop_denied(t *testing.T) {
	// Matview definition unchanged, only its index is removed.
	// With --allow-drop denying index drops, the DROP INDEX is suppressed.
	current := orderedmap.New[string, *model.View]()
	mv := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	}
	mv.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition: "CREATE INDEX idx_mv_n ON public.mv USING btree (n)",
	})
	current.Set("public.mv", mv)
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Empty(t, result.CreateStmts)
	assert.Equal(t, []string{"-- skipped: DROP INDEX public.idx_mv_n;"}, result.DisallowedDropStmts)
}

func TestDiffViews_matviewCommentAdd(t *testing.T) {
	comment := "stats"
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1", Comment: &comment, Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "COMMENT ON MATERIALIZED VIEW")
}

func TestDiffViews_matviewCommentDrop(t *testing.T) {
	comment := "stats"
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1", Comment: &comment, Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1", Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "COMMENT ON MATERIALIZED VIEW public.mv IS NULL")
}

func TestDiffViews_matviewIndexChange(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	mvCurrent := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	}
	mvCurrent.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition: "CREATE INDEX idx_mv_n ON public.mv USING btree (n)",
	})
	current.Set("public.mv", mvCurrent)

	desired := orderedmap.New[string, *model.View]()
	mvDesired := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	}
	mvDesired.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition: "CREATE UNIQUE INDEX idx_mv_n ON public.mv USING btree (n)",
	})
	desired.Set("public.mv", mvDesired)

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	require.Len(t, result.CreateStmts, 2)
	assert.Contains(t, result.CreateStmts[0], "DROP INDEX")
	assert.Contains(t, result.CreateStmts[1], "CREATE UNIQUE INDEX")
}

func TestDiffViews_matviewNoChange(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Empty(t, result.CreateStmts)
}

func TestDiffViews_renameMatview(t *testing.T) {
	old := "public.old_mv"
	current := orderedmap.New[string, *model.View]()
	current.Set("public.old_mv", &model.View{
		Schema: "public", Name: "old_mv", Materialized: true,
		Definition: "SELECT 1", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.new_mv", &model.View{
		Schema: "public", Name: "new_mv", Materialized: true,
		Definition: "SELECT 1", RenameFrom: &old,
		Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	require.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "ALTER MATERIALIZED VIEW")
	assert.Contains(t, result.CreateStmts[0], "RENAME TO new_mv")
}

func TestDiffViews_viewToMatview(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v", &model.View{
		Schema: "public", Name: "v", Materialized: false,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v", &model.View{
		Schema: "public", Name: "v", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.DropStmts, 1)
	assert.Contains(t, result.DropStmts[0], "DROP VIEW public.v")
	require.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE MATERIALIZED VIEW")
}

func TestDiffViews_matviewToView(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v", &model.View{
		Schema: "public", Name: "v", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v", &model.View{
		Schema: "public", Name: "v", Materialized: false,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.DropStmts, 1)
	assert.Contains(t, result.DropStmts[0], "DROP MATERIALIZED VIEW public.v")
	require.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE OR REPLACE VIEW")
}

func TestDiffViews_viewToMatview_dropDenied(t *testing.T) {
	comment := "my view"
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v", &model.View{
		Schema: "public", Name: "v", Materialized: false,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v", &model.View{
		Schema: "public", Name: "v", Materialized: true,
		Definition: "SELECT 1 AS n", Comment: &comment,
		Indexes: orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, denyAllDrops{})
	require.NoError(t, err)
	// Type change denied: no executable DROP/CREATE/comment change; the
	// suppressed recreation is surfaced as a skipped comment.
	assert.Empty(t, result.DropStmts)
	assert.Empty(t, result.CreateStmts)
	assert.Equal(t, []string{"-- skipped: DROP VIEW public.v;"}, result.DisallowedDropStmts)
}

func TestDiffViews_renameTypeMismatch(t *testing.T) {
	old := "public.old_v"
	current := orderedmap.New[string, *model.View]()
	current.Set("public.old_v", &model.View{
		Schema: "public", Name: "old_v", Materialized: false,
		Definition: "SELECT 1", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.new_v", &model.View{
		Schema: "public", Name: "new_v", Materialized: true,
		Definition: "SELECT 1", RenameFrom: &old,
		Indexes: orderedmap.New[string, *model.Index](),
	})

	_, err := DiffViews(current, desired, allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "view type mismatch")
}

func TestDiffViews_newMatviewWithIndex_perIndexDirective(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	desired := orderedmap.New[string, *model.View]()
	mv := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n",
		Indexes:    orderedmap.New[string, *model.Index](),
	}
	mv.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition:   "CREATE INDEX idx_mv_n ON public.mv USING btree (n)",
		Concurrently: true,
	})
	desired.Set("public.mv", mv)

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.CreateStmts, 2)
	assert.Contains(t, result.CreateStmts[0], "CREATE MATERIALIZED VIEW")
	assert.Equal(t, "CREATE INDEX CONCURRENTLY idx_mv_n ON public.mv USING btree (n);", result.CreateStmts[1])
}

func TestDiffViews_matviewIndexChange_perIndexDirective(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	mvCurrent := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	}
	mvCurrent.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition: "CREATE INDEX idx_mv_n ON public.mv USING btree (n)",
	})
	current.Set("public.mv", mvCurrent)

	desired := orderedmap.New[string, *model.View]()
	mvDesired := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	}
	mvDesired.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition:   "CREATE UNIQUE INDEX idx_mv_n ON public.mv USING btree (n)",
		Concurrently: true,
	})
	desired.Set("public.mv", mvDesired)

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.CreateStmts, 2)
	assert.Equal(t, "DROP INDEX CONCURRENTLY public.idx_mv_n;", result.CreateStmts[0])
	assert.Equal(t, "CREATE UNIQUE INDEX CONCURRENTLY idx_mv_n ON public.mv USING btree (n);", result.CreateStmts[1])
}

func TestDiffViews_matviewIndexAdd_perIndexDirective(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	mv := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	}
	mv.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition:   "CREATE INDEX idx_mv_n ON public.mv USING btree (n)",
		Concurrently: true,
	})
	mv.Indexes.Set("idx_mv_n2", &model.Index{
		Schema: "public", Name: "idx_mv_n2", Table: "mv",
		Definition: "CREATE INDEX idx_mv_n2 ON public.mv USING btree (n)",
	})
	desired.Set("public.mv", mv)

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 2)
	assert.Equal(t, "CREATE INDEX CONCURRENTLY idx_mv_n ON public.mv USING btree (n);", result.CreateStmts[0])
	assert.Equal(t, "CREATE INDEX idx_mv_n2 ON public.mv USING btree (n);", result.CreateStmts[1])
}

func TestDiffViews_modifyMatviewWithIndex_perIndexDirective(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n", Indexes: orderedmap.New[string, *model.Index](),
	})
	desired := orderedmap.New[string, *model.View]()
	mv := &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 2 AS n", Indexes: orderedmap.New[string, *model.Index](),
	}
	mv.Indexes.Set("idx_mv_n", &model.Index{
		Schema: "public", Name: "idx_mv_n", Table: "mv",
		Definition:   "CREATE INDEX idx_mv_n ON public.mv USING btree (n)",
		Concurrently: true,
	})
	desired.Set("public.mv", mv)

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.DropStmts, 1)
	require.Len(t, result.CreateStmts, 2)
	assert.Contains(t, result.CreateStmts[0], "CREATE MATERIALIZED VIEW")
	assert.Equal(t, "CREATE INDEX CONCURRENTLY idx_mv_n ON public.mv USING btree (n);", result.CreateStmts[1])
}

func TestCanCreateOrReplaceView(t *testing.T) {
	tests := []struct {
		name    string
		current string
		desired string
		want    bool
	}{
		{
			name:    "identical column list",
			current: "SELECT id, name FROM t",
			desired: "SELECT id, name FROM t WHERE active",
			want:    true,
		},
		{
			name:    "append column at end (allowed by PG)",
			current: "SELECT id, name FROM t",
			desired: "SELECT id, name, email FROM t",
			want:    true,
		},
		{
			name:    "remove column",
			current: "SELECT id, name FROM t",
			desired: "SELECT id FROM t",
			want:    false,
		},
		{
			name:    "rename column via alias",
			current: "SELECT id, name FROM t",
			desired: "SELECT id, name AS display FROM t",
			want:    false,
		},
		{
			name:    "reorder columns",
			current: "SELECT id, name FROM t",
			desired: "SELECT name, id FROM t",
			want:    false,
		},
		{
			name:    "desired uses SELECT * (cannot analyze -> safer DROP+CREATE)",
			current: "SELECT id FROM t",
			desired: "SELECT * FROM t",
			want:    false,
		},
		{
			name:    "current is a UNION (first SELECT decides column names)",
			current: "SELECT id, name FROM t1 UNION SELECT id, name FROM t2",
			desired: "SELECT id, name FROM t1 UNION SELECT id, name FROM t2 WHERE active",
			want:    true,
		},
		{
			name:    "expression without alias (unanalyzable)",
			current: "SELECT id, length(name) FROM t",
			desired: "SELECT id, upper(name) FROM t",
			want:    false,
		},
		{
			name:    "parse error on one side",
			current: "SELECT id FROM t",
			desired: "NOT A VIEW",
			want:    false,
		},
		{
			name:    "CTE in view body (outer SELECT decides)",
			current: "WITH c AS (SELECT id FROM t) SELECT id FROM c",
			desired: "WITH c AS (SELECT id, name FROM t) SELECT id FROM c",
			want:    true,
		},
		{
			name:    "INTERSECT (first SELECT decides)",
			current: "SELECT id FROM t INTERSECT SELECT id FROM u",
			desired: "SELECT id FROM t WHERE active INTERSECT SELECT id FROM u",
			want:    true,
		},
		{
			name:    "EXCEPT (first SELECT decides)",
			current: "SELECT id FROM t EXCEPT SELECT id FROM u",
			desired: "SELECT id, name FROM t EXCEPT SELECT id, name FROM u",
			want:    true,
		},
		{
			name:    "aliased expression keeps the alias as the column name",
			current: "SELECT a + b AS sum FROM t",
			desired: "SELECT a + b + 1 AS sum FROM t",
			want:    true,
		},
		{
			name:    "table-qualified column resolves to bare name",
			current: "SELECT t.id, name FROM t",
			desired: "SELECT id, name FROM t",
			want:    true,
		},
		{
			name:    "t.* (unanalyzable)",
			current: "SELECT id FROM t",
			desired: "SELECT t.* FROM t",
			want:    false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := canCreateOrReplaceView(tc.current, tc.desired)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestViewOutputColumns_NegativePaths(t *testing.T) {
	// Direct table-driven coverage for viewOutputColumns /
	// selectOutputColumns input forms that canCreateOrReplaceView only
	// hits incidentally; locking the ok=false branches against future
	// refactors and trimming the patch's uncovered-line count.
	tests := []struct {
		name string
		body string
	}{
		// pg_query accepts a bare `SELECT` (no target list) without
		// reporting a syntax error, producing a SelectStmt with an
		// empty TargetList. The function must reject it so callers
		// fall back to DROP+CREATE.
		{"empty TargetList", "SELECT"},
		// SELECT * is unanalyzable: until the catalog expands the star
		// to real column names there's no list to compare against.
		{"SELECT *", "SELECT * FROM t"},
		// Computed expression with no alias has no stable output name.
		{"bare expression target", "SELECT a + b FROM t"},
		// Parse failures short-circuit early.
		{"parse error", "this is not SQL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := viewOutputColumns(tc.body)
			assert.False(t, ok)
		})
	}
}

func TestDiffViews_renamePlusRecreateDropsOldNameAndSkipsRename(t *testing.T) {
	// A view that is both renamed AND has a column-shape change needs
	// DROP+CREATE. The DROP has to target the old name (the DB hasn't
	// renamed yet) and the ALTER RENAME must be suppressed; otherwise
	// the apply runs `DROP VIEW <new name>` first (no such view) and
	// fails before the rename can move the row.
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v_old", &model.View{
		Schema: "public", Name: "v_old",
		Definition: "SELECT id, slug FROM t",
	})
	oldKey := "public.v_old"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v_new", &model.View{
		Schema: "public", Name: "v_new", RenameFrom: &oldKey,
		Definition: "SELECT id FROM t",
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.DropStmts, 1)
	assert.Equal(t, "DROP VIEW public.v_old;", result.DropStmts[0])
	require.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE OR REPLACE VIEW public.v_new")
	// Sanity: no stray ALTER VIEW ... RENAME in the plan.
	for _, s := range result.CreateStmts {
		assert.NotContains(t, s, "RENAME TO")
	}
}

func TestDiffViews_renameWithoutDefinitionChangeKeepsRename(t *testing.T) {
	// Pure rename (definition unchanged) must still emit ALTER RENAME;
	// the new behavior only suppresses the rename when DROP+CREATE is
	// also required.
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v_old", &model.View{
		Schema: "public", Name: "v_old",
		Definition: "SELECT id FROM t",
	})
	oldKey := "public.v_old"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v_new", &model.View{
		Schema: "public", Name: "v_new", RenameFrom: &oldKey,
		Definition: "SELECT id FROM t",
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	require.Len(t, result.CreateStmts, 1)
	assert.Equal(t, "ALTER VIEW public.v_old RENAME TO v_new;", result.CreateStmts[0])
}

func TestDiffViews_renamePlusRecreateDropDeniedSkipsOldName(t *testing.T) {
	// Mirror of the executable-drop fix for the denied-drop branch:
	// when --allow-drop forbids the view drop, the `-- skipped:` comment
	// has to point at the relation that actually exists (the old name),
	// not the renamed-but-not-yet-applied new name.
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v_old", &model.View{
		Schema: "public", Name: "v_old",
		Definition: "SELECT id, slug FROM t",
	})
	oldKey := "public.v_old"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v_new", &model.View{
		Schema: "public", Name: "v_new", RenameFrom: &oldKey,
		Definition: "SELECT id FROM t",
	})

	result, err := DiffViews(current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Empty(t, result.CreateStmts)
	require.Len(t, result.DisallowedDropStmts, 1)
	assert.Equal(t, "-- skipped: DROP VIEW public.v_old;", result.DisallowedDropStmts[0])
}

func TestDiffViews_renameWithAppendOnlyChangeKeepsRename(t *testing.T) {
	// Rename + append-only change uses CREATE OR REPLACE (not DROP+CREATE).
	// The rename has to survive, then the in-place replace updates the body.
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v_old", &model.View{
		Schema: "public", Name: "v_old",
		Definition: "SELECT id FROM t",
	})
	oldKey := "public.v_old"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v_new", &model.View{
		Schema: "public", Name: "v_new", RenameFrom: &oldKey,
		Definition: "SELECT id, name FROM t",
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	require.Len(t, result.CreateStmts, 2)
	assert.Equal(t, "ALTER VIEW public.v_old RENAME TO v_new;", result.CreateStmts[0])
	assert.Contains(t, result.CreateStmts[1], "CREATE OR REPLACE VIEW public.v_new")
}

func TestDiffViews_columnRemovedTriggersDropCreate(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v", &model.View{
		Schema: "public", Name: "v",
		Definition: "SELECT id, slug FROM t",
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v", &model.View{
		Schema: "public", Name: "v",
		Definition: "SELECT id FROM t",
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.DropStmts, 1)
	assert.Equal(t, "DROP VIEW public.v;", result.DropStmts[0])
	require.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "SELECT id FROM t")
}

func TestDiffViews_columnAppendedKeepsCreateOrReplace(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v", &model.View{
		Schema: "public", Name: "v",
		Definition: "SELECT id FROM t",
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v", &model.View{
		Schema: "public", Name: "v",
		Definition: "SELECT id, name FROM t",
	})

	result, err := DiffViews(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	require.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE OR REPLACE VIEW")
}

func TestDiffViews_columnRemovedDropDeniedFallsBackToCommented(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v", &model.View{
		Schema: "public", Name: "v",
		Definition: "SELECT id, slug FROM t",
	})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v", &model.View{
		Schema: "public", Name: "v",
		Definition: "SELECT id FROM t",
	})

	result, err := DiffViews(current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Empty(t, result.CreateStmts)
	assert.Equal(t, []string{"-- skipped: DROP VIEW public.v;"}, result.DisallowedDropStmts)
}
