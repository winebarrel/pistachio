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

	result, err := DiffViews(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE OR REPLACE VIEW public.v1")
}

func TestDiffViews_dropView(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()

	result, err := DiffViews(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP VIEW public.v1;"}, result.DropStmts)
}

func TestDiffViews_dropView_denied(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()

	result, err := DiffViews(current, desired, DenyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.CreateStmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffViews_modifyView(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 2"})

	result, err := DiffViews(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE OR REPLACE VIEW public.v1")
}

func TestDiffViews_noChange(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.CreateStmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffViews_formattingDifferenceIgnored(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT   1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.CreateStmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffViews_commentAdd(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1", Comment: ptr("my view")})

	result, err := DiffViews(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.CreateStmts, 1)
	assert.Equal(t, "COMMENT ON VIEW public.v1 IS 'my view';", result.CreateStmts[0])
}

func TestDiffViews_commentDrop(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1", Comment: ptr("my view")})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER VIEW public.v1 RENAME TO v2;"}, result.CreateStmts)
}

func TestDiffViews_rename_selfRename_skipped(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	oldName := "public.v1"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", RenameFrom: &oldName, Definition: "SELECT 1"})

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	_, err := DiffViews(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffViews_rename_crossSchema_error(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	oldName := "public.v1"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("other.v2", &model.View{Schema: "other", Name: "v2", RenameFrom: &oldName, Definition: "SELECT 1"})

	_, err := DiffViews(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cross-schema rename")
}

func TestDiffViews_rename_sourceNotFound(t *testing.T) {
	current := orderedmap.New[string, *model.View]()

	oldName := "public.nonexistent"
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v2", &model.View{Schema: "public", Name: "v2", RenameFrom: &oldName, Definition: "SELECT 1"})

	_, err := DiffViews(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source")
}

func TestNormalizeViewDef(t *testing.T) {
	got, err := normalizeViewDef("SELECT   1")
	assert.NoError(t, err)
	assert.Contains(t, got, "SELECT 1")
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

func TestDiffViews_newMatview(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.mv", &model.View{
		Schema: "public", Name: "mv", Materialized: true,
		Definition: "SELECT 1 AS n",
		Indexes:    orderedmap.New[string, *model.Index](),
	})

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, DenyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	// Materialized views must be dropped and recreated
	assert.Len(t, result.DropStmts, 1)
	assert.Contains(t, result.DropStmts[0], "DROP MATERIALIZED VIEW")
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "CREATE MATERIALIZED VIEW")
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

	result, err := DiffViews(current, desired, DenyAllDrops{})
	require.NoError(t, err)
	// Drop denied: no DROP or CREATE should be generated
	assert.Empty(t, result.DropStmts)
	assert.Empty(t, result.CreateStmts)
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Len(t, result.CreateStmts, 1)
	assert.Contains(t, result.CreateStmts[0], "DROP INDEX")
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, AllowAllDrops{})
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

	result, err := DiffViews(current, desired, DenyAllDrops{})
	require.NoError(t, err)
	// Type change denied: no DROP, no CREATE, no comment change
	assert.Empty(t, result.DropStmts)
	assert.Empty(t, result.CreateStmts)
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

	_, err := DiffViews(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "view type mismatch")
}
