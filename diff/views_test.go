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

func TestEqualViewDef_implicitTextCast(t *testing.T) {
	// pg_get_viewdef adds ::text to string literals; this should be ignored
	assert.True(t, equalViewDef(
		"SELECT id FROM items WHERE label = 'active'::text",
		"SELECT id FROM items WHERE label = 'active'",
	))
}

func TestEqualViewDef_implicitTextCastMultiple(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT id FROM items WHERE label = 'active'::text AND status = 'enabled'::text",
		"SELECT id FROM items WHERE label = 'active' AND status = 'enabled'",
	))
}

func TestEqualViewDef_nonTextCastPreserved(t *testing.T) {
	// ::integer cast is meaningful and should NOT be stripped
	assert.False(t, equalViewDef(
		"SELECT id FROM items WHERE id = '123'::integer",
		"SELECT id FROM items WHERE id = '123'",
	))
}

func TestEqualViewDef_textCastOnNonStringPreserved(t *testing.T) {
	// ::text on a non-string-literal (column ref) is meaningful
	assert.False(t, equalViewDef(
		"SELECT id::text FROM items",
		"SELECT id FROM items",
	))
}

func TestEqualViewDef_implicitTextCastInSubquery(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT id FROM items WHERE label IN (SELECT name FROM categories WHERE name = 'food'::text)",
		"SELECT id FROM items WHERE label IN (SELECT name FROM categories WHERE name = 'food')",
	))
}

func TestEqualViewDef_implicitTextCastInCaseExpr(t *testing.T) {
	assert.True(t, equalViewDef(
		"SELECT CASE WHEN label = 'active'::text THEN 'yes'::text ELSE 'no'::text END FROM items",
		"SELECT CASE WHEN label = 'active' THEN 'yes' ELSE 'no' END FROM items",
	))
}
