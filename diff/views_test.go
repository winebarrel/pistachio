package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func TestDiffViews_newView(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	stmts := DiffViews(current, desired)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "CREATE OR REPLACE VIEW public.v1")
}

func TestDiffViews_dropView(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()

	stmts := DiffViews(current, desired)
	assert.Equal(t, []string{"DROP VIEW public.v1;"}, stmts)
}

func TestDiffViews_modifyView(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 2"})

	stmts := DiffViews(current, desired)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "CREATE OR REPLACE VIEW public.v1")
}

func TestDiffViews_noChange(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	stmts := DiffViews(current, desired)
	assert.Empty(t, stmts)
}

func TestDiffViews_formattingDifferenceIgnored(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT   1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	stmts := DiffViews(current, desired)
	assert.Empty(t, stmts)
}

func TestDiffViews_commentAdd(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1", Comment: ptr("my view")})

	stmts := DiffViews(current, desired)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "COMMENT ON VIEW public.v1 IS 'my view';", stmts[0])
}

func TestDiffViews_commentDrop(t *testing.T) {
	current := orderedmap.New[string, *model.View]()
	current.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1", Comment: ptr("my view")})
	desired := orderedmap.New[string, *model.View]()
	desired.Set("public.v1", &model.View{Schema: "public", Name: "v1", Definition: "SELECT 1"})

	stmts := DiffViews(current, desired)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "COMMENT ON VIEW public.v1 IS NULL;", stmts[0])
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
