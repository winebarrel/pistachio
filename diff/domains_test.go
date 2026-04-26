package diff_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/model"
)

func newDomainMap(domains ...*model.Domain) *orderedmap.Map[string, *model.Domain] {
	m := orderedmap.New[string, *model.Domain]()
	for _, d := range domains {
		m.Set(d.FQDN(), d)
	}
	return m
}

func TestDiffDomains_CreateNew(t *testing.T) {
	current := newDomainMap()
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE DOMAIN public.pos_int AS integer")
	assert.Empty(t, result.DropStmts)
}

func TestDiffDomains_CreateWithComment(t *testing.T) {
	comment := "Positive int"
	current := newDomainMap()
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer", Comment: &comment})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 2)
	assert.Contains(t, result.Stmts[1], "COMMENT ON DOMAIN")
}

func TestDiffDomains_Drop(t *testing.T) {
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	desired := newDomainMap()
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Equal(t, []string{"DROP DOMAIN public.pos_int;"}, result.DropStmts)
}

func TestDiffDomains_Drop_Denied(t *testing.T) {
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	desired := newDomainMap()
	result, err := diff.DiffDomains(current, desired, diff.DenyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
	assert.Equal(t, []string{"-- skipped: DROP DOMAIN public.pos_int;"}, result.DisallowedDropStmts)
}

func TestDiffDomains_NoDiff(t *testing.T) {
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffDomains_SetDefault(t *testing.T) {
	def := "0"
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer", Default: &def})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER DOMAIN public.pos_int SET DEFAULT 0;"}, result.Stmts)
}

func TestDiffDomains_DropDefault(t *testing.T) {
	def := "0"
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer", Default: &def})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER DOMAIN public.pos_int DROP DEFAULT;"}, result.Stmts)
}

func TestDiffDomains_SetNotNull(t *testing.T) {
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer", NotNull: true})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER DOMAIN public.pos_int SET NOT NULL;"}, result.Stmts)
}

func TestDiffDomains_DropNotNull(t *testing.T) {
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer", NotNull: true})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER DOMAIN public.pos_int DROP NOT NULL;"}, result.Stmts)
}

func TestDiffDomains_AddConstraint(t *testing.T) {
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	desired := newDomainMap(&model.Domain{
		Schema:   "public",
		Name:     "pos_int",
		BaseType: "integer",
		Constraints: []*model.DomainConstraint{
			{Name: "pos_check", Definition: "CHECK (VALUE > 0)"},
		},
	})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "ADD CONSTRAINT pos_check")
}

func TestDiffDomains_DropConstraint(t *testing.T) {
	current := newDomainMap(&model.Domain{
		Schema:   "public",
		Name:     "pos_int",
		BaseType: "integer",
		Constraints: []*model.DomainConstraint{
			{Name: "pos_check", Definition: "CHECK (VALUE > 0)"},
		},
	})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "DROP CONSTRAINT pos_check")
}

func TestDiffDomains_CollationChange_Error(t *testing.T) {
	colA := "pg_catalog.C"
	colB := "pg_catalog.POSIX"
	current := newDomainMap(&model.Domain{Schema: "public", Name: "name", BaseType: "text", Collation: &colA})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "name", BaseType: "text", Collation: &colB})
	_, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot change collation")
}

func TestDiffDomains_BaseTypeChange_Error(t *testing.T) {
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "bigint"})
	_, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot change base type")
}

func TestDiffDomains_AddComment(t *testing.T) {
	comment := "Positive int"
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer", Comment: &comment})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"COMMENT ON DOMAIN public.pos_int IS 'Positive int';"}, result.Stmts)
}

func TestDiffDomains_DropComment(t *testing.T) {
	comment := "Positive int"
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer", Comment: &comment})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"COMMENT ON DOMAIN public.pos_int IS NULL;"}, result.Stmts)
}

func TestDiffDomains_Rename(t *testing.T) {
	oldName := "public.pos_int"
	current := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "positive_int", RenameFrom: &oldName, BaseType: "integer"})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER DOMAIN public.pos_int RENAME TO positive_int;"}, result.Stmts)
}

func TestDiffDomains_Rename_AlreadyApplied(t *testing.T) {
	oldName := "public.old"
	current := newDomainMap(&model.Domain{Schema: "public", Name: "positive_int", BaseType: "integer"})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "positive_int", RenameFrom: &oldName, BaseType: "integer"})
	result, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
}

func TestDiffDomains_Rename_SourceNotFound(t *testing.T) {
	oldName := "public.nonexistent"
	current := newDomainMap()
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", RenameFrom: &oldName, BaseType: "integer"})
	_, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source")
}

func TestDiffDomains_Rename_CrossSchema_Error(t *testing.T) {
	oldName := "other.pos_int"
	current := newDomainMap(&model.Domain{Schema: "other", Name: "pos_int", BaseType: "integer"})
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "pos_int", RenameFrom: &oldName, BaseType: "integer"})
	_, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cross-schema rename")
}

func TestDiffDomains_Rename_DestinationExists_Error(t *testing.T) {
	oldName := "public.pos_int"
	current := newDomainMap(
		&model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"},
		&model.Domain{Schema: "public", Name: "positive_int", BaseType: "integer"},
	)
	desired := newDomainMap(&model.Domain{Schema: "public", Name: "positive_int", RenameFrom: &oldName, BaseType: "integer"})
	_, err := diff.DiffDomains(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}
