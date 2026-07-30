package diff_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/model"
)

// allowCompositeTypeDrops permits only composite_type drops.
type allowCompositeTypeDrops struct{}

func (allowCompositeTypeDrops) IsDropAllowed(objectType string) bool {
	return objectType == "composite_type"
}

func newCompositeTypeMap(cts ...*model.CompositeType) *orderedmap.Map[string, *model.CompositeType] {
	m := orderedmap.New[string, *model.CompositeType]()
	for _, ct := range cts {
		m.Set(ct.FQCN(), ct)
	}
	return m
}

func ct(name string, attrs ...*model.CompositeAttribute) *model.CompositeType {
	return &model.CompositeType{Schema: "public", Name: name, Attributes: attrs}
}

func attr(name, typeName string) *model.CompositeAttribute {
	return &model.CompositeAttribute{Name: name, TypeName: typeName}
}

func TestDiffCompositeTypes_CreateNew(t *testing.T) {
	current := newCompositeTypeMap()
	desired := newCompositeTypeMap(ct("address", attr("street", "text"), attr("city", "text")))
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE TYPE public.address AS (")
	assert.Empty(t, result.DropStmts)
}

func TestDiffCompositeTypes_DropExisting(t *testing.T) {
	current := newCompositeTypeMap(ct("address", attr("street", "text")))
	desired := newCompositeTypeMap()
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Equal(t, []string{"DROP TYPE public.address;"}, result.DropStmts)
}

func TestDiffCompositeTypes_DropDisallowed(t *testing.T) {
	current := newCompositeTypeMap(ct("address", attr("street", "text")))
	desired := newCompositeTypeMap()
	result, err := diff.DiffCompositeTypes(current, desired, diff.DenyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Equal(t, []string{"-- skipped: DROP TYPE public.address;"}, result.DisallowedDropStmts)
}

func TestDiffCompositeTypes_AddAttribute(t *testing.T) {
	current := newCompositeTypeMap(ct("address", attr("street", "text")))
	desired := newCompositeTypeMap(ct("address", attr("street", "text"), attr("city", "text")))
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.address ADD ATTRIBUTE city text;"}, result.Stmts)
}

func TestDiffCompositeTypes_DropAttribute(t *testing.T) {
	current := newCompositeTypeMap(ct("address", attr("street", "text"), attr("zip", "text")))
	desired := newCompositeTypeMap(ct("address", attr("street", "text")))
	result, err := diff.DiffCompositeTypes(current, desired, allowCompositeTypeDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.address DROP ATTRIBUTE zip;"}, result.Stmts)
}

func TestDiffCompositeTypes_DropAttributeDisallowed(t *testing.T) {
	current := newCompositeTypeMap(ct("address", attr("street", "text"), attr("zip", "text")))
	desired := newCompositeTypeMap(ct("address", attr("street", "text")))
	result, err := diff.DiffCompositeTypes(current, desired, diff.DenyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Equal(t, []string{"-- skipped: ALTER TYPE public.address DROP ATTRIBUTE zip;"}, result.DisallowedDropStmts)
}

func TestDiffCompositeTypes_AlterAttributeType(t *testing.T) {
	current := newCompositeTypeMap(ct("address", attr("city", "text")))
	desired := newCompositeTypeMap(ct("address", attr("city", "character varying(100)")))
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.address ALTER ATTRIBUTE city TYPE character varying(100);"}, result.Stmts)
}

func TestDiffCompositeTypes_RenameType(t *testing.T) {
	current := newCompositeTypeMap(ct("address", attr("street", "text")))
	renameFrom := "public.address"
	desired := newCompositeTypeMap(&model.CompositeType{
		Schema:     "public",
		Name:       "postal_address",
		RenameFrom: &renameFrom,
		Attributes: []*model.CompositeAttribute{attr("street", "text")},
	})
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.address RENAME TO postal_address;"}, result.Stmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffCompositeTypes_RenameAttribute(t *testing.T) {
	current := newCompositeTypeMap(ct("address", attr("street", "text"), attr("city", "text")))
	renameFrom := "street"
	desired := newCompositeTypeMap(&model.CompositeType{
		Schema: "public",
		Name:   "address",
		Attributes: []*model.CompositeAttribute{
			{Name: "road", TypeName: "text", RenameFrom: &renameFrom},
			attr("city", "text"),
		},
	})
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.address RENAME ATTRIBUTE street TO road;"}, result.Stmts)
}

func TestDiffCompositeTypes_RenameAttributeDestinationExists(t *testing.T) {
	current := newCompositeTypeMap(ct("address", attr("street", "text"), attr("road", "text")))
	renameFrom := "street"
	desired := newCompositeTypeMap(&model.CompositeType{
		Schema: "public",
		Name:   "address",
		Attributes: []*model.CompositeAttribute{
			{Name: "road", TypeName: "text", RenameFrom: &renameFrom},
		},
	})
	_, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffCompositeTypes_RenameTypeSourceNotFound(t *testing.T) {
	current := newCompositeTypeMap()
	renameFrom := "public.nonexist"
	desired := newCompositeTypeMap(&model.CompositeType{
		Schema:     "public",
		Name:       "postal_address",
		RenameFrom: &renameFrom,
		Attributes: []*model.CompositeAttribute{attr("street", "text")},
	})
	_, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source public.nonexist not found")
}

func TestDiffCompositeTypes_RenameTypeDestinationExists(t *testing.T) {
	current := newCompositeTypeMap(
		ct("address", attr("street", "text")),
		ct("postal_address", attr("street", "text")),
	)
	renameFrom := "public.address"
	desired := newCompositeTypeMap(&model.CompositeType{
		Schema:     "public",
		Name:       "postal_address",
		RenameFrom: &renameFrom,
		Attributes: []*model.CompositeAttribute{attr("street", "text")},
	})
	_, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffCompositeTypes_RenameTypeCrossSchema(t *testing.T) {
	current := newCompositeTypeMap(&model.CompositeType{
		Schema: "old", Name: "address",
		Attributes: []*model.CompositeAttribute{attr("street", "text")},
	})
	renameFrom := "old.address"
	desired := newCompositeTypeMap(&model.CompositeType{
		Schema:     "new",
		Name:       "address",
		RenameFrom: &renameFrom,
		Attributes: []*model.CompositeAttribute{attr("street", "text")},
	})
	_, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cross-schema rename is not supported")
}

func TestDiffCompositeTypes_RenameTypeAlreadyApplied(t *testing.T) {
	// RenameFrom points at the current key itself: nothing to rename.
	current := newCompositeTypeMap(ct("address", attr("street", "text")))
	renameFrom := "public.address"
	desired := newCompositeTypeMap(&model.CompositeType{
		Schema:     "public",
		Name:       "address",
		RenameFrom: &renameFrom,
		Attributes: []*model.CompositeAttribute{attr("street", "text")},
	})
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
}

func TestDiffCompositeTypes_RenameAttributeAlreadyApplied(t *testing.T) {
	// The new attribute name is already present; the old source is gone. The
	// rename was applied on a previous run, so it is silently skipped.
	current := newCompositeTypeMap(ct("address", attr("road", "text"), attr("city", "text")))
	renameFrom := "street"
	desired := newCompositeTypeMap(&model.CompositeType{
		Schema: "public",
		Name:   "address",
		Attributes: []*model.CompositeAttribute{
			{Name: "road", TypeName: "text", RenameFrom: &renameFrom},
			attr("city", "text"),
		},
	})
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
}

func TestDiffCompositeTypes_TypeCommentRemoved(t *testing.T) {
	comment := "an address"
	current := newCompositeTypeMap(&model.CompositeType{
		Schema: "public", Name: "address", Comment: &comment,
		Attributes: []*model.CompositeAttribute{attr("street", "text")},
	})
	desired := newCompositeTypeMap(ct("address", attr("street", "text")))
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"COMMENT ON TYPE public.address IS NULL;"}, result.Stmts)
}

func TestDiffCompositeTypes_AttributeCommentRemoved(t *testing.T) {
	c := "the street"
	current := newCompositeTypeMap(&model.CompositeType{
		Schema: "public", Name: "address",
		Attributes: []*model.CompositeAttribute{{Name: "street", TypeName: "text", Comment: &c}},
	})
	desired := newCompositeTypeMap(ct("address", attr("street", "text")))
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"COMMENT ON COLUMN public.address.street IS NULL;"}, result.Stmts)
}

func TestDiffCompositeTypes_CommentChange(t *testing.T) {
	comment := "an address"
	current := newCompositeTypeMap(ct("address", attr("street", "text")))
	desired := newCompositeTypeMap(&model.CompositeType{
		Schema:     "public",
		Name:       "address",
		Comment:    &comment,
		Attributes: []*model.CompositeAttribute{attr("street", "text")},
	})
	result, err := diff.DiffCompositeTypes(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"COMMENT ON TYPE public.address IS 'an address';"}, result.Stmts)
}
