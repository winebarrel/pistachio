package diff_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/model"
)

func newEnumMap(enums ...*model.Enum) *orderedmap.Map[string, *model.Enum] {
	m := orderedmap.New[string, *model.Enum]()
	for _, e := range enums {
		m.Set(e.FQEN(), e)
	}
	return m
}

func TestDiffEnums_CreateNew(t *testing.T) {
	current := newEnumMap()
	desired := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE TYPE public.status AS ENUM")
	assert.Empty(t, result.DropStmts)
}

func TestDiffEnums_DropExisting(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap()
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Equal(t, []string{"DROP TYPE public.status;"}, result.DropStmts)
}

func TestDiffEnums_DropExisting_Denied(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap()
	result, err := diff.DiffEnums(current, desired, diff.DenyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
	assert.Equal(t, []string{"-- skipped: DROP TYPE public.status;"}, result.DisallowedDropStmts)
}

func TestDiffEnums_AddValue(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive", "pending"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.status ADD VALUE 'pending' AFTER 'inactive';"}, result.Stmts)
}

func TestDiffEnums_AddValueMiddle(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "closed"},
	})
	desired := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "pending", "closed"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.status ADD VALUE 'pending' AFTER 'active';"}, result.Stmts)
}

func TestDiffEnums_AddValueBeginning(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"new", "active", "inactive"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.status ADD VALUE 'new' BEFORE 'active';"}, result.Stmts)
}

func TestDiffEnums_AddMultipleValues(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"a", "b"},
	})
	desired := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"a", "b", "c", "d"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TYPE public.status ADD VALUE 'c' AFTER 'b';",
		"ALTER TYPE public.status ADD VALUE 'd' AFTER 'c';",
	}, result.Stmts)
}

func TestDiffEnums_AddMultipleValuesMiddle(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"a", "d"},
	})
	desired := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"a", "b", "c", "d"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TYPE public.status ADD VALUE 'b' AFTER 'a';",
		"ALTER TYPE public.status ADD VALUE 'c' AFTER 'b';",
	}, result.Stmts)
}

func TestDiffEnums_NoDiff(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffEnums_AddComment(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active"},
	})
	comment := "User status"
	desired := newEnumMap(&model.Enum{
		Schema:  "public",
		Name:    "status",
		Values:  []string{"active"},
		Comment: &comment,
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"COMMENT ON TYPE public.status IS 'User status';"}, result.Stmts)
}

func TestDiffEnums_DropComment(t *testing.T) {
	comment := "User status"
	current := newEnumMap(&model.Enum{
		Schema:  "public",
		Name:    "status",
		Values:  []string{"active"},
		Comment: &comment,
	})
	desired := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"COMMENT ON TYPE public.status IS NULL;"}, result.Stmts)
}

func TestDiffEnums_CreateWithComment(t *testing.T) {
	current := newEnumMap()
	comment := "User status"
	desired := newEnumMap(&model.Enum{
		Schema:  "public",
		Name:    "status",
		Values:  []string{"active"},
		Comment: &comment,
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 2)
	assert.Contains(t, result.Stmts[0], "CREATE TYPE public.status AS ENUM")
	assert.Equal(t, "COMMENT ON TYPE public.status IS 'User status';", result.Stmts[1])
}

func TestDiffEnums_RemoveValue_Error(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive", "pending"},
	})
	desired := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	_, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot remove enum value")
	assert.Contains(t, err.Error(), "public.status")
}

func TestDiffEnums_Reorder_Error(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive", "pending"},
	})
	desired := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"inactive", "active", "pending"},
	})
	_, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot reorder enum values")
	assert.Contains(t, err.Error(), "public.status")
}

func TestDiffEnums_Rename(t *testing.T) {
	oldName := "public.status"
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:     "public",
		Name:       "user_status",
		RenameFrom: &oldName,
		Values:     []string{"active", "inactive"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.status RENAME TO user_status;"}, result.Stmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffEnums_RenameAndAddValue(t *testing.T) {
	oldName := "public.status"
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:     "public",
		Name:       "user_status",
		RenameFrom: &oldName,
		Values:     []string{"active", "inactive", "pending"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 2)
	assert.Equal(t, "ALTER TYPE public.status RENAME TO user_status;", result.Stmts[0])
	assert.Contains(t, result.Stmts[1], "ADD VALUE 'pending'")
}

func TestDiffEnums_RenameSelfRename_Skipped(t *testing.T) {
	oldName := "public.status"
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:     "public",
		Name:       "status",
		RenameFrom: &oldName,
		Values:     []string{"active"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
}

func TestDiffEnums_RenameAlreadyApplied(t *testing.T) {
	oldName := "public.old_status"
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "user_status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:     "public",
		Name:       "user_status",
		RenameFrom: &oldName,
		Values:     []string{"active", "inactive"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffEnums_RenameCrossSchema_Error(t *testing.T) {
	oldName := "other.status"
	current := newEnumMap(&model.Enum{
		Schema: "other",
		Name:   "status",
		Values: []string{"active"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:     "public",
		Name:       "user_status",
		RenameFrom: &oldName,
		Values:     []string{"active"},
	})
	_, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cross-schema rename")
}

func TestDiffEnums_RenameDestinationExists_Error(t *testing.T) {
	oldName := "public.status"
	current := newEnumMap(
		&model.Enum{Schema: "public", Name: "status", Values: []string{"active"}},
		&model.Enum{Schema: "public", Name: "user_status", Values: []string{"a"}},
	)
	desired := newEnumMap(&model.Enum{
		Schema:     "public",
		Name:       "user_status",
		RenameFrom: &oldName,
		Values:     []string{"active"},
	})
	_, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffEnums_RenameValue(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"active", "disabled"},
		ValueRenameFrom: map[string]string{"disabled": "inactive"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.status RENAME VALUE 'inactive' TO 'disabled';"}, result.Stmts)
}

func TestDiffEnums_RenameValueAlreadyApplied(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "disabled"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"active", "disabled"},
		ValueRenameFrom: map[string]string{"disabled": "inactive"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
}

func TestDiffEnums_RenameValueSelf_Skipped(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"active"},
		ValueRenameFrom: map[string]string{"active": "active"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
}

func TestDiffEnums_RenameValueAndAdd(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"active", "disabled", "pending"},
		ValueRenameFrom: map[string]string{"disabled": "inactive"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TYPE public.status RENAME VALUE 'inactive' TO 'disabled';",
		"ALTER TYPE public.status ADD VALUE 'pending' AFTER 'disabled';",
	}, result.Stmts)
}

func TestDiffEnums_RenameValueKeepsPosition_ReorderError(t *testing.T) {
	// RENAME VALUE keeps the value's position, so a rename combined with a
	// position change is still a reorder error.
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"a", "b", "c"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"b2", "a", "c"},
		ValueRenameFrom: map[string]string{"b2": "b"},
	})
	_, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot reorder enum values")
}

func TestDiffEnums_RenameValueSourceNotFound_Error(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"active", "disabled"},
		ValueRenameFrom: map[string]string{"disabled": "nonexistent"},
	})
	_, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source 'nonexistent' not found")
	assert.Contains(t, err.Error(), "public.status")
}

func TestDiffEnums_RenameValueDestinationExists_Error(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive", "disabled"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"active", "disabled"},
		ValueRenameFrom: map[string]string{"disabled": "inactive"},
	})
	_, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffEnums_RenameValueDuplicateSource_Error(t *testing.T) {
	// Two desired values renaming from the same source: the first rename
	// consumes the source, the second fails.
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"a"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"b", "c"},
		ValueRenameFrom: map[string]string{"b": "a", "c": "a"},
	})
	_, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source")
}

func TestDiffEnums_RenameValueQuoted(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"don't"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"won't"},
		ValueRenameFrom: map[string]string{"won't": "don't"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TYPE public.status RENAME VALUE 'don''t' TO 'won''t';"}, result.Stmts)
}

func TestDiffEnums_NewEnumIgnoresValueRenameDirective(t *testing.T) {
	// A value rename directive on an enum that does not exist yet is
	// meaningless: the enum is created with the desired values as-is.
	current := newEnumMap()
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"active", "disabled"},
		ValueRenameFrom: map[string]string{"disabled": "inactive"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE TYPE public.status AS ENUM")
}

func TestDiffEnums_RenameValueAndReAddOldName(t *testing.T) {
	// The old name may be reintroduced as a new value in the same plan:
	// the rename runs first, then the old name is added as a fresh value.
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"old", "keep"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "status",
		Values:          []string{"new", "old", "keep"},
		ValueRenameFrom: map[string]string{"new": "old"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TYPE public.status RENAME VALUE 'old' TO 'new';",
		"ALTER TYPE public.status ADD VALUE 'old' AFTER 'new';",
	}, result.Stmts)
}

func TestDiffEnums_RenameTypeAndValue(t *testing.T) {
	oldName := "public.status"
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap(&model.Enum{
		Schema:          "public",
		Name:            "user_status",
		RenameFrom:      &oldName,
		Values:          []string{"active", "disabled"},
		ValueRenameFrom: map[string]string{"disabled": "inactive"},
	})
	result, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TYPE public.status RENAME TO user_status;",
		"ALTER TYPE public.user_status RENAME VALUE 'inactive' TO 'disabled';",
	}, result.Stmts)
}

func TestDiffEnums_RenameSourceNotFound(t *testing.T) {
	oldName := "public.nonexistent"
	current := newEnumMap()
	desired := newEnumMap(&model.Enum{
		Schema:     "public",
		Name:       "user_status",
		RenameFrom: &oldName,
		Values:     []string{"active"},
	})
	_, err := diff.DiffEnums(current, desired, diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source")
}
