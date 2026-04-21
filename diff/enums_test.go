package diff_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	result := diff.DiffEnums(current, desired)
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
	result := diff.DiffEnums(current, desired)
	assert.Empty(t, result.Stmts)
	assert.Equal(t, []string{"DROP TYPE public.status;"}, result.DropStmts)
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
	result := diff.DiffEnums(current, desired)
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
	result := diff.DiffEnums(current, desired)
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
	result := diff.DiffEnums(current, desired)
	assert.Equal(t, []string{"ALTER TYPE public.status ADD VALUE 'new' BEFORE 'active';"}, result.Stmts)
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
	result := diff.DiffEnums(current, desired)
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
	result := diff.DiffEnums(current, desired)
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
	result := diff.DiffEnums(current, desired)
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
	result := diff.DiffEnums(current, desired)
	assert.Len(t, result.Stmts, 2)
	assert.Contains(t, result.Stmts[0], "CREATE TYPE public.status AS ENUM")
	assert.Equal(t, "COMMENT ON TYPE public.status IS 'User status';", result.Stmts[1])
}
