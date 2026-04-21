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
	stmts := diff.DiffEnums(current, desired)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "CREATE TYPE public.status AS ENUM")
}

func TestDiffEnums_DropExisting(t *testing.T) {
	current := newEnumMap(&model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	desired := newEnumMap()
	stmts := diff.DiffEnums(current, desired)
	assert.Equal(t, []string{"DROP TYPE public.status;"}, stmts)
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
	stmts := diff.DiffEnums(current, desired)
	assert.Equal(t, []string{"ALTER TYPE public.status ADD VALUE 'pending';"}, stmts)
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
	stmts := diff.DiffEnums(current, desired)
	assert.Empty(t, stmts)
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
	stmts := diff.DiffEnums(current, desired)
	assert.Equal(t, []string{"COMMENT ON TYPE public.status IS 'User status';"}, stmts)
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
	stmts := diff.DiffEnums(current, desired)
	assert.Equal(t, []string{"COMMENT ON TYPE public.status IS NULL;"}, stmts)
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
	stmts := diff.DiffEnums(current, desired)
	assert.Len(t, stmts, 2)
	assert.Contains(t, stmts[0], "CREATE TYPE public.status AS ENUM")
	assert.Equal(t, "COMMENT ON TYPE public.status IS 'User status';", stmts[1])
}
