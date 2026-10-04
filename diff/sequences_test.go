package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func newSeqMap(seqs ...*model.Sequence) *orderedmap.Map[string, *model.Sequence] {
	m := orderedmap.New[string, *model.Sequence]()
	for _, s := range seqs {
		m.Set(s.FQN(), s)
	}
	return m
}

func baseSeq() *model.Sequence {
	return &model.Sequence{
		Schema:    "public",
		Name:      "s",
		DataType:  "bigint",
		Start:     1,
		Min:       1,
		Max:       9223372036854775807,
		Increment: 1,
		Cache:     1,
	}
}

func TestDiffSequences_CreateNew(t *testing.T) {
	result, err := DiffSequences(newSeqMap(), newSeqMap(baseSeq()), allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE SEQUENCE public.s")
	assert.Empty(t, result.DropStmts)
}

func TestDiffSequences_DropExisting(t *testing.T) {
	result, err := DiffSequences(newSeqMap(baseSeq()), newSeqMap(), allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	require.Len(t, result.DropStmts, 1)
	assert.Equal(t, "DROP SEQUENCE public.s;", result.DropStmts[0])
}

func TestDiffSequences_DropDenied(t *testing.T) {
	result, err := DiffSequences(newSeqMap(baseSeq()), newSeqMap(), denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	require.Len(t, result.DisallowedDropStmts, 1)
	assert.Contains(t, result.DisallowedDropStmts[0], "-- skipped: DROP SEQUENCE public.s;")
}

func TestDiffSequences_NoDiff(t *testing.T) {
	result, err := DiffSequences(newSeqMap(baseSeq()), newSeqMap(baseSeq()), allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffSequences_AlterOptions(t *testing.T) {
	desired := baseSeq()
	desired.Increment = 2
	desired.Min = 5
	desired.Max = 5000
	desired.Start = 100
	desired.Cache = 10
	desired.Cycle = true
	result, err := DiffSequences(newSeqMap(baseSeq()), newSeqMap(desired), allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Equal(t, "ALTER SEQUENCE public.s INCREMENT BY 2 MINVALUE 5 MAXVALUE 5000 START WITH 100 CACHE 10 CYCLE;", result.Stmts[0])
}

func TestDiffSequences_AlterType(t *testing.T) {
	current := baseSeq()
	current.DataType = "smallint"
	current.Max = 100
	desired := baseSeq()
	desired.DataType = "integer"
	desired.Max = 100
	result, err := DiffSequences(newSeqMap(current), newSeqMap(desired), allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Equal(t, "ALTER SEQUENCE public.s AS integer;", result.Stmts[0])
}

func TestDiffSequences_NoCycle(t *testing.T) {
	current := baseSeq()
	current.Cycle = true
	result, err := DiffSequences(newSeqMap(current), newSeqMap(baseSeq()), allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Equal(t, "ALTER SEQUENCE public.s NO CYCLE;", result.Stmts[0])
}

func TestDiffSequences_AddComment(t *testing.T) {
	desired := baseSeq()
	comment := "id generator"
	desired.Comment = &comment
	result, err := DiffSequences(newSeqMap(baseSeq()), newSeqMap(desired), allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Equal(t, "COMMENT ON SEQUENCE public.s IS 'id generator';", result.Stmts[0])
}

func TestDiffSequences_RemoveComment(t *testing.T) {
	current := baseSeq()
	comment := "id generator"
	current.Comment = &comment
	result, err := DiffSequences(newSeqMap(current), newSeqMap(baseSeq()), allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Equal(t, "COMMENT ON SEQUENCE public.s IS NULL;", result.Stmts[0])
}

func TestDiffSequences_Rename(t *testing.T) {
	current := baseSeq()
	current.Name = "old"
	desired := baseSeq()
	desired.Name = "new"
	renameFrom := "public.old"
	desired.RenameFrom = &renameFrom
	result, err := DiffSequences(newSeqMap(current), newSeqMap(desired), allowAllDrops{})
	require.NoError(t, err)
	require.NotEmpty(t, result.Stmts)
	assert.Equal(t, "ALTER SEQUENCE public.old RENAME TO new;", result.Stmts[0])
	assert.Empty(t, result.DropStmts)
}

func TestDiffSequences_RenameAlreadyApplied(t *testing.T) {
	// After a rename is applied, re-planning still carries the renamed-from
	// directive: the source is gone but the destination exists, so the rename
	// is skipped and no diff is produced (idempotent).
	current := baseSeq()
	current.Name = "new"
	desired := baseSeq()
	desired.Name = "new"
	renameFrom := "public.old"
	desired.RenameFrom = &renameFrom
	result, err := DiffSequences(newSeqMap(current), newSeqMap(desired), allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffSequences_RenameSameName(t *testing.T) {
	// A directive naming the sequence itself is not a rename.
	desired := baseSeq()
	renameFrom := "public.s"
	desired.RenameFrom = &renameFrom
	result, err := DiffSequences(newSeqMap(baseSeq()), newSeqMap(desired), allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffSequences_RenameCrossSchemaError(t *testing.T) {
	current := &model.Sequence{Schema: "other", Name: "old", DataType: "bigint", Max: 1, Cache: 1, Increment: 1}
	desired := &model.Sequence{Schema: "public", Name: "new", DataType: "bigint", Max: 1, Cache: 1, Increment: 1}
	renameFrom := "other.old"
	desired.RenameFrom = &renameFrom
	_, err := DiffSequences(newSeqMap(current), newSeqMap(desired), allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cross-schema rename")
}

func TestDiffSequences_RenameSourceNotFound(t *testing.T) {
	desired := baseSeq()
	desired.Name = "new"
	renameFrom := "public.nonexistent"
	desired.RenameFrom = &renameFrom
	_, err := DiffSequences(newSeqMap(), newSeqMap(desired), allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source public.nonexistent not found")
}

func TestDiffSequences_RenameDestinationExists(t *testing.T) {
	old := baseSeq()
	old.Name = "old"
	current := baseSeq()
	current.Name = "new"
	desired := baseSeq()
	desired.Name = "new"
	renameFrom := "public.old"
	desired.RenameFrom = &renameFrom
	_, err := DiffSequences(newSeqMap(old, current), newSeqMap(desired), allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffSequences_SetUnlogged(t *testing.T) {
	desired := baseSeq()
	desired.Unlogged = true
	result, err := DiffSequences(newSeqMap(baseSeq()), newSeqMap(desired), allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER SEQUENCE public.s SET UNLOGGED;"}, result.Stmts)
}

func TestDiffSequences_SetLogged(t *testing.T) {
	current := baseSeq()
	current.Unlogged = true
	result, err := DiffSequences(newSeqMap(current), newSeqMap(baseSeq()), allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER SEQUENCE public.s SET LOGGED;"}, result.Stmts)
}

func ownedSeq(table, column string) *model.Sequence {
	seq := baseSeq()
	seq.OwnerTable, seq.OwnerColumn = &table, &column
	return seq
}

func TestDiffSequences_CreateOwned(t *testing.T) {
	result, err := DiffSequences(newSeqMap(), newSeqMap(ownedSeq("t", "id")), allowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE SEQUENCE public.s")
	assert.Equal(t, []string{"ALTER SEQUENCE public.s OWNED BY public.t.id;"}, result.OwnedByStmts)
	assert.Empty(t, result.DisownStmts)
}

func TestDiffSequences_SetOwner(t *testing.T) {
	result, err := DiffSequences(newSeqMap(baseSeq()), newSeqMap(ownedSeq("t", "id")), allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Equal(t, []string{"ALTER SEQUENCE public.s OWNED BY public.t.id;"}, result.OwnedByStmts)
	assert.Empty(t, result.DisownStmts)
}

func TestDiffSequences_ChangeOwner(t *testing.T) {
	result, err := DiffSequences(newSeqMap(ownedSeq("t", "a")), newSeqMap(ownedSeq("t", "b")), allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER SEQUENCE public.s OWNED BY public.t.b;"}, result.OwnedByStmts)
	assert.Empty(t, result.DisownStmts)
}

func TestDiffSequences_Disown(t *testing.T) {
	result, err := DiffSequences(newSeqMap(ownedSeq("t", "id")), newSeqMap(baseSeq()), allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.OwnedByStmts)
	assert.Equal(t, []string{"ALTER SEQUENCE public.s OWNED BY NONE;"}, result.DisownStmts)
}

func TestDiffSequences_SameOwner(t *testing.T) {
	result, err := DiffSequences(newSeqMap(ownedSeq("t", "id")), newSeqMap(ownedSeq("t", "id")), allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.OwnedByStmts)
	assert.Empty(t, result.DisownStmts)
}

func TestDiffSequences_DisownRenamed(t *testing.T) {
	current := ownedSeq("t", "id")
	current.Name = "old"
	desired := baseSeq()
	desired.Name = "new"
	from := "public.old"
	desired.RenameFrom = &from

	result, err := DiffSequences(newSeqMap(current), newSeqMap(desired), allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER SEQUENCE public.old RENAME TO new;"}, result.Stmts)
	assert.Equal(t, []string{"ALTER SEQUENCE public.old OWNED BY NONE;"}, result.DisownStmts)
}
