package diff_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/diff"
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
	result, err := diff.DiffSequences(newSeqMap(), newSeqMap(baseSeq()), diff.AllowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE SEQUENCE public.s")
	assert.Empty(t, result.DropStmts)
}

func TestDiffSequences_DropExisting(t *testing.T) {
	result, err := diff.DiffSequences(newSeqMap(baseSeq()), newSeqMap(), diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	require.Len(t, result.DropStmts, 1)
	assert.Equal(t, "DROP SEQUENCE public.s;", result.DropStmts[0])
}

func TestDiffSequences_DropDenied(t *testing.T) {
	result, err := diff.DiffSequences(newSeqMap(baseSeq()), newSeqMap(), diff.DenyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	require.Len(t, result.DisallowedDropStmts, 1)
	assert.Contains(t, result.DisallowedDropStmts[0], "-- skipped: DROP SEQUENCE public.s;")
}

func TestDiffSequences_NoDiff(t *testing.T) {
	result, err := diff.DiffSequences(newSeqMap(baseSeq()), newSeqMap(baseSeq()), diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
}

func TestDiffSequences_AlterOptions(t *testing.T) {
	desired := baseSeq()
	desired.Increment = 2
	desired.Max = 5000
	desired.Cache = 10
	desired.Cycle = true
	result, err := diff.DiffSequences(newSeqMap(baseSeq()), newSeqMap(desired), diff.AllowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Equal(t, "ALTER SEQUENCE public.s INCREMENT BY 2 MAXVALUE 5000 CACHE 10 CYCLE;", result.Stmts[0])
}

func TestDiffSequences_NoCycle(t *testing.T) {
	current := baseSeq()
	current.Cycle = true
	result, err := diff.DiffSequences(newSeqMap(current), newSeqMap(baseSeq()), diff.AllowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Equal(t, "ALTER SEQUENCE public.s NO CYCLE;", result.Stmts[0])
}

func TestDiffSequences_AddComment(t *testing.T) {
	desired := baseSeq()
	comment := "id generator"
	desired.Comment = &comment
	result, err := diff.DiffSequences(newSeqMap(baseSeq()), newSeqMap(desired), diff.AllowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Equal(t, "COMMENT ON SEQUENCE public.s IS 'id generator';", result.Stmts[0])
}

func TestDiffSequences_RemoveComment(t *testing.T) {
	current := baseSeq()
	comment := "id generator"
	current.Comment = &comment
	result, err := diff.DiffSequences(newSeqMap(current), newSeqMap(baseSeq()), diff.AllowAllDrops{})
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
	result, err := diff.DiffSequences(newSeqMap(current), newSeqMap(desired), diff.AllowAllDrops{})
	require.NoError(t, err)
	require.NotEmpty(t, result.Stmts)
	assert.Equal(t, "ALTER SEQUENCE public.old RENAME TO new;", result.Stmts[0])
	assert.Empty(t, result.DropStmts)
}

func TestDiffSequences_RenameCrossSchemaError(t *testing.T) {
	current := &model.Sequence{Schema: "other", Name: "old", DataType: "bigint", Max: 1, Cache: 1, Increment: 1}
	desired := &model.Sequence{Schema: "public", Name: "new", DataType: "bigint", Max: 1, Cache: 1, Increment: 1}
	renameFrom := "other.old"
	desired.RenameFrom = &renameFrom
	_, err := diff.DiffSequences(newSeqMap(current), newSeqMap(desired), diff.AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cross-schema rename")
}
