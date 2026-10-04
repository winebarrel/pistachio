package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func sampleSequence() *model.Sequence {
	return &model.Sequence{
		Schema:    "public",
		Name:      "order_seq",
		DataType:  "bigint",
		Start:     1,
		Min:       1,
		Max:       9223372036854775807,
		Increment: 1,
		Cache:     1,
	}
}

func TestSequence_FQN(t *testing.T) {
	assert.Equal(t, "public.order_seq", sampleSequence().FQN())
}

func TestSequence_Owned(t *testing.T) {
	seq := sampleSequence()
	assert.False(t, seq.Owned())
	assert.Empty(t, seq.OwnerFQTN())
	assert.Empty(t, seq.OwnedBySQL())
	table, column := "users", "id"
	seq.OwnerTable, seq.OwnerColumn = &table, &column
	assert.True(t, seq.Owned())
	assert.Equal(t, "public.users", seq.OwnerFQTN())
	assert.Equal(t, "ALTER SEQUENCE public.order_seq OWNED BY public.users.id;", seq.OwnedBySQL())
}

func TestSequencesOwnedBySQL(t *testing.T) {
	owned := sampleSequence()
	table, column := "users", "id"
	owned.OwnerTable, owned.OwnerColumn = &table, &column
	standalone := sampleSequence()
	standalone.Name = "other_seq"

	seqs := orderedmap.New[string, *model.Sequence]()
	seqs.Set(standalone.FQN(), standalone)
	assert.Empty(t, model.SequencesOwnedBySQL(seqs))

	seqs.Set(owned.FQN(), owned)
	assert.Equal(t, "ALTER SEQUENCE public.order_seq OWNED BY public.users.id;", model.SequencesOwnedBySQL(seqs))
}

func TestSequence_SQL(t *testing.T) {
	assert.Equal(t, `CREATE SEQUENCE public.order_seq
    AS bigint
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;`, sampleSequence().SQL())
}

func TestSequence_SQL_Cycle(t *testing.T) {
	seq := sampleSequence()
	seq.Cycle = true
	assert.Contains(t, seq.SQL(), "\n    CYCLE;")
}

func TestSequence_CommentSQL(t *testing.T) {
	seq := sampleSequence()
	assert.Empty(t, seq.CommentSQL())
	comment := "id generator"
	seq.Comment = &comment
	assert.Equal(t, "COMMENT ON SEQUENCE public.order_seq IS 'id generator';", seq.CommentSQL())
}

func TestSequenceToSQL_WithComment(t *testing.T) {
	seq := sampleSequence()
	comment := "id generator"
	seq.Comment = &comment
	sql := model.SequenceToSQL(seq)
	assert.Contains(t, sql, "-- public.order_seq")
	assert.Contains(t, sql, "CREATE SEQUENCE public.order_seq")
	assert.Contains(t, sql, "COMMENT ON SEQUENCE public.order_seq IS 'id generator';")
}

func TestSequencesToSQL(t *testing.T) {
	m := orderedmap.New[string, *model.Sequence]()
	m.Set("public.a_seq", &model.Sequence{Schema: "public", Name: "a_seq", DataType: "bigint", Max: 1, Cache: 1, Increment: 1})
	m.Set("public.b_seq", &model.Sequence{Schema: "public", Name: "b_seq", DataType: "bigint", Max: 1, Cache: 1, Increment: 1})
	sql := model.SequencesToSQL(m)
	assert.Contains(t, sql, "public.a_seq")
	assert.Contains(t, sql, "public.b_seq")
}

func TestSequence_String(t *testing.T) {
	seq := &model.Sequence{Schema: "public", Name: "users_id_seq", DataType: "bigint"}
	assert.Contains(t, seq.String(), "users_id_seq")
}

func TestSequence_SQL_Unlogged(t *testing.T) {
	seq := sampleSequence()
	seq.Unlogged = true
	assert.Contains(t, seq.SQL(), "CREATE UNLOGGED SEQUENCE public.order_seq\n")
}
