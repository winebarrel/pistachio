package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap"
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
	owner := "public.users"
	seq.OwnerTable = &owner
	assert.True(t, seq.Owned())
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
