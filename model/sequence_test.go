package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio/model"
)

func TestSequence_String(t *testing.T) {
	seq := &model.Sequence{Schema: "public", Name: "users_id_seq", DataType: "bigint"}
	s := seq.String()
	assert.Contains(t, s, "users_id_seq")
}
