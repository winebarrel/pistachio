package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSequence_String(t *testing.T) {
	seq := &Sequence{Schema: "public", Name: "users_id_seq", DataType: "bigint"}
	s := seq.String()
	assert.Contains(t, s, "users_id_seq")
}
