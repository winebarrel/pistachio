package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio/model"
)

func TestIndex_FQTN(t *testing.T) {
	idx := model.Index{Schema: "public", Name: "users_pkey", Table: "users"}
	assert.Equal(t, "public.users", idx.FQTN())
}

func TestIndex_SQL(t *testing.T) {
	idx := model.Index{Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"}
	assert.Equal(t, "CREATE INDEX idx_name ON public.users USING btree (name);", idx.SQL())
}

func TestIndex_CommentSQL(t *testing.T) {
	comment := "Lookup by name"
	idx := model.Index{Schema: "public", Name: "idx_users_name", Comment: &comment}
	assert.Equal(t, "COMMENT ON INDEX public.idx_users_name IS 'Lookup by name';", idx.CommentSQL())

	idx.Comment = nil
	assert.Empty(t, idx.CommentSQL())
}
