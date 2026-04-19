package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex_FQTN(t *testing.T) {
	idx := Index{Schema: "public", Name: "users_pkey", Table: "users"}
	assert.Equal(t, "public.users", idx.FQTN())
}

func TestIndex_SQL(t *testing.T) {
	idx := Index{Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"}
	assert.Equal(t, "CREATE INDEX idx_name ON public.users USING btree (name);", idx.SQL())
}
