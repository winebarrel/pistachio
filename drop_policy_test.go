package pistachio_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio"
)

func TestDropPolicy_IsDropAllowed(t *testing.T) {
	t.Run("empty (no drops)", func(t *testing.T) {
		p := &pistachio.DropPolicy{}
		assert.False(t, p.IsDropAllowed("table"))
		assert.False(t, p.IsDropAllowed("view"))
		assert.False(t, p.IsDropAllowed("enum"))
		assert.False(t, p.IsDropAllowed("domain"))
		assert.False(t, p.IsDropAllowed("column"))
		assert.False(t, p.IsDropAllowed("constraint"))
		assert.False(t, p.IsDropAllowed("foreign_key"))
		assert.False(t, p.IsDropAllowed("index"))
	})

	t.Run("all", func(t *testing.T) {
		p := &pistachio.DropPolicy{AllowDrop: []string{"all"}}
		assert.True(t, p.IsDropAllowed("table"))
		assert.True(t, p.IsDropAllowed("view"))
		assert.True(t, p.IsDropAllowed("enum"))
		assert.True(t, p.IsDropAllowed("domain"))
		assert.True(t, p.IsDropAllowed("column"))
		assert.True(t, p.IsDropAllowed("constraint"))
		assert.True(t, p.IsDropAllowed("foreign_key"))
		assert.True(t, p.IsDropAllowed("index"))
	})

	t.Run("specific types", func(t *testing.T) {
		p := &pistachio.DropPolicy{AllowDrop: []string{"table", "column"}}
		assert.True(t, p.IsDropAllowed("table"))
		assert.True(t, p.IsDropAllowed("column"))
		assert.False(t, p.IsDropAllowed("view"))
		assert.False(t, p.IsDropAllowed("enum"))
	})
}
