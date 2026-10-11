package pistachio

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDropPolicy_IsDropAllowed(t *testing.T) {
	t.Run("nil (no drops)", func(t *testing.T) {
		var p *DropPolicy
		assert.False(t, p.IsDropAllowed("table"))
		assert.False(t, p.IsDropAllowed("all"))
	})

	t.Run("empty (no drops)", func(t *testing.T) {
		p := &DropPolicy{}
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
		p := &DropPolicy{AllowDrop: []string{"all"}}
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
		p := &DropPolicy{AllowDrop: []string{"table", "column"}}
		assert.True(t, p.IsDropAllowed("table"))
		assert.True(t, p.IsDropAllowed("column"))
		assert.False(t, p.IsDropAllowed("view"))
		assert.False(t, p.IsDropAllowed("enum"))
	})

	t.Run("newer object types", func(t *testing.T) {
		p := &DropPolicy{AllowDrop: []string{"composite_type", "sequence", "routine", "policy", "trigger"}}
		assert.True(t, p.IsDropAllowed("composite_type"))
		assert.True(t, p.IsDropAllowed("sequence"))
		assert.True(t, p.IsDropAllowed("routine"))
		assert.True(t, p.IsDropAllowed("policy"))
		assert.True(t, p.IsDropAllowed("trigger"))
		assert.False(t, p.IsDropAllowed("table"))
		assert.False(t, p.IsDropAllowed("index"))
	})
}
