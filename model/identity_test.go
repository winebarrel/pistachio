package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio/model"
)

func TestDefaultIdentitySequence(t *testing.T) {
	tests := []struct {
		name      string
		typeName  string
		increment int64
		expected  model.IdentitySequence
	}{
		{
			name: "smallint", typeName: "smallint", increment: 1,
			expected: model.IdentitySequence{Start: 1, Min: 1, Max: 32767, Increment: 1, Cache: 1},
		},
		{
			name: "integer", typeName: "integer", increment: 1,
			expected: model.IdentitySequence{Start: 1, Min: 1, Max: 2147483647, Increment: 1, Cache: 1},
		},
		{
			name: "bigint", typeName: "bigint", increment: 1,
			expected: model.IdentitySequence{Start: 1, Min: 1, Max: 9223372036854775807, Increment: 1, Cache: 1},
		},
		{
			name: "an unknown type falls back to bigint", typeName: "numeric", increment: 1,
			expected: model.IdentitySequence{Start: 1, Min: 1, Max: 9223372036854775807, Increment: 1, Cache: 1},
		},
		{
			name: "descending", typeName: "smallint", increment: -1,
			expected: model.IdentitySequence{Start: -1, Min: -32768, Max: -1, Increment: -1, Cache: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, model.DefaultIdentitySequence(tt.typeName, tt.increment))
		})
	}
}

func TestIdentitySequenceRetypedTo(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var seq *model.IdentitySequence
		assert.Nil(t, seq.RetypedTo("integer", "bigint"))
	})

	t.Run("same type", func(t *testing.T) {
		seq := new(model.DefaultIdentitySequence("integer", 1))
		assert.Same(t, seq, seq.RetypedTo("integer", "integer"))
	})

	t.Run("a bound at the old default follows the type", func(t *testing.T) {
		seq := new(model.DefaultIdentitySequence("integer", 1))
		assert.Equal(t, new(model.DefaultIdentitySequence("bigint", 1)), seq.RetypedTo("integer", "bigint"))
	})

	t.Run("descending too", func(t *testing.T) {
		seq := new(model.DefaultIdentitySequence("bigint", -1))
		assert.Equal(t, new(model.DefaultIdentitySequence("integer", -1)), seq.RetypedTo("bigint", "integer"))
	})

	t.Run("an explicit bound stays", func(t *testing.T) {
		seq := &model.IdentitySequence{Start: 1, Min: 1, Max: 1000, Increment: 1, Cache: 1}
		assert.Equal(t, seq, seq.RetypedTo("integer", "bigint"))
	})
}

func TestIdentitySequenceOptionsSQL(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		seq      *model.IdentitySequence
		expected string
	}{
		{
			name:     "nil",
			typeName: "integer",
			seq:      nil,
			expected: "",
		},
		{
			name:     "all at the defaults",
			typeName: "integer",
			seq:      new(model.DefaultIdentitySequence("integer", 1)),
			expected: "",
		},
		{
			name:     "every option",
			typeName: "integer",
			seq: &model.IdentitySequence{
				Start: 100, Min: 10, Max: 1000, Increment: 5, Cache: 20, Cycle: true,
			},
			expected: " (START WITH 100 INCREMENT BY 5 MINVALUE 10 MAXVALUE 1000 CACHE 20 CYCLE)",
		},
		{
			name:     "descending leaves the bounds the direction implies",
			typeName: "bigint",
			seq:      new(model.DefaultIdentitySequence("bigint", -1)),
			expected: " (INCREMENT BY -1)",
		},
		{
			name:     "bounds of another type are not the defaults",
			typeName: "smallint",
			seq: &model.IdentitySequence{
				Start: 1, Min: 1, Max: 2147483647, Increment: 1, Cache: 1,
			},
			expected: " (MAXVALUE 2147483647)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.seq.OptionsSQL(tt.typeName))
		})
	}
}
