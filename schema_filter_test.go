package pistachio_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio"
)

func TestObjectCount_SchemaLabel(t *testing.T) {
	c := pistachio.ObjectCount{Schemas: []string{"public"}}
	assert.Equal(t, "schema public", c.SchemaLabel())

	c2 := pistachio.ObjectCount{Schemas: []string{"public", "myschema"}}
	assert.Equal(t, "schemas public, myschema", c2.SchemaLabel())
}

func TestObjectCount_Summary(t *testing.T) {
	c := pistachio.ObjectCount{Tables: 3, Views: 1, Enums: 2, Domains: 0, CompositeTypes: 0, Sequences: 4}
	assert.Equal(t, "3 tables, 1 view, 2 enums, 0 domains, 0 composite types, 4 sequences", c.Summary())
}

func TestObjectCount_Summary_Singular(t *testing.T) {
	c := pistachio.ObjectCount{Tables: 1, Views: 1, Enums: 1, Domains: 1, CompositeTypes: 1, Sequences: 1}
	assert.Equal(t, "1 table, 1 view, 1 enum, 1 domain, 1 composite type, 1 sequence", c.Summary())
}

// Routines are opt-in, so the slot appears only when they are managed. A nil
// leaves the line reading exactly as it did before routines existed.
func TestObjectCount_Summary_Routines(t *testing.T) {
	c := pistachio.ObjectCount{Tables: 1}
	assert.Equal(t, "1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences", c.Summary())

	n := 2
	c.Routines = &n
	assert.Equal(t, "1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences, 2 routines", c.Summary())

	one := 1
	c.Routines = &one
	assert.Contains(t, c.Summary(), "1 routine")
}
