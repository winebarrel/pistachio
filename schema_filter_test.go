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
	c := pistachio.ObjectCount{Tables: 3, Views: 1, Enums: 2, Domains: 0}
	assert.Equal(t, "3 tables, 1 view, 2 enums, 0 domains", c.Summary())
}

func TestObjectCount_Summary_Singular(t *testing.T) {
	c := pistachio.ObjectCount{Tables: 1, Views: 1, Enums: 1, Domains: 1}
	assert.Equal(t, "1 table, 1 view, 1 enum, 1 domain", c.Summary())
}
