package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func persistenceTable(schema, name string, unlogged bool, refs ...string) *model.Table {
	t := &model.Table{
		Schema:      schema,
		Name:        name,
		Unlogged:    unlogged,
		ForeignKeys: orderedmap.New[string, *model.ForeignKey](),
	}
	for i, ref := range refs {
		refSchema, refTable := schema, ref
		fk := &model.ForeignKey{Schema: schema, Table: name, RefSchema: &refSchema, RefTable: &refTable}
		fk.Name = name + "_fk" + string(rune('0'+i))
		t.ForeignKeys.Set(fk.Name, fk)
	}
	return t
}

func changeFor(t *model.Table) *persistenceChange {
	c := diffPersistence(t.FQTN(), &model.Table{Unlogged: !t.Unlogged}, t)
	return c
}

// Mutual foreign keys have no order PostgreSQL accepts, so the only thing to
// hold is that every table still gets its statement exactly once.
func TestOrderPersistenceChanges_MutualForeignKeys(t *testing.T) {
	a := persistenceTable("public", "a", true, "b")
	b := persistenceTable("public", "b", true, "a")

	stmts := orderPersistenceChanges([]*persistenceChange{changeFor(a), changeFor(b)})

	assert.ElementsMatch(t, []string{
		"ALTER TABLE public.a SET UNLOGGED;",
		"ALTER TABLE public.b SET UNLOGGED;",
	}, stmts)
}

// A reference written without a schema is read as the owning table's, the way
// normalizeFKSchema reads it, so the edge is still found.
func TestOrderPersistenceChanges_UnqualifiedReference(t *testing.T) {
	parent := persistenceTable("public", "parent", true)
	child := persistenceTable("public", "child", true)
	refTable := "parent"
	fk := &model.ForeignKey{Schema: "public", Table: "child", RefTable: &refTable}
	fk.Name = "child_p_fkey"
	child.ForeignKeys.Set(fk.Name, fk)

	stmts := orderPersistenceChanges([]*persistenceChange{changeFor(parent), changeFor(child)})

	assert.Equal(t, []string{
		"ALTER TABLE public.child SET UNLOGGED;",
		"ALTER TABLE public.parent SET UNLOGGED;",
	}, stmts)
}

// Both directions in one plan: the tables turning logged run first, each group
// in its own order.
func TestOrderPersistenceChanges_BothDirections(t *testing.T) {
	toLogged := persistenceTable("public", "kept", false)
	unloggedParent := persistenceTable("public", "parent", true)
	unloggedChild := persistenceTable("public", "child", true, "parent")

	stmts := orderPersistenceChanges([]*persistenceChange{
		changeFor(unloggedParent), changeFor(toLogged), changeFor(unloggedChild),
	})

	assert.Equal(t, []string{
		"ALTER TABLE public.kept SET LOGGED;",
		"ALTER TABLE public.child SET UNLOGGED;",
		"ALTER TABLE public.parent SET UNLOGGED;",
	}, stmts)
}

// A partitioned table is left alone: the statement would report success and
// change nothing, so the diff would repeat it on every run.
func TestDiffPersistence_PartitionedSkipped(t *testing.T) {
	current := &model.Table{Schema: "public", Name: "events", Unlogged: true, Partitioned: true}
	desired := &model.Table{Schema: "public", Name: "events", Partitioned: true}

	assert.Nil(t, diffPersistence(desired.FQTN(), current, desired))
}
