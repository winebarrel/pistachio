package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
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

// Both directions in one plan: the tables turning logged run first, each group
// in its own order. The unlogged pair arrives referencing-table-first, which
// the reversal would put the wrong way round on its own, so the group order is
// pinned rather than reached by accident.
func TestOrderPersistenceChanges_BothDirections(t *testing.T) {
	toLogged := persistenceTable("public", "kept", false)
	unloggedParent := persistenceTable("public", "parent", true)
	unloggedChild := persistenceTable("public", "child", true, "parent")

	stmts := orderPersistenceChanges([]*persistenceChange{
		changeFor(unloggedChild), changeFor(toLogged), changeFor(unloggedParent),
	})

	assert.Equal(t, []string{
		"ALTER TABLE public.kept SET LOGGED;",
		"ALTER TABLE public.child SET UNLOGGED;",
		"ALTER TABLE public.parent SET UNLOGGED;",
	}, stmts)
}

// A key pointing at a table that is not changing constrains nothing, so the
// two keep the order they came in, reversed for the unlogged direction.
func TestOrderPersistenceChanges_ReferenceOutsideTheSet(t *testing.T) {
	a := persistenceTable("public", "a", true, "elsewhere")
	b := persistenceTable("public", "b", true)

	stmts := orderPersistenceChanges([]*persistenceChange{changeFor(a), changeFor(b)})

	assert.Equal(t, []string{
		"ALTER TABLE public.b SET UNLOGGED;",
		"ALTER TABLE public.a SET UNLOGGED;",
	}, stmts)
}

// The referenced table living in another schema is the shape the parser
// produces for a bare reference under --schemas public,app, where it resolves
// to the first target schema rather than the owning table's.
//
// The logged direction, with the referencing table first, is what makes this
// pin the lookup: that group is not reversed, so reaching the expected order
// needs the edge. The unlogged direction would hold without one.
func TestOrderPersistenceChanges_CrossSchemaReference(t *testing.T) {
	parent := persistenceTable("public", "parent", false)
	child := persistenceTable("app", "child", false)
	refSchema, refTable := "public", "parent"
	fk := &model.ForeignKey{Schema: "app", Table: "child", RefSchema: &refSchema, RefTable: &refTable}
	fk.Name = "child_p_fkey"
	child.ForeignKeys.Set(fk.Name, fk)

	stmts := orderPersistenceChanges([]*persistenceChange{changeFor(child), changeFor(parent)})

	assert.Equal(t, []string{
		"ALTER TABLE public.parent SET LOGGED;",
		"ALTER TABLE app.child SET LOGGED;",
	}, stmts)
}

// DiffTables carries the transitions in their own field, out of Stmts, which
// the caller sorts by object dependency.
func TestDiffTables_PersistenceStmts(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	cur := newTable("public", "sessions")
	cur.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	current.Set("public.sessions", cur)

	des := newTable("public", "sessions")
	des.Unlogged = true
	des.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	desired.Set("public.sessions", des)

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.sessions SET UNLOGGED;"}, result.PersistenceStmts)
	assert.Empty(t, result.Stmts)
}

// A partitioned table is left alone: the statement would report success and
// change nothing, so the diff would repeat it on every run.
func TestDiffPersistence_PartitionedSkipped(t *testing.T) {
	current := &model.Table{Schema: "public", Name: "events", Unlogged: true, Partitioned: true}
	desired := &model.Table{Schema: "public", Name: "events", Partitioned: true}

	assert.Nil(t, diffPersistence(desired.FQTN(), current, desired))
}
