package diff

import (
	"slices"

	"github.com/winebarrel/pistachio/model"
)

// persistenceChange is one table's logged <-> unlogged transition, kept next to
// the desired table so the ordering below can read its foreign keys.
type persistenceChange struct {
	table *model.Table
	stmt  string
}

// diffPersistence returns the logged <-> unlogged transition for a table that
// holds storage, or nil when there is nothing to change.
//
// A partitioned table is skipped. PostgreSQL changes persistence by rewriting
// the table, and a partitioned one has nothing to rewrite, so ALTER TABLE ...
// SET LOGGED reports success and leaves relpersistence alone, on the parent and
// on the partitions under it alike; emitting it there would replan forever. The
// test is on the current table, since what decides whether the statement does
// anything is the relation already in the database. A partition holds its own
// storage and takes the transition normally.
func diffPersistence(fqtn string, current, desired *model.Table) *persistenceChange {
	if current.Partitioned || current.Unlogged == desired.Unlogged {
		return nil
	}
	stmt := "ALTER TABLE " + fqtn + " SET LOGGED;"
	if desired.Unlogged {
		stmt = "ALTER TABLE " + fqtn + " SET UNLOGGED;"
	}
	return &persistenceChange{table: desired, stmt: stmt}
}

// orderPersistenceChanges puts the transitions in an order PostgreSQL accepts.
// It refuses to leave a logged table referencing an unlogged one and checks it
// on every ALTER, so a table turning unlogged waits for the tables that
// reference it, and a table turning logged waits for the tables it references.
// The two directions therefore run in opposite orders, and the tables turning
// logged go first. Nothing is owed to a cross-direction pair. Take X
// referencing Y: X turning unlogged next to Y turning logged would need a
// logged X already referencing an unlogged Y, which is the state the rule
// forbids, so it never reaches here. The mirror, X turning logged next to Y
// turning unlogged, does reach here and asks for a logged X referencing an
// unlogged Y, which PostgreSQL rejects whichever order this picks.
//
// Only the tables that change are ordered. One that keeps its persistence is
// already on the right side of the rule, or the desired schema breaks it
// regardless. The order is built from the desired foreign keys, which are a
// superset of the ones in force at this point: the drops have already run, so
// what exists here is the current keys intersected with the desired, and a key
// that only the desired side has is added later. Every extra edge tightens an
// order that was valid without it, so the superset is safe. It bites in one
// place, which is that a key added in the same plan can close a cycle the keys
// in force do not have, and the tables in a cycle keep their input order.
func orderPersistenceChanges(changes []*persistenceChange) []string {
	var toLogged, toUnlogged []*persistenceChange
	for _, c := range changes {
		if c.table.Unlogged {
			toUnlogged = append(toUnlogged, c)
		} else {
			toLogged = append(toLogged, c)
		}
	}

	stmts := make([]string, 0, len(changes))
	for _, c := range sortByRefs(toLogged) {
		stmts = append(stmts, c.stmt)
	}
	referencingFirst := sortByRefs(toUnlogged)
	slices.Reverse(referencingFirst)
	for _, c := range referencingFirst {
		stmts = append(stmts, c.stmt)
	}
	return stmts
}

// sortByRefs orders the changes so a table comes after the tables it
// references, counting only references that reach another table in the same
// set. Mutual foreign keys have no valid order at all, and the tables in such
// a cycle keep the order they came in, so a cycle is not an error here and
// nothing has to detect one.
func sortByRefs(changes []*persistenceChange) []*persistenceChange {
	if len(changes) < 2 {
		return changes
	}

	index := make(map[string]int, len(changes))
	for i, c := range changes {
		index[c.table.FQTN()] = i
	}
	refs := make([][]int, len(changes))
	for i, c := range changes {
		for _, fk := range c.table.ForeignKeys.CollectValues() {
			// A reference written without a schema does not arrive here
			// without one: the parser fills RefSchema from the owning table's
			// schema, and the catalog reads it from pg_namespace. So there is
			// no bare name to resolve, and nothing like
			// toposort.resolveUnqualified is needed.
			if fk.RefSchema == nil || fk.RefTable == nil {
				continue
			}
			if j, ok := index[model.Ident(*fk.RefSchema, *fk.RefTable)]; ok && j != i {
				refs[i] = append(refs[i], j)
			}
		}
	}

	visited := make([]bool, len(changes))
	ordered := make([]*persistenceChange, 0, len(changes))
	var visit func(int)
	visit = func(i int) {
		if visited[i] {
			return
		}
		visited[i] = true
		for _, j := range refs[i] {
			visit(j)
		}
		ordered = append(ordered, changes[i])
	}
	for i := range changes {
		visit(i)
	}
	return ordered
}
