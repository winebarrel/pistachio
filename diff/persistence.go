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
// logged go first; a cross-direction pair either has no constraint between it
// or describes a desired schema PostgreSQL rejects whichever order this picks.
//
// Only the tables that change are ordered. One that keeps its persistence is
// already on the right side of the rule, or the desired schema breaks it
// regardless. The desired foreign keys are what the order is built from, since
// those are the keys in force here: the drops have already run and the adds
// come later.
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
// set. An unqualified reference is read as the owning table's schema, the way
// normalizeFKSchema reads it. Mutual foreign keys have no valid order at all,
// and the tables in such a cycle keep the order they came in.
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
			if fk.RefTable == nil {
				continue
			}
			schema := c.table.Schema
			if fk.RefSchema != nil {
				schema = *fk.RefSchema
			}
			if j, ok := index[model.Ident(schema, *fk.RefTable)]; ok && j != i {
				refs[i] = append(refs[i], j)
			}
		}
	}

	const (
		unvisited = iota
		open
		done
	)
	state := make([]int, len(changes))
	ordered := make([]*persistenceChange, 0, len(changes))
	var visit func(int)
	visit = func(i int) {
		if state[i] != unvisited {
			return
		}
		state[i] = open
		for _, j := range refs[i] {
			visit(j)
		}
		state[i] = done
		ordered = append(ordered, changes[i])
	}
	for i := range changes {
		visit(i)
	}
	return ordered
}
