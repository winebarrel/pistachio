package toposort

import "fmt"

// SortSQL parses SQL containing multiple CREATE statements and returns
// the individual statements sorted in dependency order (dependencies first).
// An optional defaultSchema can be provided to qualify unqualified identifiers
// (defaults to "public").
func SortSQL(sql string, defaultSchema ...string) ([]string, error) {
	stmts, err := ExtractDeps(sql, defaultSchema...)
	if err != nil {
		return nil, err
	}

	if len(stmts) == 0 {
		return nil, nil
	}

	g := NewGraph()
	stmtByName := make(map[string]*StmtInfo, len(stmts))

	for _, s := range stmts {
		g.AddNode(s.Name)
		stmtByName[s.Name] = s
	}

	for _, s := range stmts {
		for _, dep := range s.Deps {
			g.AddEdge(s.Name, dep)
		}
	}

	order, err := g.Sort()
	if err != nil {
		return nil, fmt.Errorf("topological sort failed: %w", err)
	}

	sorted := make([]string, 0, len(order))
	for _, name := range order {
		if s, ok := stmtByName[name]; ok {
			sorted = append(sorted, s.SQL)
		}
	}

	return sorted, nil
}
