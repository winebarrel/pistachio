package toposort

import (
	"fmt"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

// OrderFromSchema builds a dependency graph from parsed model objects and
// returns the object names in topological order (dependencies first).
func OrderFromSchema(
	enums *orderedmap.Map[string, *model.Enum],
	domains *orderedmap.Map[string, *model.Domain],
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
) ([]string, error) {
	g := NewGraph()
	defined := collectDefined(enums, domains, tables, views)

	// Enums: no dependencies (leaf nodes)
	for k := range enums.Keys() {
		g.AddNode(k)
	}

	// Domains: may depend on enums or other domains via base type
	for k, d := range domains.All() {
		g.AddNode(k)
		if dep := resolveTypeDep(d.BaseType, d.Schema, defined); dep != "" {
			g.AddEdge(k, dep)
		}
	}

	// Tables: may depend on enums/domains (column types) and other tables (FKs)
	for k, t := range tables.All() {
		g.AddNode(k)

		// Column type dependencies
		if t.Columns != nil {
			for _, col := range t.Columns.CollectValues() {
				if dep := resolveTypeDep(col.TypeName, t.Schema, defined); dep != "" {
					g.AddEdge(k, dep)
				}
			}
		}

		// FK dependencies
		if t.ForeignKeys != nil {
			for _, fk := range t.ForeignKeys.CollectValues() {
				refSchema := t.Schema
				if fk.RefSchema != nil {
					refSchema = *fk.RefSchema
				}
				if fk.RefTable != nil {
					ref := refSchema + "." + *fk.RefTable
					if defined[ref] {
						g.AddEdge(k, ref)
					}
				}
			}
		}

		// Partition parent dependency
		if t.PartitionOf != nil {
			if defined[*t.PartitionOf] {
				g.AddEdge(k, *t.PartitionOf)
			}
		}
	}

	// Views: depend on tables/views referenced in their definition
	for k, v := range views.All() {
		g.AddNode(k)
		deps := extractViewDeps(v.Definition, v.Schema, defined)
		for _, dep := range deps {
			if dep != k {
				g.AddEdge(k, dep)
			}
		}
	}

	order, err := g.Sort()
	if err != nil {
		return nil, fmt.Errorf("schema dependency sort failed: %w", err)
	}

	return order, nil
}

// collectDefined returns a set of all defined object names.
func collectDefined(
	enums *orderedmap.Map[string, *model.Enum],
	domains *orderedmap.Map[string, *model.Domain],
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
) map[string]bool {
	defined := make(map[string]bool)
	for k := range enums.Keys() {
		defined[k] = true
	}
	for k := range domains.Keys() {
		defined[k] = true
	}
	for k := range tables.Keys() {
		defined[k] = true
	}
	for k := range views.Keys() {
		defined[k] = true
	}
	return defined
}

// resolveTypeDep checks if a type name refers to a defined object.
// Handles schema-qualified ("public.status"), unqualified ("status"),
// and array types ("status[]"). Uses defaultSchema to qualify unqualified names.
func resolveTypeDep(typeName, defaultSchema string, defined map[string]bool) string {
	if typeName == "" {
		return ""
	}

	// Try as-is (already schema-qualified)
	if defined[typeName] {
		return typeName
	}

	// Try with default schema prefix for unqualified names
	if !strings.Contains(typeName, ".") {
		qualified := defaultSchema + "." + typeName
		if defined[qualified] {
			return qualified
		}
	}

	// Array types: strip trailing []
	base := strings.TrimSuffix(typeName, "[]")
	if base != typeName {
		return resolveTypeDep(base, defaultSchema, defined)
	}

	return ""
}

// extractViewDeps parses a view definition SQL to find referenced tables/views.
// Uses pg_query to parse the SELECT statement and extract RangeVar references.
func extractViewDeps(definition, defaultSchema string, defined map[string]bool) []string {
	seen := make(map[string]bool)

	// Parse the view definition as a SELECT statement
	sql := "SELECT * FROM (" + definition + ") _sub"
	result, err := pg_query.Parse(sql)
	if err != nil {
		// Fallback: try substring matching if parsing fails
		return extractViewDepsFallback(definition, defined)
	}

	// Walk the AST to find RangeVar nodes
	for _, stmt := range result.Stmts {
		collectRangeVars(stmt.Stmt, defaultSchema, defined, seen)
	}

	deps := make([]string, 0, len(seen))
	for dep := range seen {
		deps = append(deps, dep)
	}
	sort.Strings(deps)
	return deps
}

// collectRangeVars recursively walks a pg_query Node tree to find all
// RangeVar references (table/view names in FROM clauses, JOINs, subqueries).
func collectRangeVars(node *pg_query.Node, defaultSchema string, defined map[string]bool, seen map[string]bool) {
	if node == nil {
		return
	}

	// Check if this node is a RangeVar
	if rv := node.GetRangeVar(); rv != nil {
		name := qualifyRangeVar(rv, defaultSchema)
		if name != "" && defined[name] {
			seen[name] = true
		}
		return
	}

	// Check if this is a SelectStmt and walk its parts
	if ss := node.GetSelectStmt(); ss != nil {
		for _, from := range ss.FromClause {
			collectRangeVars(from, defaultSchema, defined, seen)
		}
		if ss.WhereClause != nil {
			collectRangeVars(ss.WhereClause, defaultSchema, defined, seen)
		}
		if ss.Larg != nil {
			collectRangeVars(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Larg}}, defaultSchema, defined, seen)
		}
		if ss.Rarg != nil {
			collectRangeVars(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Rarg}}, defaultSchema, defined, seen)
		}
		return
	}

	// Walk JoinExpr
	if join := node.GetJoinExpr(); join != nil {
		collectRangeVars(join.Larg, defaultSchema, defined, seen)
		collectRangeVars(join.Rarg, defaultSchema, defined, seen)
		return
	}

	// Walk subselect
	if sub := node.GetRangeSubselect(); sub != nil {
		collectRangeVars(sub.Subquery, defaultSchema, defined, seen)
		return
	}

	// Walk sublink (subquery in WHERE clause, e.g., EXISTS, IN)
	if sl := node.GetSubLink(); sl != nil {
		collectRangeVars(sl.Subselect, defaultSchema, defined, seen)
		return
	}
}

// qualifyRangeVar returns the schema-qualified name from a RangeVar.
func qualifyRangeVar(rv *pg_query.RangeVar, defaultSchema string) string {
	if rv == nil || rv.Relname == "" {
		return ""
	}
	schema := rv.Schemaname
	if schema == "" {
		schema = defaultSchema
	}
	return schema + "." + rv.Relname
}

// extractViewDepsFallback uses substring matching as a fallback when
// pg_query parsing fails.
func extractViewDepsFallback(definition string, defined map[string]bool) []string {
	seen := make(map[string]bool)
	for name := range defined {
		if strings.Contains(definition, name) {
			seen[name] = true
		}
	}
	deps := make([]string, 0, len(seen))
	for dep := range seen {
		deps = append(deps, dep)
	}
	sort.Strings(deps)
	return deps
}
