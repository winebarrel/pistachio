package toposort

import (
	"fmt"
	"sort"
	"strings"

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
		if dep := resolveBaseTypeDep(d.BaseType, defined); dep != "" {
			g.AddEdge(k, dep)
		}
	}

	// Tables: may depend on enums/domains (column types) and other tables (FKs)
	for k, t := range tables.All() {
		g.AddNode(k)

		// Column type dependencies
		if t.Columns != nil {
			for _, col := range t.Columns.CollectValues() {
				if dep := resolveBaseTypeDep(col.TypeName, defined); dep != "" {
					g.AddEdge(k, dep)
				}
			}
		}

		// FK dependencies
		if t.ForeignKeys != nil {
			for _, fk := range t.ForeignKeys.CollectValues() {
				refSchema := "public"
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
		deps := extractViewDeps(v.Definition, defined)
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

// resolveBaseTypeDep checks if a type name refers to a defined object.
// Handles both schema-qualified ("public.status") and unqualified ("status") names.
func resolveBaseTypeDep(typeName string, defined map[string]bool) string {
	if typeName == "" {
		return ""
	}

	// Try as-is (already schema-qualified)
	if defined[typeName] {
		return typeName
	}

	// Try with public schema prefix
	if !strings.Contains(typeName, ".") {
		qualified := "public." + typeName
		if defined[qualified] {
			return qualified
		}
	}

	// Array types: strip trailing []
	base := strings.TrimSuffix(typeName, "[]")
	if base != typeName {
		return resolveBaseTypeDep(base, defined)
	}

	return ""
}

// extractViewDeps parses a view definition SQL to find referenced tables/views.
// Uses simple identifier extraction since view definitions are already deparsed SQL.
func extractViewDeps(definition string, defined map[string]bool) []string {
	seen := make(map[string]bool)

	// Check each defined object name to see if it appears in the view definition
	for name := range defined {
		if containsIdentifier(definition, name) {
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

// containsIdentifier checks if a SQL string references a schema-qualified identifier.
func containsIdentifier(sql, ident string) bool {
	return strings.Contains(sql, ident)
}
