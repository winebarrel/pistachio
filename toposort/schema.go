package toposort

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

// OrderFromSchema builds a dependency graph from parsed model objects and
// returns the object names in topological order (dependencies first).
// Sequences are leaf nodes; a table depends on a sequence when a column
// default references it via nextval, so the sequence is created first.
func OrderFromSchema(
	enums *orderedmap.Map[string, *model.Enum],
	domains *orderedmap.Map[string, *model.Domain],
	compositeTypes *orderedmap.Map[string, *model.CompositeType],
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
	sequences *orderedmap.Map[string, *model.Sequence],
	routines *orderedmap.Map[string, *model.Routine],
) ([]string, error) {
	g := newGraph()
	defined := collectDefined(enums, domains, compositeTypes, tables, views, sequences)

	// Enums: no dependencies (leaf nodes)
	for k := range enums.Keys() {
		g.AddNode(k)
	}

	// Sequences: no dependencies (leaf nodes)
	for k := range sequences.Keys() {
		g.AddNode(k)
	}

	// Domains: may depend on enums or other domains via base type
	for k, d := range domains.All() {
		g.AddNode(k)
		if dep := resolveTypeDep(d.BaseType, d.Schema, k, defined); dep != "" {
			g.AddEdge(k, dep)
		}
	}

	// Composite types: may depend on enums, domains, or other composite types
	// via attribute types.
	for k, ct := range compositeTypes.All() {
		g.AddNode(k)
		for _, a := range ct.Attributes {
			if dep := resolveTypeDep(a.TypeName, ct.Schema, k, defined); dep != "" {
				g.AddEdge(k, dep)
			}
		}
	}

	// Tables: may depend on enums/domains (column types) and sequences (column
	// defaults via nextval). Foreign keys draw no edge. The plan drops them
	// before the tables and adds them after, so they never order the tables.
	for k, t := range tables.All() {
		g.AddNode(k)

		// Column type dependencies
		if t.Columns != nil {
			for _, col := range t.Columns.CollectValues() {
				if dep := resolveTypeDep(col.TypeName, t.Schema, k, defined); dep != "" {
					g.AddEdge(k, dep)
				}
				if col.Default != nil {
					for _, dep := range extractSeqDeps(*col.Default, t.Schema, defined) {
						g.AddEdge(k, dep)
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
	addViewDeps(g, views, defined)

	addRoutineDeps(g, routines, tables, views, defined)

	order, err := g.Sort()
	if err != nil {
		return nil, fmt.Errorf("schema dependency sort failed: %w", err)
	}

	return order, nil
}

// OrderViews returns the views in dependency order, with nothing else in the
// graph. It is for a caller that has a set of views to order and no whole
// schema to hand over, which OrderFromSchema wants.
//
// A reference to anything but another view in the set resolves to nothing and
// drops out, so what is left are the view-to-view edges. Those cannot hold a
// cycle, since PostgreSQL rejects a view that reads a view reading it back,
// which is what lets this succeed where OrderFromSchema fails on a cycle
// elsewhere in the schema.
func OrderViews(views *orderedmap.Map[string, *model.View]) ([]string, error) {
	g := newGraph()

	defined := make(map[string]bool, views.Len())
	for k := range views.Keys() {
		defined[k] = true
	}

	addViewDeps(g, views, defined)

	order, err := g.Sort()
	if err != nil {
		return nil, fmt.Errorf("view dependency sort failed: %w", err)
	}

	return order, nil
}

// addViewDeps gives each view a node and an edge to every object in defined
// that its definition reads.
func addViewDeps(g *graph, views *orderedmap.Map[string, *model.View], defined map[string]bool) {
	for k, v := range views.All() {
		g.AddNode(k)
		for _, dep := range extractViewDeps(v.Definition, v.Schema, defined) {
			if dep != k {
				g.AddEdge(k, dep)
			}
		}
	}
}

// collectDefined returns a set of all defined object names.
func collectDefined(
	enums *orderedmap.Map[string, *model.Enum],
	domains *orderedmap.Map[string, *model.Domain],
	compositeTypes *orderedmap.Map[string, *model.CompositeType],
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
	sequences *orderedmap.Map[string, *model.Sequence],
) map[string]bool {
	defined := make(map[string]bool)
	for k := range enums.Keys() {
		defined[k] = true
	}
	for k := range domains.Keys() {
		defined[k] = true
	}
	for k := range compositeTypes.Keys() {
		defined[k] = true
	}
	for k := range tables.Keys() {
		defined[k] = true
	}
	for k := range views.Keys() {
		defined[k] = true
	}
	for k := range sequences.Keys() {
		defined[k] = true
	}
	return defined
}

// extractSeqDeps finds the sequences referenced by nextval(...) in a column
// default expression and returns their resolved (schema-qualified) names.
// Only sequences present in defined are returned; references to unmanaged
// (e.g. serial/identity-owned) sequences are ignored.
func extractSeqDeps(defaultExpr, defaultSchema string, defined map[string]bool) []string {
	var deps []string
	rest := defaultExpr
	for {
		_, afterCall, found := strings.Cut(rest, "nextval(")
		if !found {
			break
		}
		_, afterOpen, found := strings.Cut(afterCall, "'")
		if !found {
			break
		}
		lit, remainder, found := strings.Cut(afterOpen, "'")
		if !found {
			break
		}
		rest = remainder

		// lit is the identifier as pg_get_expr / pg_query deparse emit it:
		// already quoted when the name requires it, matching the form of both
		// the `defined` keys and resolveUnqualified's name argument. Do not
		// re-quote it with model.Ident, which would double-quote a name like
		// "MySeq" and miss the lookup.
		if defined[lit] {
			deps = append(deps, lit)
		} else if !strings.Contains(lit, ".") {
			// No self to skip: a table and a sequence cannot share a name.
			if q := resolveUnqualified(lit, defaultSchema, "", defined); q != "" {
				deps = append(deps, q)
			}
		}
	}
	return deps
}

// resolveTypeDep checks if a type name refers to a defined object.
// Handles schema-qualified ("public.status"), unqualified ("status"),
// and array types ("status[]"). Uses defaultSchema to qualify unqualified names.
//
// self is the key of the object the type is written on, or "" for a caller
// that cannot collide with one. An object named after the type it is built on
// resolves to itself, which is no dependency at all: the type it means is the
// one further along the search path, as it was when the object was created.
func resolveTypeDep(typeName, defaultSchema, self string, defined map[string]bool) string {
	if typeName == "" {
		return ""
	}

	// Try as-is (already schema-qualified)
	if defined[typeName] && typeName != self {
		return typeName
	}

	// Unqualified names: try default schema, then public (search_path fallback).
	if !strings.Contains(typeName, ".") {
		if q := resolveUnqualified(typeName, defaultSchema, self, defined); q != "" {
			return q
		}
	}

	// Array types: strip trailing []
	base := strings.TrimSuffix(typeName, "[]")
	if base != typeName {
		return resolveTypeDep(base, defaultSchema, self, defined)
	}

	return ""
}

// resolveUnqualified returns the schema-qualified FQDN of an unqualified
// identifier by modeling PostgreSQL's default search_path: try defaultSchema
// first, then fall back to public. Returns "" if no defined object matches.
//
// `defined` keys are model.Ident-formed (schema quoted when needed), so the
// schema component must go through model.Ident too; raw concatenation would
// miss schemas like "MySchema". `name` is taken as-is because callers already
// normalize it to the form that appears in `defined` (typically the deparsed
// identifier for type names, or model.Ident-quoted for raw RangeVar names).
// self is skipped the way resolveTypeDep describes, so a name that matches the
// object it is written on falls through to the next schema on the path.
func resolveUnqualified(name, defaultSchema, self string, defined map[string]bool) string {
	if q := model.Ident(defaultSchema) + "." + name; defined[q] && q != self {
		return q
	}
	if defaultSchema != "public" {
		if q := "public." + name; defined[q] && q != self {
			return q
		}
	}
	return ""
}

// extractViewDeps parses a view definition SQL to find referenced tables/views.
// Uses pg_query to parse the SELECT statement and extract RangeVar references.
//
// The walk reaches every node kind, which the older enumeration could not do: a
// sub-query under a cast, GREATEST, an array or row constructor, a subscript,
// ORDER BY, GROUP BY, LIMIT, an OVER clause or an aggregate FILTER named a
// relation the view was then ordered without waiting for. Two new views in one
// run, one reading the other from such a position, applied in the wrong order.
func extractViewDeps(definition, defaultSchema string, defined map[string]bool) []string {
	seen := make(map[string]bool)

	// Parse the view definition directly as a SELECT statement.
	// pg_get_viewdef returns a standalone SELECT, so it can be parsed as-is.
	def := strings.TrimSpace(definition)
	def = strings.TrimSuffix(def, ";")
	result, err := pg_query.Parse(def)
	if err != nil {
		// Fallback: try substring matching if parsing fails
		return extractViewDepsFallback(definition, defaultSchema, defined)
	}

	for _, stmt := range result.Stmts {
		pgast.Walk(stmt.Stmt, pgast.WalkOptions{}, func(_ pgast.Ctx, node *pg_query.Node) *pg_query.Node {
			if rv := node.GetRangeVar(); rv != nil {
				if name := qualifyRangeVar(rv, defaultSchema, defined); name != "" {
					seen[name] = true
				}
			}
			return node
		})
	}

	return slices.Sorted(maps.Keys(seen))
}

// qualifyRangeVar returns the schema-qualified FQDN of a RangeVar that exists
// in defined. If the RangeVar has no schema, defaultSchema and "public" are
// tried in order, modeling PostgreSQL's default search_path. Returns "" when
// the RangeVar does not match any defined object.
//
// rv.Schemaname / rv.Relname are raw identifiers from pg_query, so both are
// run through model.Ident to match the way `defined` keys are formed.
func qualifyRangeVar(rv *pg_query.RangeVar, defaultSchema string, defined map[string]bool) string {
	if rv == nil || rv.Relname == "" {
		return ""
	}
	if rv.Schemaname != "" {
		q := model.Ident(rv.Schemaname, rv.Relname)
		if defined[q] {
			return q
		}
		return ""
	}
	// No self to skip: a view cannot name itself in its own definition.
	return resolveUnqualified(model.Ident(rv.Relname), defaultSchema, "", defined)
}

// extractViewDepsFallback uses substring matching as a fallback when
// pg_query parsing fails. It checks both fully-qualified names and
// unqualified names (with defaultSchema or public prefix, matching the
// search_path semantics modeled elsewhere) to handle schemaless refs.
func extractViewDepsFallback(definition, defaultSchema string, defined map[string]bool) []string {
	seen := make(map[string]bool)
	for name := range defined {
		if strings.Contains(definition, name) {
			seen[name] = true
			continue
		}
		// Try matching the unqualified part (e.g., "users" for "public.users").
		// `name` is a model.Ident-formed key, so its schema portion may be
		// quoted (e.g. `"MySchema"`); compare against the same form.
		parts := strings.SplitN(name, ".", 2)
		if len(parts) == 2 && (parts[0] == model.Ident(defaultSchema) || parts[0] == "public") {
			if strings.Contains(definition, parts[1]) {
				seen[name] = true
			}
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// RoutineNode returns the graph node name for a routine, given its
// schema-qualified name. The identity argument list is left out, so an overload
// set shares one node: the ordering rules below hold for every overload alike,
// and a CREATE statement spells its parameters with names and defaults, which
// no FQRN could be recovered from.
//
// The "routine:" prefix keeps the node out of the relation and type namespace.
// A schema may hold a table and a function of the same name, and without the
// prefix the two would collapse into one node and read as a cycle.
func RoutineNode(qualifiedName string) string {
	if qualifiedName == "" {
		return ""
	}
	return "routine:" + qualifiedName
}

// addRoutineDeps places routines between the types they name and the tables
// that call them.
//
// A routine depends on the types in its signature, so it is created after
// them. Every table and view is then made to depend on every routine, so
// routines come first: a CHECK constraint, a GENERATED expression, an index
// expression, a policy or a trigger can call one, and those run as part of the
// table DDL. The edge is drawn wholesale rather than by reading each
// expression, since the answer is the same "routine first" either way.
//
// A signature can name a relation as well as a type: RETURNS SETOF <table>, or
// a table row type as a parameter. The routine then follows that relation, and
// a table or view it reaches that way gets no wholesale edge, which would
// close a cycle. When two routines name two different relations, the first
// still precedes the other relation and the second follows both.
//
// The reverse direction is otherwise absent. A LANGUAGE sql body that reads a
// table would want the table first, which cannot hold at the same time as the
// rule above, so that case is a documented limitation rather than an edge.
//
// A routine with a SQL-standard body is the exception. PostgreSQL parses such
// a body at creation time, so the routine follows every table, view and
// routine its body names, the way a view does, and gets no wholesale edge. A
// view that calls it follows it in turn. Its statements run with the views,
// after every table, so a CHECK or an index that calls it cannot be created
// in the same run. An overload set that mixes the two forms shares one node
// and is treated as the plain form.
func addRoutineDeps(
	g *graph,
	routines *orderedmap.Map[string, *model.Routine],
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
	defined map[string]bool,
) {
	if routines.Len() == 0 {
		return
	}

	nodes := make([]string, 0, routines.Len())
	// atomic maps every routine node to whether each routine under it has a
	// SQL-standard body.
	atomic := make(map[string]bool, routines.Len())

	for _, r := range routines.All() {
		node := RoutineNode(model.Ident(r.Schema, r.Name))
		if _, ok := atomic[node]; !ok {
			atomic[node] = true
			nodes = append(nodes, node)
		}
		atomic[node] = atomic[node] && r.Atomic()
		g.AddNode(node)

		// No self-edge is possible: resolveTypeDep returns a key from defined,
		// and nothing there carries the routine: prefix.
		for _, a := range r.Args {
			if dep := resolveTypeDep(a.Type, r.Schema, "", defined); dep != "" {
				g.AddEdge(node, dep)
			}
		}
		if dep := resolveTypeDep(r.ReturnType, r.Schema, "", defined); dep != "" {
			g.AddEdge(node, dep)
		}
	}

	for _, r := range routines.All() {
		if !r.Atomic() {
			continue
		}
		node := RoutineNode(model.Ident(r.Schema, r.Name))
		for _, dep := range extractBodyDeps(r.SQLBody, r.Schema, defined, atomic) {
			if dep != node {
				g.AddEdge(node, dep)
			}
		}
	}

	atomicOnly := make(map[string]bool, len(atomic))
	for node, ok := range atomic {
		if ok {
			atomicOnly[node] = true
		}
	}
	for k, v := range views.All() {
		for _, dep := range extractCallDeps(v.Definition, v.Schema, atomicOnly) {
			g.AddEdge(k, dep)
		}
	}

	// An edge into a routine does not change what the routine reaches, so one
	// walk per routine covers every table and view.
	addDependents := func(keys func(func(string) bool)) {
		for _, node := range nodes {
			if atomic[node] {
				continue
			}
			reach := g.reachable(node)
			for k := range keys {
				if reach[k] {
					continue
				}
				g.AddEdge(k, node)
			}
		}
	}
	addDependents(tables.Keys())
	addDependents(views.Keys())
}

// extractBodyDeps returns what a SQL-standard routine body names: the tables
// and views in defined it reads, and the routines in routines it calls.
func extractBodyDeps(body, defaultSchema string, defined, routines map[string]bool) []string {
	result, err := pg_query.Parse("CREATE FUNCTION _f() " + body)
	if err != nil {
		return nil
	}

	seen := make(map[string]bool)
	for _, stmt := range result.Stmts {
		pgast.Walk(stmt.Stmt, pgast.WalkOptions{}, func(_ pgast.Ctx, node *pg_query.Node) *pg_query.Node {
			if rv := node.GetRangeVar(); rv != nil {
				if name := qualifyRangeVar(rv, defaultSchema, defined); name != "" {
					seen[name] = true
				}
			}
			if fc := node.GetFuncCall(); fc != nil {
				if name := resolveRoutineCall(fc.Funcname, defaultSchema, routines); name != "" {
					seen[name] = true
				}
			}
			return node
		})
	}

	return slices.Sorted(maps.Keys(seen))
}

// extractCallDeps returns the routines in routines a view definition calls.
func extractCallDeps(definition, defaultSchema string, routines map[string]bool) []string {
	if len(routines) == 0 {
		return nil
	}

	def := strings.TrimSuffix(strings.TrimSpace(definition), ";")
	result, err := pg_query.Parse(def)
	if err != nil {
		return nil
	}

	seen := make(map[string]bool)
	for _, stmt := range result.Stmts {
		pgast.Walk(stmt.Stmt, pgast.WalkOptions{}, func(_ pgast.Ctx, node *pg_query.Node) *pg_query.Node {
			if fc := node.GetFuncCall(); fc != nil {
				if name := resolveRoutineCall(fc.Funcname, defaultSchema, routines); name != "" {
					seen[name] = true
				}
			}
			return node
		})
	}

	return slices.Sorted(maps.Keys(seen))
}

// resolveRoutineCall returns the routine node a function call names, trying
// defaultSchema and then public for an unqualified name, or "" when the call
// names none of routines.
func resolveRoutineCall(funcname []*pg_query.Node, defaultSchema string, routines map[string]bool) string {
	parts := make([]string, 0, len(funcname))
	for _, n := range funcname {
		parts = append(parts, n.GetString_().GetSval())
	}

	var candidates []string
	switch len(parts) {
	case 1:
		candidates = []string{model.Ident(defaultSchema, parts[0]), model.Ident("public", parts[0])}
	case 2:
		candidates = []string{model.Ident(parts[0], parts[1])}
	}
	for _, c := range candidates {
		if node := RoutineNode(c); routines[node] {
			return node
		}
	}
	return ""
}
