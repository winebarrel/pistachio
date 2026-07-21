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
// Sequences are leaf nodes; a table depends on a sequence when a column
// default references it via nextval, so the sequence is created first.
func OrderFromSchema(
	enums *orderedmap.Map[string, *model.Enum],
	domains *orderedmap.Map[string, *model.Domain],
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
	sequences *orderedmap.Map[string, *model.Sequence],
) ([]string, error) {
	g := newGraph()
	defined := collectDefined(enums, domains, tables, views, sequences)

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
		if dep := resolveTypeDep(d.BaseType, d.Schema, defined); dep != "" {
			g.AddEdge(k, dep)
		}
	}

	// Tables: may depend on enums/domains (column types), sequences (column
	// defaults via nextval), and other tables (FKs)
	for k, t := range tables.All() {
		g.AddNode(k)

		// Column type dependencies
		if t.Columns != nil {
			for _, col := range t.Columns.CollectValues() {
				if dep := resolveTypeDep(col.TypeName, t.Schema, defined); dep != "" {
					g.AddEdge(k, dep)
				}
				if col.Default != nil {
					for _, dep := range extractSeqDeps(*col.Default, t.Schema, defined) {
						g.AddEdge(k, dep)
					}
				}
			}
		}

		// FK dependencies. Use model.Ident so the lookup matches the map keys
		// (which are also model.Ident-formed) for non-safe identifiers like
		// quoted or reserved-word names.
		if t.ForeignKeys != nil {
			for _, fk := range t.ForeignKeys.CollectValues() {
				if fk.RefTable == nil {
					continue
				}
				if fk.RefSchema != nil {
					ref := model.Ident(*fk.RefSchema, *fk.RefTable)
					if defined[ref] {
						g.AddEdge(k, ref)
					}
				} else if ref := resolveUnqualified(model.Ident(*fk.RefTable), t.Schema, defined); ref != "" {
					g.AddEdge(k, ref)
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
	sequences *orderedmap.Map[string, *model.Sequence],
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
			if q := resolveUnqualified(lit, defaultSchema, defined); q != "" {
				deps = append(deps, q)
			}
		}
	}
	return deps
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

	// Unqualified names: try default schema, then public (search_path fallback).
	if !strings.Contains(typeName, ".") {
		if q := resolveUnqualified(typeName, defaultSchema, defined); q != "" {
			return q
		}
	}

	// Array types: strip trailing []
	base := strings.TrimSuffix(typeName, "[]")
	if base != typeName {
		return resolveTypeDep(base, defaultSchema, defined)
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
func resolveUnqualified(name, defaultSchema string, defined map[string]bool) string {
	if q := model.Ident(defaultSchema) + "." + name; defined[q] {
		return q
	}
	if defaultSchema != "public" {
		if q := "public." + name; defined[q] {
			return q
		}
	}
	return ""
}

// extractViewDeps parses a view definition SQL to find referenced tables/views.
// Uses pg_query to parse the SELECT statement and extract RangeVar references.
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
		if name := qualifyRangeVar(rv, defaultSchema, defined); name != "" {
			seen[name] = true
		}
		return
	}

	// Check if this is a SelectStmt and walk its parts
	if ss := node.GetSelectStmt(); ss != nil {
		// CTEs (WITH clause)
		if ss.WithClause != nil {
			for _, cte := range ss.WithClause.Ctes {
				if c := cte.GetCommonTableExpr(); c != nil {
					collectRangeVars(c.Ctequery, defaultSchema, defined, seen)
				}
			}
		}
		// FROM clause
		for _, from := range ss.FromClause {
			collectRangeVars(from, defaultSchema, defined, seen)
		}
		// Target list (scalar subqueries in SELECT)
		for _, target := range ss.TargetList {
			if rt := target.GetResTarget(); rt != nil && rt.Val != nil {
				collectRangeVars(rt.Val, defaultSchema, defined, seen)
			}
		}
		if ss.WhereClause != nil {
			collectRangeVars(ss.WhereClause, defaultSchema, defined, seen)
		}
		if ss.HavingClause != nil {
			collectRangeVars(ss.HavingClause, defaultSchema, defined, seen)
		}
		// Set operations (UNION, INTERSECT, EXCEPT)
		if ss.Larg != nil {
			collectRangeVars(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Larg}}, defaultSchema, defined, seen)
		}
		if ss.Rarg != nil {
			collectRangeVars(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Rarg}}, defaultSchema, defined, seen)
		}
		return
	}

	// Walk JoinExpr (including join qualifiers that may contain subqueries)
	if join := node.GetJoinExpr(); join != nil {
		collectRangeVars(join.Larg, defaultSchema, defined, seen)
		collectRangeVars(join.Rarg, defaultSchema, defined, seen)
		if join.Quals != nil {
			collectRangeVars(join.Quals, defaultSchema, defined, seen)
		}
		return
	}

	// Walk subselect
	if sub := node.GetRangeSubselect(); sub != nil {
		collectRangeVars(sub.Subquery, defaultSchema, defined, seen)
		return
	}

	// Walk sublink (subquery in WHERE/HAVING clause, e.g., EXISTS, IN)
	if sl := node.GetSubLink(); sl != nil {
		collectRangeVars(sl.Subselect, defaultSchema, defined, seen)
		return
	}

	// Walk expression nodes that may contain subqueries
	if expr := node.GetAExpr(); expr != nil {
		collectRangeVars(expr.Lexpr, defaultSchema, defined, seen)
		collectRangeVars(expr.Rexpr, defaultSchema, defined, seen)
		return
	}

	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		for _, arg := range boolExpr.Args {
			collectRangeVars(arg, defaultSchema, defined, seen)
		}
		return
	}

	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.Args {
			collectRangeVars(arg, defaultSchema, defined, seen)
		}
		return
	}

	if ce := node.GetCoalesceExpr(); ce != nil {
		for _, arg := range ce.Args {
			collectRangeVars(arg, defaultSchema, defined, seen)
		}
		return
	}

	if cs := node.GetCaseExpr(); cs != nil {
		if cs.Arg != nil {
			collectRangeVars(cs.Arg, defaultSchema, defined, seen)
		}
		for _, when := range cs.Args {
			if w := when.GetCaseWhen(); w != nil {
				collectRangeVars(w.Expr, defaultSchema, defined, seen)
				collectRangeVars(w.Result, defaultSchema, defined, seen)
			}
		}
		if cs.Defresult != nil {
			collectRangeVars(cs.Defresult, defaultSchema, defined, seen)
		}
		return
	}
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
	return resolveUnqualified(model.Ident(rv.Relname), defaultSchema, defined)
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
	deps := make([]string, 0, len(seen))
	for dep := range seen {
		deps = append(deps, dep)
	}
	sort.Strings(deps)
	return deps
}
