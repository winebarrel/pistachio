package toposort

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// StmtInfo holds a parsed statement and its extracted metadata.
type StmtInfo struct {
	Name string   // Fully qualified object name (e.g. "public.users")
	SQL  string   // Deparsed SQL statement
	Deps []string // Names of objects this statement depends on
}

// ExtractDeps parses SQL containing multiple CREATE statements and returns
// dependency information for each object. It uses defaultSchema to qualify
// unqualified identifiers.
func ExtractDeps(sql string, defaultSchema ...string) ([]*StmtInfo, error) {
	schema := "public"
	if len(defaultSchema) > 0 && defaultSchema[0] != "" {
		schema = defaultSchema[0]
	}

	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	// First pass: collect all defined object names so we only track internal deps
	defined := make(map[string]bool)
	for _, rawStmt := range result.Stmts {
		if name := objectName(rawStmt, schema); name != "" {
			defined[name] = true
		}
	}

	var stmts []*StmtInfo
	for _, rawStmt := range result.Stmts {
		name := objectName(rawStmt, schema)
		if name == "" {
			continue
		}

		deparsed, err := pg_query.Deparse(&pg_query.ParseResult{
			Stmts: []*pg_query.RawStmt{rawStmt},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to deparse statement for %s: %w", name, err)
		}

		deps := extractStmtDeps(rawStmt, schema, defined)

		stmts = append(stmts, &StmtInfo{
			Name: name,
			SQL:  deparsed,
			Deps: deps,
		})
	}

	return stmts, nil
}

// objectName extracts the fully qualified name from a CREATE statement.
func objectName(rawStmt *pg_query.RawStmt, defaultSchema string) string {
	node := rawStmt.Stmt
	if node == nil {
		return ""
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_CreateStmt:
		return qualifyRangeVar(n.CreateStmt.Relation, defaultSchema)
	case *pg_query.Node_ViewStmt:
		return qualifyRangeVar(n.ViewStmt.View, defaultSchema)
	case *pg_query.Node_CreateEnumStmt:
		return typeNameFromNames(n.CreateEnumStmt.TypeName, defaultSchema)
	case *pg_query.Node_CreateDomainStmt:
		return typeNameFromNames(n.CreateDomainStmt.Domainname, defaultSchema)
	}

	return ""
}

// extractStmtDeps extracts dependency names from a statement, filtered to only
// include names that are defined in the same SQL input.
func extractStmtDeps(rawStmt *pg_query.RawStmt, defaultSchema string, defined map[string]bool) []string {
	node := rawStmt.Stmt
	if node == nil {
		return nil
	}

	seen := make(map[string]bool)
	selfName := objectName(rawStmt, defaultSchema)

	switch n := node.Node.(type) {
	case *pg_query.Node_CreateStmt:
		extractCreateStmtDeps(n.CreateStmt, defaultSchema, seen)
	case *pg_query.Node_ViewStmt:
		extractSelectDeps(n.ViewStmt.Query, defaultSchema, seen)
	case *pg_query.Node_CreateDomainStmt:
		extractDomainDeps(n.CreateDomainStmt, defaultSchema, seen)
	}

	var deps []string
	for dep := range seen {
		if dep != selfName && defined[dep] {
			deps = append(deps, dep)
		}
	}

	return deps
}

// extractCreateStmtDeps extracts dependencies from a CREATE TABLE statement.
func extractCreateStmtDeps(cs *pg_query.CreateStmt, defaultSchema string, seen map[string]bool) {
	// Column type references
	for _, elt := range cs.TableElts {
		if cd := elt.GetColumnDef(); cd != nil {
			if typeName := resolveTypeName(cd.TypeName, defaultSchema); typeName != "" {
				seen[typeName] = true
			}
			// Column-level constraints (inline FK)
			for _, con := range cd.Constraints {
				if c := con.GetConstraint(); c != nil {
					if c.Contype == pg_query.ConstrType_CONSTR_FOREIGN && c.Pktable != nil {
						seen[qualifyRangeVar(c.Pktable, defaultSchema)] = true
					}
				}
			}
		}
		// Table-level constraints
		if con := elt.GetConstraint(); con != nil {
			if con.Contype == pg_query.ConstrType_CONSTR_FOREIGN && con.Pktable != nil {
				seen[qualifyRangeVar(con.Pktable, defaultSchema)] = true
			}
		}
	}

	// Partition parent
	for _, inh := range cs.InhRelations {
		if rv := inh.GetRangeVar(); rv != nil {
			seen[qualifyRangeVar(rv, defaultSchema)] = true
		}
	}
}

// extractSelectDeps recursively extracts table references from a SELECT statement.
func extractSelectDeps(node *pg_query.Node, defaultSchema string, seen map[string]bool) {
	if node == nil {
		return
	}

	ss := node.GetSelectStmt()
	if ss == nil {
		return
	}

	// CTEs (WITH clause)
	if ss.WithClause != nil {
		for _, cte := range ss.WithClause.Ctes {
			if c := cte.GetCommonTableExpr(); c != nil {
				extractSelectDeps(c.Ctequery, defaultSchema, seen)
			}
		}
	}

	// FROM clause
	for _, from := range ss.FromClause {
		extractFromDeps(from, defaultSchema, seen)
	}

	// Target list (scalar subqueries in SELECT)
	for _, target := range ss.TargetList {
		if rt := target.GetResTarget(); rt != nil && rt.Val != nil {
			extractExprDeps(rt.Val, defaultSchema, seen)
		}
	}

	// WHERE clause
	if ss.WhereClause != nil {
		extractExprDeps(ss.WhereClause, defaultSchema, seen)
	}

	// HAVING clause
	if ss.HavingClause != nil {
		extractExprDeps(ss.HavingClause, defaultSchema, seen)
	}

	// Set operations (UNION, INTERSECT, EXCEPT)
	if ss.Larg != nil {
		extractSelectDeps(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Larg}}, defaultSchema, seen)
	}
	if ss.Rarg != nil {
		extractSelectDeps(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Rarg}}, defaultSchema, seen)
	}
}

// extractExprDeps walks expression nodes to find SubLink (subquery) references.
func extractExprDeps(node *pg_query.Node, defaultSchema string, seen map[string]bool) {
	if node == nil {
		return
	}

	if sl := node.GetSubLink(); sl != nil {
		extractSelectDeps(sl.Subselect, defaultSchema, seen)
		return
	}

	if expr := node.GetAExpr(); expr != nil {
		extractExprDeps(expr.Lexpr, defaultSchema, seen)
		extractExprDeps(expr.Rexpr, defaultSchema, seen)
		return
	}

	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		for _, arg := range boolExpr.Args {
			extractExprDeps(arg, defaultSchema, seen)
		}
		return
	}
}

// extractFromDeps extracts table references from FROM clause items.
func extractFromDeps(node *pg_query.Node, defaultSchema string, seen map[string]bool) {
	if node == nil {
		return
	}

	if rv := node.GetRangeVar(); rv != nil {
		seen[qualifyRangeVar(rv, defaultSchema)] = true
		return
	}

	if join := node.GetJoinExpr(); join != nil {
		extractFromDeps(join.Larg, defaultSchema, seen)
		extractFromDeps(join.Rarg, defaultSchema, seen)
		if join.Quals != nil {
			extractExprDeps(join.Quals, defaultSchema, seen)
		}
		return
	}

	if sub := node.GetRangeSubselect(); sub != nil {
		extractSelectDeps(sub.Subquery, defaultSchema, seen)
		return
	}
}

// extractDomainDeps extracts dependencies from a CREATE DOMAIN statement.
func extractDomainDeps(ds *pg_query.CreateDomainStmt, defaultSchema string, seen map[string]bool) {
	if typeName := resolveTypeName(ds.TypeName, defaultSchema); typeName != "" {
		seen[typeName] = true
	}
}

// qualifyRangeVar returns "schema.name" from a RangeVar, using defaultSchema
// when the RangeVar has no schema.
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

// typeNameFromNames builds "schema.name" from a list of name nodes.
func typeNameFromNames(names []*pg_query.Node, defaultSchema string) string {
	var parts []string
	for _, n := range names {
		if s := n.GetString_(); s != nil {
			parts = append(parts, s.Sval)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return defaultSchema + "." + parts[0]
	}
	return strings.Join(parts, ".")
}

// resolveTypeName extracts a user-defined type name from a TypeName node.
// Returns "" for built-in types (pg_catalog.*).
func resolveTypeName(tn *pg_query.TypeName, defaultSchema string) string {
	if tn == nil {
		return ""
	}
	var parts []string
	for _, n := range tn.Names {
		if s := n.GetString_(); s != nil {
			parts = append(parts, s.Sval)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	// Built-in types are in pg_catalog
	if parts[0] == "pg_catalog" {
		return ""
	}
	if len(parts) == 1 {
		return defaultSchema + "." + parts[0]
	}
	return strings.Join(parts, ".")
}
