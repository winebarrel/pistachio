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
// dependency information for each object.
func ExtractDeps(sql string) ([]*StmtInfo, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	// First pass: collect all defined object names so we only track internal deps
	defined := make(map[string]bool)
	for _, rawStmt := range result.Stmts {
		if name := objectName(rawStmt); name != "" {
			defined[name] = true
		}
	}

	var stmts []*StmtInfo
	for _, rawStmt := range result.Stmts {
		name := objectName(rawStmt)
		if name == "" {
			continue
		}

		deparsed, err := pg_query.Deparse(&pg_query.ParseResult{
			Stmts: []*pg_query.RawStmt{rawStmt},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to deparse statement for %s: %w", name, err)
		}

		deps := extractStmtDeps(rawStmt, defined)

		stmts = append(stmts, &StmtInfo{
			Name: name,
			SQL:  deparsed,
			Deps: deps,
		})
	}

	return stmts, nil
}

// objectName extracts the fully qualified name from a CREATE statement.
func objectName(rawStmt *pg_query.RawStmt) string {
	node := rawStmt.Stmt
	if node == nil {
		return ""
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_CreateStmt:
		return rangeVarName(n.CreateStmt.Relation)
	case *pg_query.Node_ViewStmt:
		return rangeVarName(n.ViewStmt.View)
	case *pg_query.Node_CreateEnumStmt:
		return typeNameFromNames(n.CreateEnumStmt.TypeName)
	case *pg_query.Node_CreateDomainStmt:
		return typeNameFromNames(n.CreateDomainStmt.Domainname)
	}

	return ""
}

// extractStmtDeps extracts dependency names from a statement, filtered to only
// include names that are defined in the same SQL input.
func extractStmtDeps(rawStmt *pg_query.RawStmt, defined map[string]bool) []string {
	node := rawStmt.Stmt
	if node == nil {
		return nil
	}

	seen := make(map[string]bool)
	selfName := objectName(rawStmt)

	switch n := node.Node.(type) {
	case *pg_query.Node_CreateStmt:
		extractCreateStmtDeps(n.CreateStmt, seen)
	case *pg_query.Node_ViewStmt:
		extractSelectDeps(n.ViewStmt.Query, seen)
	case *pg_query.Node_CreateDomainStmt:
		extractDomainDeps(n.CreateDomainStmt, seen)
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
func extractCreateStmtDeps(cs *pg_query.CreateStmt, seen map[string]bool) {
	// Column type references
	for _, elt := range cs.TableElts {
		if cd := elt.GetColumnDef(); cd != nil {
			if typeName := resolveTypeName(cd.TypeName); typeName != "" {
				seen[typeName] = true
			}
			// Column-level constraints (inline FK)
			for _, con := range cd.Constraints {
				if c := con.GetConstraint(); c != nil {
					if c.Contype == pg_query.ConstrType_CONSTR_FOREIGN && c.Pktable != nil {
						seen[rangeVarName(c.Pktable)] = true
					}
				}
			}
		}
		// Table-level constraints
		if con := elt.GetConstraint(); con != nil {
			if con.Contype == pg_query.ConstrType_CONSTR_FOREIGN && con.Pktable != nil {
				seen[rangeVarName(con.Pktable)] = true
			}
		}
	}

	// Partition parent
	for _, inh := range cs.InhRelations {
		if rv := inh.GetRangeVar(); rv != nil {
			seen[rangeVarName(rv)] = true
		}
	}
}

// extractSelectDeps recursively extracts table references from a SELECT statement.
func extractSelectDeps(node *pg_query.Node, seen map[string]bool) {
	if node == nil {
		return
	}

	ss := node.GetSelectStmt()
	if ss == nil {
		return
	}

	// FROM clause
	for _, from := range ss.FromClause {
		extractFromDeps(from, seen)
	}

	// Set operations (UNION, INTERSECT, EXCEPT)
	if ss.Larg != nil {
		extractSelectDeps(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Larg}}, seen)
	}
	if ss.Rarg != nil {
		extractSelectDeps(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: ss.Rarg}}, seen)
	}
}

// extractFromDeps extracts table references from FROM clause items.
func extractFromDeps(node *pg_query.Node, seen map[string]bool) {
	if node == nil {
		return
	}

	if rv := node.GetRangeVar(); rv != nil {
		seen[rangeVarName(rv)] = true
		return
	}

	if join := node.GetJoinExpr(); join != nil {
		extractFromDeps(join.Larg, seen)
		extractFromDeps(join.Rarg, seen)
		return
	}

	if sub := node.GetRangeSubselect(); sub != nil {
		extractSelectDeps(sub.Subquery, seen)
		return
	}
}

// extractDomainDeps extracts dependencies from a CREATE DOMAIN statement.
func extractDomainDeps(ds *pg_query.CreateDomainStmt, seen map[string]bool) {
	if typeName := resolveTypeName(ds.TypeName); typeName != "" {
		seen[typeName] = true
	}
}

// rangeVarName returns "schema.name" from a RangeVar, defaulting schema to "public".
func rangeVarName(rv *pg_query.RangeVar) string {
	if rv == nil {
		return ""
	}
	schema := rv.Schemaname
	if schema == "" {
		schema = "public"
	}
	return schema + "." + rv.Relname
}

// typeNameFromNames builds "schema.name" from a list of name nodes.
func typeNameFromNames(names []*pg_query.Node) string {
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
		return "public." + parts[0]
	}
	return strings.Join(parts, ".")
}

// resolveTypeName extracts a user-defined type name from a TypeName node.
// Returns "" for built-in types (pg_catalog.*).
func resolveTypeName(tn *pg_query.TypeName) string {
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
		return "public." + parts[0]
	}
	return strings.Join(parts, ".")
}
