package parser

import (
	"errors"
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

// ValidateColumnRefs returns an error if any index, constraint, or foreign-key
// definition on a desired table references a column that does not exist in
// that table's desired column set. All violations across all tables are
// aggregated via errors.Join so a single plan run reports every problem.
//
// Scope: only same-table references are checked. Foreign-key referenced
// columns (PkAttrs, on the parent table) are out of scope. Partition
// children that inherit columns from the parent are skipped.
func ValidateColumnRefs(tables *orderedmap.Map[string, *model.Table]) error {
	var errs []error
	for fqtn, t := range tables.All() {
		if t.PartitionOf != nil && t.PartitionBound != nil {
			continue
		}

		cols := make(map[string]bool, t.Columns.Len())
		for name := range t.Columns.Keys() {
			cols[name] = true
		}

		for _, idx := range t.Indexes.CollectValues() {
			for _, ref := range collectColumnRefsInIndexDef(idx.Definition) {
				if !cols[ref] {
					errs = append(errs, fmt.Errorf("column %s referenced in index %s does not exist on table %s",
						model.Ident(ref), model.Ident(idx.Name), fqtn))
				}
			}
		}

		for _, con := range t.Constraints.CollectValues() {
			kind := constraintKindLabel(con.Type)
			for _, ref := range collectColumnRefsInConstraintDef(con.Definition) {
				if !cols[ref] {
					errs = append(errs, fmt.Errorf("column %s referenced in %s %s does not exist on table %s",
						model.Ident(ref), kind, model.Ident(con.Name), fqtn))
				}
			}
		}

		for _, fk := range t.ForeignKeys.CollectValues() {
			for _, ref := range collectColumnRefsInFKDef(fk.Definition) {
				if !cols[ref] {
					errs = append(errs, fmt.Errorf("column %s referenced in foreign key %s does not exist on table %s",
						model.Ident(ref), model.Ident(fk.Name), fqtn))
				}
			}
		}
	}
	return errors.Join(errs...)
}

func constraintKindLabel(ct model.ConstraintType) string {
	switch {
	case ct.IsCheckConstraint():
		return "CHECK constraint"
	case ct.IsPrimaryKeyConstraint():
		return "PRIMARY KEY constraint"
	case ct.IsUniqueConstraint():
		return "UNIQUE constraint"
	case ct.IsForeignKeyConstraint():
		return "FOREIGN KEY constraint"
	case ct.IsExclusionConstraint():
		return "EXCLUDE constraint"
	default:
		return "constraint"
	}
}

// collectColumnRefsInIndexDef returns the unqualified column names referenced
// by an index definition (IndexParams, IndexIncludingParams, WhereClause).
// Returns nil on parse errors so validation degrades to a no-op for
// unparsable definitions.
func collectColumnRefsInIndexDef(def string) []string {
	result, err := pg_query.Parse(def)
	if err != nil || len(result.Stmts) == 0 {
		return nil
	}
	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil {
		return nil
	}
	var refs []string
	collect := func(params []*pg_query.Node) {
		for _, p := range params {
			ie := p.GetIndexElem()
			if ie == nil {
				continue
			}
			if ie.Name != "" {
				refs = append(refs, ie.Name)
			}
			refs = append(refs, walkExprColumnRefs(ie.Expr)...)
		}
	}
	collect(is.IndexParams)
	collect(is.IndexIncludingParams)
	refs = append(refs, walkExprColumnRefs(is.WhereClause)...)
	return refs
}

// collectColumnRefsInConstraintDef returns the unqualified column names
// referenced by a constraint definition fragment (Keys, Including, RawExpr,
// EXCLUDE Exclusions IndexElem).
func collectColumnRefsInConstraintDef(def string) []string {
	con := pgast.ParseConstraintDef(def)
	if con == nil {
		return nil
	}
	var refs []string
	collectStringList := func(nodes []*pg_query.Node) {
		for _, n := range nodes {
			if s := n.GetString_(); s != nil && s.Sval != "" {
				refs = append(refs, s.Sval)
			}
		}
	}
	collectStringList(con.Keys)
	collectStringList(con.Including)
	refs = append(refs, walkExprColumnRefs(con.RawExpr)...)
	for _, ex := range con.Exclusions {
		list := ex.GetList()
		if list == nil {
			continue
		}
		for _, item := range list.Items {
			ie := item.GetIndexElem()
			if ie == nil {
				continue
			}
			if ie.Name != "" {
				refs = append(refs, ie.Name)
			}
			refs = append(refs, walkExprColumnRefs(ie.Expr)...)
		}
	}
	return refs
}

// collectColumnRefsInFKDef returns the local-side column names (FkAttrs)
// referenced by a foreign-key definition. PkAttrs (parent-table columns) are
// intentionally excluded; cross-table validation is out of scope.
func collectColumnRefsInFKDef(def string) []string {
	con := pgast.ParseConstraintDef(def)
	if con == nil {
		return nil
	}
	var refs []string
	for _, n := range con.FkAttrs {
		if s := n.GetString_(); s != nil && s.Sval != "" {
			refs = append(refs, s.Sval)
		}
	}
	return refs
}

// walkExprColumnRefs returns the unqualified ColumnRef names found in an
// expression tree.
func walkExprColumnRefs(node *pg_query.Node) []string {
	var refs []string
	pgast.WalkExprColumnRefs(node, func(s *pg_query.String) {
		if s.Sval != "" {
			refs = append(refs, s.Sval)
		}
	})
	return refs
}
