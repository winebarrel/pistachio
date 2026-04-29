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
// Within a single object (index / constraint / FK), each missing column
// name is reported at most once so multiple references to the same name
// don't produce duplicate error lines.
//
// Scope: only same-table references are checked. Foreign-key referenced
// columns (PkAttrs, on the parent table) are out of scope. Tables that
// inherit columns from a parent (declarative partition children and
// INHERITS-style children) are skipped.
func ValidateColumnRefs(tables *orderedmap.Map[string, *model.Table]) error {
	var errs []error
	for fqtn, t := range tables.All() {
		// Skip both partition children (PartitionOf + PartitionBound set) and
		// INHERITS-style children (PartitionOf set, PartitionBound nil) — both
		// inherit their columns from the parent rather than declaring their
		// own complete column list.
		if t.PartitionOf != nil {
			continue
		}

		cols := make(map[string]bool, t.Columns.Len())
		for name := range t.Columns.Keys() {
			cols[name] = true
		}

		for _, idx := range t.Indexes.CollectValues() {
			reportMissing(&errs, cols, collectColumnRefsInIndexDef(idx.Definition), func(ref string) error {
				return fmt.Errorf("column %s referenced in index %s does not exist on table %s",
					model.Ident(ref), model.Ident(idx.Name), fqtn)
			})
		}

		for _, con := range t.Constraints.CollectValues() {
			kind := constraintKindLabel(con.Type)
			reportMissing(&errs, cols, collectColumnRefsInConstraintDef(con.Definition), func(ref string) error {
				return fmt.Errorf("column %s referenced in %s %s does not exist on table %s",
					model.Ident(ref), kind, model.Ident(con.Name), fqtn)
			})
		}

		for _, fk := range t.ForeignKeys.CollectValues() {
			reportMissing(&errs, cols, collectColumnRefsInFKDef(fk.Definition), func(ref string) error {
				return fmt.Errorf("column %s referenced in foreign key %s does not exist on table %s",
					model.Ident(ref), model.Ident(fk.Name), fqtn)
			})
		}
	}
	return errors.Join(errs...)
}

// reportMissing appends one error per distinct refs[i] that is not in cols,
// preserving first-encounter order so the aggregated error is deterministic.
func reportMissing(errs *[]error, cols map[string]bool, refs []string, mkErr func(string) error) {
	seen := map[string]bool{}
	for _, ref := range refs {
		if cols[ref] || seen[ref] {
			continue
		}
		seen[ref] = true
		*errs = append(*errs, mkErr(ref))
	}
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
