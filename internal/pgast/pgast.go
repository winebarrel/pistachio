// Package pgast provides shared pg_query AST helpers used by both the
// rename rewriter (diff package) and the column-reference validator
// (parser package).
package pgast

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// constraintWrapPrefix is the wrapper used to parse a constraint definition
// fragment (e.g. "PRIMARY KEY (id)") as part of a full statement so that
// pg_query produces a Constraint node we can inspect or mutate.
const constraintWrapPrefix = "ALTER TABLE _t ADD CONSTRAINT _c "

// ParseConstraintDef parses a constraint definition fragment and returns
// the underlying *pg_query.Constraint. Returns nil for unparseable or
// unexpected input so callers can degrade gracefully.
func ParseConstraintDef(def string) *pg_query.Constraint {
	_, con, err := ParseConstraintDefStrict(def)
	if err != nil {
		return nil
	}
	return con
}

// ParseConstraintDefStrict is like ParseConstraintDef but also returns the
// full ParseResult and a typed error, so callers that need to mutate the
// AST and deparse it back can distinguish failure modes.
func ParseConstraintDefStrict(def string) (*pg_query.ParseResult, *pg_query.Constraint, error) {
	result, err := pg_query.Parse(constraintWrapPrefix + def)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse constraint definition: %w", err)
	}
	if len(result.Stmts) == 0 {
		return nil, nil, fmt.Errorf("empty constraint definition")
	}
	as := result.Stmts[0].Stmt.GetAlterTableStmt()
	if as == nil || len(as.Cmds) == 0 {
		return nil, nil, fmt.Errorf("unexpected parse result for constraint definition")
	}
	cmd := as.Cmds[0].GetAlterTableCmd()
	if cmd == nil || cmd.Def == nil {
		return nil, nil, fmt.Errorf("unexpected parse result for constraint definition")
	}
	con := cmd.Def.GetConstraint()
	if con == nil {
		return nil, nil, fmt.Errorf("unexpected parse result for constraint definition")
	}
	return result, con, nil
}

// DeparseConstraintDef deparses a ParseResult that came from
// ParseConstraintDefStrict and strips the wrapper so the caller receives
// just the constraint fragment.
func DeparseConstraintDef(result *pg_query.ParseResult) (string, error) {
	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse constraint definition: %w", err)
	}
	deparsed = strings.TrimSuffix(deparsed, ";")
	if !strings.HasPrefix(deparsed, constraintWrapPrefix) {
		return "", fmt.Errorf("unexpected deparsed form: %s", deparsed)
	}
	return strings.TrimPrefix(deparsed, constraintWrapPrefix), nil
}

// WalkExprColumnRefs walks an expression tree and invokes visit for each
// unqualified single-field ColumnRef's underlying String node. Visitors may
// mutate the String's Sval to rename the column. Qualified references
// (table.col, schema.table.col) are skipped because they refer outside the
// local scope.
func WalkExprColumnRefs(node *pg_query.Node, visit func(*pg_query.String)) {
	if node == nil {
		return
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_ColumnRef:
		if len(n.ColumnRef.Fields) == 1 {
			if s := n.ColumnRef.Fields[0].GetString_(); s != nil {
				visit(s)
			}
		}
	case *pg_query.Node_AExpr:
		WalkExprColumnRefs(n.AExpr.Lexpr, visit)
		WalkExprColumnRefs(n.AExpr.Rexpr, visit)
	case *pg_query.Node_BoolExpr:
		for _, arg := range n.BoolExpr.Args {
			WalkExprColumnRefs(arg, visit)
		}
	case *pg_query.Node_TypeCast:
		WalkExprColumnRefs(n.TypeCast.Arg, visit)
	case *pg_query.Node_FuncCall:
		for _, arg := range n.FuncCall.Args {
			WalkExprColumnRefs(arg, visit)
		}
	case *pg_query.Node_NullTest:
		WalkExprColumnRefs(n.NullTest.Arg, visit)
	case *pg_query.Node_AArrayExpr:
		for _, elem := range n.AArrayExpr.Elements {
			WalkExprColumnRefs(elem, visit)
		}
	case *pg_query.Node_List:
		for _, item := range n.List.Items {
			WalkExprColumnRefs(item, visit)
		}
	case *pg_query.Node_CoalesceExpr:
		for _, arg := range n.CoalesceExpr.Args {
			WalkExprColumnRefs(arg, visit)
		}
	case *pg_query.Node_CaseExpr:
		WalkExprColumnRefs(n.CaseExpr.Arg, visit)
		WalkExprColumnRefs(n.CaseExpr.Defresult, visit)
		for _, when := range n.CaseExpr.Args {
			if w := when.GetCaseWhen(); w != nil {
				WalkExprColumnRefs(w.Expr, visit)
				WalkExprColumnRefs(w.Result, visit)
			}
		}
	case *pg_query.Node_AIndirection:
		// col[1], col[1:2], (col).field
		WalkExprColumnRefs(n.AIndirection.Arg, visit)
		for _, ind := range n.AIndirection.Indirection {
			if idx := ind.GetAIndices(); idx != nil {
				WalkExprColumnRefs(idx.Lidx, visit)
				WalkExprColumnRefs(idx.Uidx, visit)
			}
		}
	case *pg_query.Node_MinMaxExpr:
		// GREATEST / LEAST
		for _, arg := range n.MinMaxExpr.Args {
			WalkExprColumnRefs(arg, visit)
		}
	case *pg_query.Node_RowExpr:
		// ROW(col), (col1, col2)
		for _, arg := range n.RowExpr.Args {
			WalkExprColumnRefs(arg, visit)
		}
	case *pg_query.Node_BooleanTest:
		// col IS TRUE / IS NOT FALSE / IS UNKNOWN
		WalkExprColumnRefs(n.BooleanTest.Arg, visit)
	case *pg_query.Node_CollateClause:
		// col COLLATE "C"
		WalkExprColumnRefs(n.CollateClause.Arg, visit)
	case *pg_query.Node_NamedArgExpr:
		// f(x => col). The argument name is a plain string, not a node.
		WalkExprColumnRefs(n.NamedArgExpr.Arg, visit)
	case *pg_query.Node_XmlExpr:
		// xmlelement(name e, col), xmlconcat(col, ...). ArgNames holds the
		// attribute names rather than columns, so it stays out.
		for _, arg := range n.XmlExpr.NamedArgs {
			// xmlattributes(col AS x) and xmlforest(col AS x) wrap each
			// element in a ResTarget whose Name is the alias, not a column.
			if rt := arg.GetResTarget(); rt != nil {
				arg = rt.Val
			}
			WalkExprColumnRefs(arg, visit)
		}
		for _, arg := range n.XmlExpr.Args {
			WalkExprColumnRefs(arg, visit)
		}
	case *pg_query.Node_XmlSerialize:
		// xmlserialize(content col AS text) is its own node kind rather than
		// an XmlExpr. TypeName holds a type, so only Expr is walked.
		WalkExprColumnRefs(n.XmlSerialize.Expr, visit)
	}
}
