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
	WalkExprColumnRefNodes(node, func(cr *pg_query.ColumnRef) {
		if len(cr.Fields) != 1 {
			return
		}
		if s := cr.Fields[0].GetString_(); s != nil {
			visit(s)
		}
	}, nil)
}

// WalkExprColumnRefNodes walks an expression tree and invokes visit for every
// ColumnRef it holds, qualified or not. Visitors may mutate what they are
// given. A nested SELECT is reached only through visitSubLink, which receives
// the SubLink and decides what to do with it; pass nil to leave sub-queries
// alone.
func WalkExprColumnRefNodes(node *pg_query.Node, visit func(*pg_query.ColumnRef), visitSubLink func(*pg_query.SubLink)) {
	if node == nil {
		return
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_ColumnRef:
		visit(n.ColumnRef)
	case *pg_query.Node_AExpr:
		WalkExprColumnRefNodes(n.AExpr.Lexpr, visit, visitSubLink)
		WalkExprColumnRefNodes(n.AExpr.Rexpr, visit, visitSubLink)
	case *pg_query.Node_BoolExpr:
		for _, arg := range n.BoolExpr.Args {
			WalkExprColumnRefNodes(arg, visit, visitSubLink)
		}
	case *pg_query.Node_TypeCast:
		WalkExprColumnRefNodes(n.TypeCast.Arg, visit, visitSubLink)
	case *pg_query.Node_FuncCall:
		for _, arg := range n.FuncCall.Args {
			WalkExprColumnRefNodes(arg, visit, visitSubLink)
		}
		// An aggregate holds its ORDER BY and FILTER outside the argument
		// list, and a window function its OVER clause. Funcname is the
		// function's, not a column's.
		for _, ob := range n.FuncCall.AggOrder {
			WalkExprColumnRefNodes(ob, visit, visitSubLink)
		}
		WalkExprColumnRefNodes(n.FuncCall.AggFilter, visit, visitSubLink)
		WalkWindowDefColumnRefNodes(n.FuncCall.Over, visit, visitSubLink)
	case *pg_query.Node_NullTest:
		WalkExprColumnRefNodes(n.NullTest.Arg, visit, visitSubLink)
	case *pg_query.Node_AArrayExpr:
		for _, elem := range n.AArrayExpr.Elements {
			WalkExprColumnRefNodes(elem, visit, visitSubLink)
		}
	case *pg_query.Node_List:
		for _, item := range n.List.Items {
			WalkExprColumnRefNodes(item, visit, visitSubLink)
		}
	case *pg_query.Node_CoalesceExpr:
		for _, arg := range n.CoalesceExpr.Args {
			WalkExprColumnRefNodes(arg, visit, visitSubLink)
		}
	case *pg_query.Node_CaseExpr:
		WalkExprColumnRefNodes(n.CaseExpr.Arg, visit, visitSubLink)
		WalkExprColumnRefNodes(n.CaseExpr.Defresult, visit, visitSubLink)
		for _, when := range n.CaseExpr.Args {
			if w := when.GetCaseWhen(); w != nil {
				WalkExprColumnRefNodes(w.Expr, visit, visitSubLink)
				WalkExprColumnRefNodes(w.Result, visit, visitSubLink)
			}
		}
	case *pg_query.Node_AIndirection:
		// col[1], col[1:2], (col).field
		WalkExprColumnRefNodes(n.AIndirection.Arg, visit, visitSubLink)
		for _, ind := range n.AIndirection.Indirection {
			if idx := ind.GetAIndices(); idx != nil {
				WalkExprColumnRefNodes(idx.Lidx, visit, visitSubLink)
				WalkExprColumnRefNodes(idx.Uidx, visit, visitSubLink)
			}
		}
	case *pg_query.Node_MinMaxExpr:
		// GREATEST / LEAST
		for _, arg := range n.MinMaxExpr.Args {
			WalkExprColumnRefNodes(arg, visit, visitSubLink)
		}
	case *pg_query.Node_RowExpr:
		// ROW(col), (col1, col2)
		for _, arg := range n.RowExpr.Args {
			WalkExprColumnRefNodes(arg, visit, visitSubLink)
		}
	case *pg_query.Node_BooleanTest:
		// col IS TRUE / IS NOT FALSE / IS UNKNOWN
		WalkExprColumnRefNodes(n.BooleanTest.Arg, visit, visitSubLink)
	case *pg_query.Node_CollateClause:
		// col COLLATE "C"
		WalkExprColumnRefNodes(n.CollateClause.Arg, visit, visitSubLink)
	case *pg_query.Node_NamedArgExpr:
		// f(x => col). The argument name is a plain string, not a node.
		WalkExprColumnRefNodes(n.NamedArgExpr.Arg, visit, visitSubLink)
	case *pg_query.Node_XmlExpr:
		// xmlelement(name e, col), xmlconcat(col, ...). ArgNames holds the
		// attribute names rather than columns, so it stays out.
		for _, arg := range n.XmlExpr.NamedArgs {
			// xmlattributes(col AS x) and xmlforest(col AS x) wrap each
			// element in a ResTarget whose Name is the alias, not a column.
			if rt := arg.GetResTarget(); rt != nil {
				arg = rt.Val
			}
			WalkExprColumnRefNodes(arg, visit, visitSubLink)
		}
		for _, arg := range n.XmlExpr.Args {
			WalkExprColumnRefNodes(arg, visit, visitSubLink)
		}
	case *pg_query.Node_XmlSerialize:
		// xmlserialize(content col AS text) is its own node kind rather than
		// an XmlExpr. TypeName holds a type, so only Expr is walked.
		WalkExprColumnRefNodes(n.XmlSerialize.Expr, visit, visitSubLink)
	case *pg_query.Node_JsonObjectConstructor:
		// JSON_OBJECT('k': col). Exprs holds JsonKeyValue nodes.
		for _, e := range n.JsonObjectConstructor.Exprs {
			WalkExprColumnRefNodes(e, visit, visitSubLink)
		}
	case *pg_query.Node_JsonKeyValue:
		// A key is an expression as well, and PostgreSQL counts a column
		// written there, so JSON_OBJECT(a: b) reaches both columns.
		WalkExprColumnRefNodes(n.JsonKeyValue.Key, visit, visitSubLink)
		walkJsonValueExpr(n.JsonKeyValue.Value, visit, visitSubLink)
	case *pg_query.Node_JsonArrayConstructor:
		// JSON_ARRAY(col, ...). Exprs holds JsonValueExpr nodes.
		for _, e := range n.JsonArrayConstructor.Exprs {
			WalkExprColumnRefNodes(e, visit, visitSubLink)
		}
	case *pg_query.Node_JsonValueExpr:
		walkJsonValueExpr(n.JsonValueExpr, visit, visitSubLink)
	case *pg_query.Node_JsonIsPredicate:
		// col IS JSON
		WalkExprColumnRefNodes(n.JsonIsPredicate.Expr, visit, visitSubLink)
	case *pg_query.Node_JsonParseExpr:
		// JSON(col)
		walkJsonValueExpr(n.JsonParseExpr.Expr, visit, visitSubLink)
	case *pg_query.Node_JsonScalarExpr:
		// JSON_SCALAR(col)
		WalkExprColumnRefNodes(n.JsonScalarExpr.Expr, visit, visitSubLink)
	case *pg_query.Node_JsonSerializeExpr:
		// JSON_SERIALIZE(col)
		walkJsonValueExpr(n.JsonSerializeExpr.Expr, visit, visitSubLink)
	case *pg_query.Node_JsonFuncExpr:
		// JSON_EXISTS / JSON_VALUE / JSON_QUERY. ColumnName names a JSON_TABLE
		// output column rather than a table column, so it stays out.
		walkJsonValueExpr(n.JsonFuncExpr.ContextItem, visit, visitSubLink)
		WalkExprColumnRefNodes(n.JsonFuncExpr.Pathspec, visit, visitSubLink)
		for _, arg := range n.JsonFuncExpr.Passing {
			WalkExprColumnRefNodes(arg, visit, visitSubLink)
		}
		walkJsonBehavior(n.JsonFuncExpr.OnEmpty, visit, visitSubLink)
		walkJsonBehavior(n.JsonFuncExpr.OnError, visit, visitSubLink)
	case *pg_query.Node_JsonArgument:
		// PASSING col AS name. The name is the argument's, not a column's.
		walkJsonValueExpr(n.JsonArgument.Val, visit, visitSubLink)
	case *pg_query.Node_JsonObjectAgg:
		// JSON_OBJECTAGG(key: value)
		if kv := n.JsonObjectAgg.Arg; kv != nil {
			WalkExprColumnRefNodes(kv.Key, visit, visitSubLink)
			walkJsonValueExpr(kv.Value, visit, visitSubLink)
		}
		walkJsonAggConstructor(n.JsonObjectAgg.Constructor, visit, visitSubLink)
	case *pg_query.Node_JsonArrayAgg:
		// JSON_ARRAYAGG(col)
		walkJsonValueExpr(n.JsonArrayAgg.Arg, visit, visitSubLink)
		walkJsonAggConstructor(n.JsonArrayAgg.Constructor, visit, visitSubLink)
	case *pg_query.Node_SortBy:
		// An ORDER BY item, which an aggregate and a window function both
		// carry.
		WalkExprColumnRefNodes(n.SortBy.Node, visit, visitSubLink)
	case *pg_query.Node_SubLink:
		// EXISTS (SELECT ...), x IN (SELECT ...). The contained SELECT is out
		// of the local scope, so only a caller that asked for it gets here.
		if visitSubLink != nil {
			visitSubLink(n.SubLink)
		}
	}
	// Two SQL/JSON node kinds are left out. JSON_ARRAY over a subquery carries
	// a raw SELECT rather than a SubLink, and PostgreSQL stores the whole
	// construct rewritten as an aggregate over a derived table, so matching
	// the written form takes more than a walk. JSON_TABLE is a FROM item, not
	// an expression.
}

// WalkWindowDefColumnRefNodes walks the PARTITION BY and ORDER BY of an OVER
// clause, written inline or under a WINDOW name. The window's own name and the
// name it refines are not column references, and a frame offset cannot hold a
// variable.
func WalkWindowDefColumnRefNodes(w *pg_query.WindowDef, visit func(*pg_query.ColumnRef), visitSubLink func(*pg_query.SubLink)) {
	if w == nil {
		return
	}
	for _, pb := range w.PartitionClause {
		WalkExprColumnRefNodes(pb, visit, visitSubLink)
	}
	for _, ob := range w.OrderClause {
		WalkExprColumnRefNodes(ob, visit, visitSubLink)
	}
}

// walkJsonAggConstructor walks the ORDER BY, FILTER and OVER clauses that a
// SQL/JSON aggregate carries, the same ones a FuncCall holds.
func walkJsonAggConstructor(c *pg_query.JsonAggConstructor, visit func(*pg_query.ColumnRef), visitSubLink func(*pg_query.SubLink)) {
	if c == nil {
		return
	}
	for _, ob := range c.AggOrder {
		WalkExprColumnRefNodes(ob, visit, visitSubLink)
	}
	WalkExprColumnRefNodes(c.AggFilter, visit, visitSubLink)
	WalkWindowDefColumnRefNodes(c.Over, visit, visitSubLink)
}

// walkJsonValueExpr walks the expression a JsonValueExpr wraps. The node shows
// up both on its own and as a typed field of the SQL/JSON nodes. FormattedExpr
// is filled in by the analyzer, not by the raw parser these callers run, so
// only RawExpr is walked.
func walkJsonValueExpr(v *pg_query.JsonValueExpr, visit func(*pg_query.ColumnRef), visitSubLink func(*pg_query.SubLink)) {
	WalkExprColumnRefNodes(v.GetRawExpr(), visit, visitSubLink)
}

// walkJsonBehavior walks the expression of an ON EMPTY / ON ERROR clause,
// which only DEFAULT <expr> carries.
func walkJsonBehavior(b *pg_query.JsonBehavior, visit func(*pg_query.ColumnRef), visitSubLink func(*pg_query.SubLink)) {
	WalkExprColumnRefNodes(b.GetExpr(), visit, visitSubLink)
}
