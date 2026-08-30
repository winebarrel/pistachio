package diff

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// The folds here undo the rewrites the PostgreSQL grammar and parse analysis
// perform on the way into the catalog. pg_get_constraintdef, pg_get_expr,
// pg_get_indexdef, pg_get_viewdef and pg_get_triggerdef print the stored form,
// so a definition written with the keyword spelling never matches what comes
// back and re-creates its object on every plan. A generated column fails the
// run instead, since its expression cannot be altered in place.
//
// Each fold moves the written side onto the stored form, the direction
// PostgreSQL itself defines. None changes what the expression means, and none
// reaches the DDL: the diff emits the node it parsed.
//
// A typed literal is re-printed in its type's output form and still drifts.
// TODO.md covers it.

// desugarBetween expands BETWEEN into the comparisons parse analysis defines it
// as, and returns nil for anything else. The four shapes follow
// transformAExprBetween (src/backend/parser/parse_expr.c):
//
//	x BETWEEN a AND b               -> (x >= a) AND (x <= b)
//	x NOT BETWEEN a AND b           -> (x < a) OR (x > b)
//	x BETWEEN SYMMETRIC a AND b     -> ((x >= a) AND (x <= b)) OR ((x >= b) AND (x <= a))
//	x NOT BETWEEN SYMMETRIC a AND b -> ((x < a) OR (x > b)) AND ((x < b) OR (x > a))
//
// The operand appears in each comparison, as a clone: a later walk must not
// reach one copy through another.
func desugarBetween(node *pg_query.Node) *pg_query.Node {
	ae := node.GetAExpr()
	if ae == nil {
		return nil
	}

	var negated, symmetric bool
	switch ae.Kind {
	case pg_query.A_Expr_Kind_AEXPR_BETWEEN:
	case pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN:
		negated = true
	case pg_query.A_Expr_Kind_AEXPR_BETWEEN_SYM:
		symmetric = true
	case pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN_SYM:
		negated, symmetric = true, true
	default:
		return nil
	}

	list := ae.Rexpr.GetList()
	if ae.Lexpr == nil || list == nil || len(list.Items) != 2 {
		return nil
	}
	x, lo, hi := ae.Lexpr, list.Items[0], list.Items[1]

	// The negated form is the plain one with both operators flipped and the
	// two levels of AND/OR swapped, so one pair of names covers both.
	loOp, hiOp := ">=", "<="
	inner, outer := pg_query.BoolExprType_AND_EXPR, pg_query.BoolExprType_OR_EXPR
	if negated {
		loOp, hiOp = "<", ">"
		inner, outer = outer, inner
	}

	bounded := makeBoolExpr(inner,
		makeCmpExpr(loOp, cloneNode(x), lo),
		makeCmpExpr(hiOp, cloneNode(x), hi),
	)
	if !symmetric {
		return bounded
	}

	return makeBoolExpr(outer, bounded, makeBoolExpr(inner,
		makeCmpExpr(loOp, cloneNode(x), cloneNode(hi)),
		makeCmpExpr(hiOp, cloneNode(x), cloneNode(lo)),
	))
}

// desugarPatternMatch drops the keyword spelling from LIKE, ILIKE and SIMILAR
// TO. The grammar already puts the operator in the node's name (`~~`, `~~*`,
// `!~~`, `!~~*`, `~`, `!~`) and an ESCAPE clause in the right operand as a
// like_escape() or similar_to_escape() call, so all that is left is the kind
// tag that makes the deparser print the keyword. The catalog prints the
// operator, which parses as a plain one.
//
// The grammar writes the escape call pg_catalog-qualified and the catalog
// prints it bare. stripFuncSchema settles that, and the walk has already
// reached the call by the time it reaches this node.
func desugarPatternMatch(node *pg_query.Node) {
	ae := node.GetAExpr()
	if ae == nil {
		return
	}
	switch ae.Kind {
	case pg_query.A_Expr_Kind_AEXPR_LIKE,
		pg_query.A_Expr_Kind_AEXPR_ILIKE,
		pg_query.A_Expr_Kind_AEXPR_SIMILAR:
		ae.Kind = pg_query.A_Expr_Kind_AEXPR_OP
	}
}

// normalizeRowFormat settles `(a, b)` against `ROW(a, b)`. Both build the same
// row constructor and differ only in the field ruleutils reads to pick a
// spelling. Both sides take the explicit form, which the catalog prints and
// which a single-element row needs to stay a row.
func normalizeRowFormat(node *pg_query.Node) {
	if re := node.GetRowExpr(); re != nil {
		re.RowFormat = pg_query.CoercionForm_COERCE_EXPLICIT_CALL
	}
}

// foldArrayComparison turns the scalar-array comparison an IN list is stored as
// back into the list: `= ANY (ARRAY[...])` for IN and `<> ALL (ARRAY[...])` for
// NOT IN, the two shapes transformAExprIn produces.
//
// The operator is part of the match. Every other one reaches the catalog as
// written, and folding it would make `x > ANY (...)` and `x > ALL (...)`
// compare equal.
func foldArrayComparison(node *pg_query.Node) {
	ae := node.GetAExpr()
	if ae == nil {
		return
	}
	var op string
	switch ae.Kind {
	case pg_query.A_Expr_Kind_AEXPR_OP_ANY:
		op = "="
	case pg_query.A_Expr_Kind_AEXPR_OP_ALL:
		op = "<>"
	default:
		return
	}
	if operatorName(ae.Name) != op {
		return
	}
	arr := ae.Rexpr.GetAArrayExpr()
	if arr == nil {
		return
	}
	ae.Kind = pg_query.A_Expr_Kind_AEXPR_IN
	ae.Rexpr = &pg_query.Node{
		Node: &pg_query.Node_List{List: &pg_query.List{Items: arr.Elements}},
	}
}

// flattenBoolExpr merges an AND argument of an AND, and an OR argument of an
// OR, into the argument list holding it.
//
// makeAndExpr and makeOrExpr (src/backend/parser/gram.y) do this while parsing,
// but only for a left operand that is a BoolExpr by then.
// `(x BETWEEN 1 AND 10) AND y` is not one, so expanding the BETWEEN afterwards
// leaves nesting the grammar would have removed, while re-parsing the catalog's
// own output flattens it. The two sides would deparse differently over the same
// expression.
//
// AND and OR are associative, so the flat list means what the nesting meant.
// The walk reaches an argument before the node holding it, so one pass
// collapses a chain of any depth.
func flattenBoolExpr(node *pg_query.Node) {
	be := node.GetBoolExpr()
	if be == nil || be.Boolop == pg_query.BoolExprType_NOT_EXPR {
		return
	}
	args := make([]*pg_query.Node, 0, len(be.Args))
	for _, arg := range be.Args {
		if nested := arg.GetBoolExpr(); nested != nil && nested.Boolop == be.Boolop {
			args = append(args, nested.Args...)
			continue
		}
		args = append(args, arg)
	}
	be.Args = args
}

// operatorName returns a bare operator name, and "" for a qualified one such as
// OPERATOR(pg_catalog.=), which the folds above have no rule for.
func operatorName(name []*pg_query.Node) string {
	if len(name) != 1 {
		return ""
	}
	return name[0].GetString_().GetSval()
}

func makeCmpExpr(op string, lexpr, rexpr *pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_AExpr{AExpr: &pg_query.A_Expr{
		Kind:  pg_query.A_Expr_Kind_AEXPR_OP,
		Name:  []*pg_query.Node{pg_query.MakeStrNode(op)},
		Lexpr: lexpr,
		Rexpr: rexpr,
	}}}
}

func makeBoolExpr(op pg_query.BoolExprType, args ...*pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: &pg_query.BoolExpr{
		Boolop: op,
		Args:   args,
	}}}
}

func cloneNode(node *pg_query.Node) *pg_query.Node {
	return proto.Clone(node).(*pg_query.Node)
}
