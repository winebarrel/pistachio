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
// local scope, and so is a sub-link's contained SELECT.
//
// Every other node kind is reached, because Walk enumerates children through
// protoreflect rather than from a list. That matters for the three callers:
// the constraint namer copies the column into the name PostgreSQL would
// generate, the desired-schema validator reports a column that does not
// exist, and the column renamer rewrites the reference in place. A node kind
// missing from the walk breaks all three at once.
//
// The names written where a column could go stay out on their own: an
// xmlelement name, an xmlattributes alias, a named-argument label, a function
// name, an A_Indirection field and a type name are all String nodes outside
// any ColumnRef.
func WalkExprColumnRefs(node *pg_query.Node, visit func(*pg_query.String)) {
	Walk(node, WalkOptions{SkipSubqueries: true}, func(_ Ctx, n *pg_query.Node) *pg_query.Node {
		if cr := n.GetColumnRef(); cr != nil && len(cr.Fields) == 1 {
			if s := cr.Fields[0].GetString_(); s != nil {
				visit(s)
			}
		}
		return n
	})
}
