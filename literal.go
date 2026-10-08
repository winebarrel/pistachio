package pistachio

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

// --evaluate-literals compares a typed literal by its value rather than by its
// spelling. PostgreSQL stores the value of a literal, not the text it was
// written as, and the catalog prints it back in the type's output format:
// DEFAULT '1 hour' on an interval column reads back as '01:00:00'::interval.
// The diff compares text, so a desired schema that spells the literal some
// other way plans the same statement on every run.
//
// The server is the only thing that knows each type's output format, so the
// literals that differ are sent to it to print. Where the printed form of the
// desired literal matches the current one, the current side is rewritten to
// the desired spelling before the diff runs, the way referenceRenames carries
// a rename into it. The desired side, which the statements are written from,
// is never touched.
//
// The scope is a column or domain DEFAULT and a table or domain CHECK
// constraint. A column or domain whose type changes in the same run is left
// alone, since the current side's cast names the old type.

// literalPrinter returns the text the server prints for each cast expression.
// An expression it cannot evaluate is left out of the result.
type literalPrinter func(exprs []string) map[string]string

// printLiterals asks the server for the output form of each expression in one
// round trip. format's %s goes through the type's output function, which is
// what the catalog prints. A cast to text need not: inet's keeps the netmask.
// A literal the type refuses fails the whole query, so on an error each is
// asked on its own and the failures are dropped. apply reports them when it
// runs the statement.
func printLiterals(ctx context.Context, conn *pgx.Conn, exprs []string) map[string]string {
	cols := make([]string, len(exprs))
	for i, e := range exprs {
		cols[i] = "format('%s', " + e + ")"
	}
	var printed []string
	err := conn.QueryRow(ctx, "SELECT ARRAY["+strings.Join(cols, ", ")+"]").Scan(&printed)
	if err == nil {
		out := make(map[string]string, len(exprs))
		for i, e := range exprs {
			out[e] = printed[i]
		}
		return out
	}
	out := map[string]string{}
	for _, e := range exprs {
		var s string
		if conn.QueryRow(ctx, "SELECT format('%s', "+e+")").Scan(&s) == nil {
			out[e] = s
		}
	}
	return out
}

// literalPair is one current-side literal the desired side spells
// differently: the string the desired side wrote, and the cast expression
// that applies the current side's type to it.
type literalPair struct {
	desired string
	expr    string
}

// castLiteral returns the string constant under a cast and the cast, or nil
// when node is not a cast on a string constant.
func castLiteral(node *pg_query.Node) (*pg_query.String, *pg_query.TypeCast) {
	tc := node.GetTypeCast()
	if tc == nil {
		return nil, nil
	}
	s := tc.Arg.GetAConst().GetSval()
	if s == nil {
		return nil, nil
	}
	return s, tc
}

// castExpr writes the SQL for value cast to tn.
func castExpr(value string, tn *pg_query.TypeName) (string, error) {
	cast := &pg_query.Node{Node: &pg_query.Node_TypeCast{TypeCast: &pg_query.TypeCast{
		Arg:      pg_query.MakeAConstStrNode(value, 0),
		TypeName: tn,
	}}}
	sql, err := pg_query.Deparse(&pg_query.ParseResult{
		Version: pgast.ParseTreeVersion,
		Stmts: []*pg_query.RawStmt{{Stmt: &pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: &pg_query.SelectStmt{
			TargetList: []*pg_query.Node{pg_query.MakeResTargetNodeWithVal(cast, 0)},
		}}}}},
	})
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(sql, "SELECT "), nil
}

// matchLiterals walks desired and current in step and calls visit for each
// position where the current side holds a cast string constant and the
// desired side the same constant spelled differently, bare or cast to the
// same type.
func matchLiterals(desired, current *pg_query.Node, visit func(cur *pg_query.String, pair literalPair)) {
	pgast.WalkPair(desired, current, func(_ pgast.Ctx, des, cur *pg_query.Node) *pg_query.Node {
		curStr, curCast := castLiteral(cur)
		if curStr == nil {
			return cur
		}
		desStr := des.GetAConst().GetSval()
		desStr2, desCast := castLiteral(des)
		if desStr == nil {
			desStr = desStr2
		}
		if desStr == nil || desStr.Sval == curStr.Sval {
			return cur
		}
		expr, err := castExpr(desStr.Sval, curCast.TypeName)
		if err != nil {
			return cur
		}
		if desCast != nil {
			if desExpr, err := castExpr(desStr.Sval, desCast.TypeName); err != nil || desExpr != expr {
				return cur
			}
		}
		visit(curStr, literalPair{desired: desStr.Sval, expr: expr})
		return cur
	})
}

// literalAligner collects the literals to print on its first pass and
// rewrites the current side with the printed forms on its second.
type literalAligner struct {
	printed map[string]string
	exprs   []string
	seen    map[string]bool
}

// align compares one current-side expression with its desired counterpart.
// Before the literals are printed it records what to ask; after, it returns
// the current expression with each literal whose printed form matches spelled
// the desired way, and whether anything changed.
func (a *literalAligner) align(current, desired string, parse func(string) (*pg_query.ParseResult, *pg_query.Node, error), deparse func(*pg_query.ParseResult) (string, error)) (string, bool) {
	if current == desired {
		return current, false
	}
	curResult, curNode, err := parse(current)
	if err != nil {
		return current, false
	}
	_, desNode, err := parse(desired)
	if err != nil {
		return current, false
	}
	changed := false
	matchLiterals(desNode, curNode, func(cur *pg_query.String, pair literalPair) {
		if a.printed == nil {
			if !a.seen[pair.expr] {
				a.seen[pair.expr] = true
				a.exprs = append(a.exprs, pair.expr)
			}
			return
		}
		if p, ok := a.printed[pair.expr]; ok && p == cur.Sval {
			cur.Sval = pair.desired
			changed = true
		}
	})
	if !changed {
		return current, false
	}
	out, err := deparse(curResult)
	if err != nil {
		return current, false
	}
	return out, true
}

func parseDefault(expr string) (*pg_query.ParseResult, *pg_query.Node, error) {
	result, target, err := pgast.ParseExpr(expr)
	if err != nil {
		return nil, nil, err
	}
	return result, target.Val, nil
}

func deparseDefault(result *pg_query.ParseResult) (string, error) {
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(sql, "SELECT "), nil
}

func parseCheck(def string) (*pg_query.ParseResult, *pg_query.Node, error) {
	result, con, err := pgast.ParseConstraintDefStrict(def)
	if err != nil {
		return nil, nil, err
	}
	if con.Contype != pg_query.ConstrType_CONSTR_CHECK {
		return nil, nil, fmt.Errorf("not a check constraint")
	}
	return result, con.RawExpr, nil
}

func (a *literalAligner) alignDefault(current, desired *string) (*string, bool) {
	if current == nil || desired == nil {
		return current, false
	}
	out, changed := a.align(*current, *desired, parseDefault, deparseDefault)
	return &out, changed
}

func (a *literalAligner) alignCheck(current, desired string) (string, bool) {
	return a.align(current, desired, parseCheck, pgast.DeparseConstraintDef)
}

// alignTables returns tables with the literals in their column defaults and
// check constraints spelled the way desired spells them, where the server
// prints the two the same. A table any of whose columns changes type is
// left whole, since its checks may name that column.
func (a *literalAligner) alignTables(tables, desired *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
	out := tables.Clone()
	for key, t := range tables.All() {
		d, ok := desired.GetOk(key)
		if !ok || retypedTable(t, d) {
			continue
		}
		var cols *orderedmap.Map[string, *model.Column]
		for name, col := range t.Columns.All() {
			dc, ok := d.Columns.GetOk(name)
			if !ok {
				continue
			}
			def, changed := a.alignDefault(col.Default, dc.Default)
			if !changed {
				continue
			}
			if cols == nil {
				cols = t.Columns.Clone()
			}
			c := *col
			c.Default = def
			cols.Set(name, &c)
		}
		var cons *orderedmap.Map[string, *model.Constraint]
		for name, con := range t.Constraints.All() {
			dc, ok := d.Constraints.GetOk(name)
			if !ok || !con.Type.IsCheckConstraint() || !dc.Type.IsCheckConstraint() {
				continue
			}
			def, changed := a.alignCheck(con.Definition, dc.Definition)
			if !changed {
				continue
			}
			if cons == nil {
				cons = t.Constraints.Clone()
			}
			c := *con
			c.Definition = def
			cons.Set(name, &c)
		}
		if cols == nil && cons == nil {
			continue
		}
		copied := *t
		if cols != nil {
			copied.Columns = cols
		}
		if cons != nil {
			copied.Constraints = cons
		}
		out.Set(key, &copied)
	}
	return out
}

// retypedTable reports whether any column the two sides share changes type.
func retypedTable(current, desired *model.Table) bool {
	for name, col := range current.Columns.All() {
		if dc, ok := desired.Columns.GetOk(name); ok && !diff.EqualTypeName(col.TypeName, dc.TypeName, current.Schema) {
			return true
		}
	}
	return false
}

// alignDomains is alignTables for domains: the default and each check
// constraint, unless the base type changes.
func (a *literalAligner) alignDomains(domains, desired *orderedmap.Map[string, *model.Domain]) *orderedmap.Map[string, *model.Domain] {
	out := domains.Clone()
	for key, d := range domains.All() {
		dd, ok := desired.GetOk(key)
		if !ok || !diff.EqualTypeName(d.BaseType, dd.BaseType, d.Schema) {
			continue
		}
		def, defChanged := a.alignDefault(d.Default, dd.Default)
		var cons []*model.DomainConstraint
		for i, c := range d.Constraints {
			for _, dc := range dd.Constraints {
				if dc.Name != c.Name {
					continue
				}
				aligned, changed := a.alignCheck(c.Definition, dc.Definition)
				if !changed {
					break
				}
				if cons == nil {
					cons = make([]*model.DomainConstraint, len(d.Constraints))
					copy(cons, d.Constraints)
				}
				copied := *c
				copied.Definition = aligned
				cons[i] = &copied
				break
			}
		}
		if !defChanged && cons == nil {
			continue
		}
		copied := *d
		if defChanged {
			copied.Default = def
		}
		if cons != nil {
			copied.Constraints = cons
		}
		out.Set(key, &copied)
	}
	return out
}

// evaluateLiterals runs the aligner's two passes: it collects the literals
// that differ, has print evaluate them in one call, and returns the current
// tables and domains rewritten with the matches.
func evaluateLiterals(
	print literalPrinter,
	tables, desiredTables *orderedmap.Map[string, *model.Table],
	domains, desiredDomains *orderedmap.Map[string, *model.Domain],
) (*orderedmap.Map[string, *model.Table], *orderedmap.Map[string, *model.Domain]) {
	a := &literalAligner{seen: map[string]bool{}}
	a.alignTables(tables, desiredTables)
	a.alignDomains(domains, desiredDomains)
	if len(a.exprs) == 0 {
		return tables, domains
	}
	a.printed = print(a.exprs)
	return a.alignTables(tables, desiredTables), a.alignDomains(domains, desiredDomains)
}
