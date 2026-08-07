package pgast_test

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/pgast"
)

func parseStmt(t *testing.T, sql string) *pg_query.Node {
	t.Helper()
	result, err := pg_query.Parse(sql)
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	return result.Stmts[0].Stmt
}

func deparse(t *testing.T, node *pg_query.Node) string {
	t.Helper()
	out, err := pg_query.Deparse(&pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{Stmt: node}},
	})
	require.NoError(t, err)
	return out
}

func TestWalk_NilSafe(t *testing.T) {
	assert.Nil(t, pgast.Walk(nil, pgast.WalkOptions{}, func(_ pgast.Ctx, n *pg_query.Node) *pg_query.Node {
		t.Fatal("visitor should not be invoked on a nil node")
		return n
	}))
}

// A node kind nobody enumerated still gets walked, which is the whole point.
// JSON_OBJECT reaches its column through JsonObjectConstructor, JsonKeyValue
// and JsonValueExpr, the middle two of which are not Nodes at all but plain
// messages holding them.
func TestWalk_ReachesNestedContainers(t *testing.T) {
	stmt := parseStmt(t, "SELECT JSON_OBJECT('k': a) AS c FROM t")
	found := false
	pgast.Walk(stmt, pgast.WalkOptions{}, func(_ pgast.Ctx, n *pg_query.Node) *pg_query.Node {
		if cr := n.GetColumnRef(); cr != nil && cr.Fields[0].GetString_().Sval == "a" {
			found = true
		}
		return n
	})
	assert.True(t, found)
}

// A replacement is written back both into a singular field and into a repeated
// one, and the walk continues past it. COALESCE holds its arguments in a
// repeated field, the WHERE operand in a singular one.
func TestWalk_ReplacesNodes(t *testing.T) {
	stmt := parseStmt(t, "SELECT COALESCE(a, b) FROM t WHERE c = 1")
	out := pgast.Walk(stmt, pgast.WalkOptions{}, func(_ pgast.Ctx, n *pg_query.Node) *pg_query.Node {
		cr := n.GetColumnRef()
		if cr == nil || len(cr.Fields) != 1 {
			return n
		}
		s := cr.Fields[0].GetString_()
		if s == nil || s.Sval == "z" {
			return n
		}
		return &pg_query.Node{Node: &pg_query.Node_ColumnRef{
			ColumnRef: &pg_query.ColumnRef{Fields: []*pg_query.Node{
				{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "z"}}},
			}},
		}}
	})
	assert.Equal(t, "SELECT COALESCE(z, z) FROM t WHERE z = 1", deparse(t, out))
}

// The root itself can be replaced, which only the return value can carry.
func TestWalk_ReplacesRoot(t *testing.T) {
	node := parseStmt(t, "SELECT 1").GetSelectStmt().TargetList[0].GetResTarget().Val
	replacement := &pg_query.Node{Node: &pg_query.Node_ColumnRef{
		ColumnRef: &pg_query.ColumnRef{Fields: []*pg_query.Node{
			{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "a"}}},
		}},
	}}
	out := pgast.Walk(node, pgast.WalkOptions{}, func(_ pgast.Ctx, n *pg_query.Node) *pg_query.Node {
		if n.GetAConst() != nil {
			return replacement
		}
		return n
	})
	assert.Same(t, replacement, out)
}

// A nested query is skipped wherever it hangs, not only under a sub-link:
// JSON_ARRAY holds one directly, which is a shape that only appeared once the
// walk stopped enumerating node kinds.
func TestWalk_SkipSubqueries(t *testing.T) {
	collect := func(sql string, opts pgast.WalkOptions) []string {
		stmt := parseStmt(t, sql)
		var names []string
		pgast.Walk(stmt, opts, func(_ pgast.Ctx, n *pg_query.Node) *pg_query.Node {
			if cr := n.GetColumnRef(); cr != nil && len(cr.Fields) == 1 {
				names = append(names, cr.Fields[0].GetString_().Sval)
			}
			return n
		})
		return names
	}
	sublink := "SELECT a FROM t WHERE b IN (SELECT c FROM other)"
	assert.Equal(t, []string{"a", "b", "c"}, collect(sublink, pgast.WalkOptions{}))
	assert.Equal(t, []string{"a", "b"}, collect(sublink, pgast.WalkOptions{SkipSubqueries: true}))

	jsonArray := "SELECT a FROM t WHERE JSON_ARRAY(SELECT c FROM other) IS NOT NULL"
	assert.Equal(t, []string{"a", "c"}, collect(jsonArray, pgast.WalkOptions{}))
	assert.Equal(t, []string{"a"}, collect(jsonArray, pgast.WalkOptions{SkipSubqueries: true}))

	cte := "SELECT a FROM t WHERE b IN (WITH x AS (SELECT c FROM other) SELECT c FROM x)"
	assert.Equal(t, []string{"a", "b"}, collect(cte, pgast.WalkOptions{SkipSubqueries: true}))
}

// IsSelectTarget has to separate a SELECT target from the ResTarget that
// xmlattributes() and xmlforest() also use, which is why it looks two levels up.
func TestCtx_IsSelectTarget(t *testing.T) {
	stmt := parseStmt(t, "SELECT a::text AS c, xmlelement(name e, xmlattributes(b::text AS x)) FROM t")
	var atTarget, elsewhere []string
	pgast.Walk(stmt, pgast.WalkOptions{}, func(ctx pgast.Ctx, n *pg_query.Node) *pg_query.Node {
		tc := n.GetTypeCast()
		if tc == nil {
			return n
		}
		name := tc.Arg.GetColumnRef().Fields[0].GetString_().Sval
		if ctx.IsSelectTarget() {
			atTarget = append(atTarget, name)
		} else {
			elsewhere = append(elsewhere, name)
		}
		return n
	})
	assert.Equal(t, []string{"a"}, atTarget)
	assert.Equal(t, []string{"b"}, elsewhere)
}

// The zero Ctx is what the root of a walk gets, and it is not a target.
func TestCtx_IsSelectTarget_Zero(t *testing.T) {
	assert.False(t, pgast.Ctx{}.IsSelectTarget())
}

func TestWalkPair_NilSafe(t *testing.T) {
	node := parseStmt(t, "SELECT 1")
	visit := func(_ pgast.Ctx, _, current *pg_query.Node) *pg_query.Node {
		t.Fatal("visitor should not be invoked when either side is nil")
		return current
	}
	assert.Nil(t, pgast.WalkPair(node, nil, visit))
	assert.Same(t, node, pgast.WalkPair(nil, node, visit))
}

// The pair walk unwraps a cast the current side carries and the desired side
// does not, then keeps pairing what is underneath.
func TestWalkPair_StripsCurrentOnlyWrapper(t *testing.T) {
	desired := parseStmt(t, "SELECT a FROM t WHERE b = 'x'")
	current := parseStmt(t, "SELECT a FROM t WHERE b = 'x'::text")
	out := pgast.WalkPair(desired, current, stripCurrentCast)
	assert.Equal(t, deparse(t, desired), deparse(t, out))
}

// Where one field holds different kinds of node on the two sides the walk
// stops there rather than pairing unrelated positions, so a cast nested under
// it is left alone. Other fields keep going: the guard is per field, not per
// statement.
func TestWalkPair_StopsOnKindMismatch(t *testing.T) {
	desired := parseStmt(t, "SELECT lower(a) FROM t")
	current := parseStmt(t, "SELECT COALESCE(a, 'x'::text) FROM t")
	before := deparse(t, current)
	out := pgast.WalkPair(desired, current, stripCurrentCast)
	assert.Equal(t, before, deparse(t, out))
}

// Same for a repeated field whose lengths disagree: pairing by index would
// compare unrelated arguments.
func TestWalkPair_StopsOnListLengthMismatch(t *testing.T) {
	desired := parseStmt(t, "SELECT COALESCE(a, b) FROM t")
	current := parseStmt(t, "SELECT COALESCE(a, b, 'x'::text) FROM t")
	before := deparse(t, current)
	out := pgast.WalkPair(desired, current, stripCurrentCast)
	assert.Equal(t, before, deparse(t, out))
}

func TestWalkPair_ReplacesInList(t *testing.T) {
	desired := parseStmt(t, "SELECT COALESCE(a, 'x') FROM t")
	current := parseStmt(t, "SELECT COALESCE(a, 'x'::text) FROM t")
	out := pgast.WalkPair(desired, current, stripCurrentCast)
	assert.Equal(t, deparse(t, desired), deparse(t, out))
}

func stripCurrentCast(_ pgast.Ctx, desired, current *pg_query.Node) *pg_query.Node {
	if ct := current.GetTypeCast(); ct != nil && ct.Arg != nil && desired.GetTypeCast() == nil {
		return ct.Arg
	}
	return current
}
