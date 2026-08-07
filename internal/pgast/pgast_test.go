package pgast_test

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/pgast"
)

func TestParseConstraintDef_Unique(t *testing.T) {
	con := pgast.ParseConstraintDef("UNIQUE (email)")
	require.NotNil(t, con)
	require.Len(t, con.Keys, 1)
	assert.Equal(t, "email", con.Keys[0].GetString_().Sval)
}

func TestParseConstraintDef_Invalid(t *testing.T) {
	assert.Nil(t, pgast.ParseConstraintDef("not a valid def"))
	assert.Nil(t, pgast.ParseConstraintDef(""))
}

func TestParseConstraintDefStrict_Error(t *testing.T) {
	_, _, err := pgast.ParseConstraintDefStrict("not a valid def")
	require.Error(t, err)
}

func TestDeparseConstraintDef_RoundTrip(t *testing.T) {
	result, _, err := pgast.ParseConstraintDefStrict("UNIQUE (email)")
	require.NoError(t, err)
	got, err := pgast.DeparseConstraintDef(result)
	require.NoError(t, err)
	assert.Equal(t, "UNIQUE (email)", got)
}

func TestWalkExprColumnRefs_CollectsAndMutates(t *testing.T) {
	con := pgast.ParseConstraintDef("CHECK ((qty > 0 AND qty < 1000))")
	require.NotNil(t, con)

	var seen []string
	pgast.WalkExprColumnRefs(con.RawExpr, func(s *pg_query.String) {
		seen = append(seen, s.Sval)
	})
	assert.Equal(t, []string{"qty", "qty"}, seen)

	// Mutate via visitor.
	pgast.WalkExprColumnRefs(con.RawExpr, func(s *pg_query.String) {
		if s.Sval == "qty" {
			s.Sval = "quantity"
		}
	})
	var after []string
	pgast.WalkExprColumnRefs(con.RawExpr, func(s *pg_query.String) {
		after = append(after, s.Sval)
	})
	assert.Equal(t, []string{"quantity", "quantity"}, after)
}

// collectRefs is a small helper that returns all unqualified column refs
// in the RawExpr of a CHECK constraint definition.
func collectRefs(t *testing.T, def string) []string {
	t.Helper()
	con := pgast.ParseConstraintDef(def)
	require.NotNil(t, con)
	var refs []string
	pgast.WalkExprColumnRefs(con.RawExpr, func(s *pg_query.String) {
		refs = append(refs, s.Sval)
	})
	return refs
}

func TestWalkExprColumnRefs_TypeCast(t *testing.T) {
	assert.Equal(t, []string{"col"}, collectRefs(t, "CHECK ((col::text = 'x'))"))
}

func TestWalkExprColumnRefs_FuncCall(t *testing.T) {
	assert.Equal(t, []string{"col"}, collectRefs(t, "CHECK ((lower(col) = 'x'))"))
}

func TestWalkExprColumnRefs_NullTest(t *testing.T) {
	assert.Equal(t, []string{"col"}, collectRefs(t, "CHECK ((col IS NULL))"))
}

func TestWalkExprColumnRefs_Coalesce(t *testing.T) {
	assert.Equal(t, []string{"col"}, collectRefs(t, "CHECK ((COALESCE(col, 0) > 0))"))
}

func TestWalkExprColumnRefs_CaseExpr(t *testing.T) {
	assert.Equal(t, []string{"col"}, collectRefs(t,
		"CHECK (((CASE WHEN col > 0 THEN 1 ELSE 0 END) = 1))"))
}

func TestWalkExprColumnRefs_AnyArray(t *testing.T) {
	assert.Equal(t, []string{"status"}, collectRefs(t,
		"CHECK ((status = ANY (ARRAY['a'::text, 'b'::text])))"))
}

func TestWalkExprColumnRefs_InList(t *testing.T) {
	assert.Equal(t, []string{"status"}, collectRefs(t,
		"CHECK ((status IN ('a', 'b')))"))
}

// PostgreSQL names a CHECK constraint after the column its expression
// references, so a node kind the walker skips costs the constraint its column
// name. Each of these forms is named t_a_check by the server.
func TestWalkExprColumnRefs_Indirection(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((a[1] > 0))"))
}

func TestWalkExprColumnRefs_MinMaxExpr(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((GREATEST(a, 1) > 0))"))
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((LEAST(a, 1) > 0))"))
}

func TestWalkExprColumnRefs_RowExpr(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((ROW(a) IS NOT NULL))"))
	assert.Equal(t, []string{"a", "b"}, collectRefs(t, "CHECK (((a, b) IS NOT NULL))"))
}

func TestWalkExprColumnRefs_BooleanTest(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((a IS TRUE))"))
}

func TestWalkExprColumnRefs_CollateClause(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, `CHECK (((a COLLATE "C") > 'x'))`))
}

func TestWalkExprColumnRefs_NamedArgExpr(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((f(x => a, y => 1) > 0))"))
}

func TestWalkExprColumnRefs_XmlExpr(t *testing.T) {
	// The element name lives in XmlExpr.Name, so only the column comes back.
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((xmlelement(name e, a) IS NOT NULL))"))
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((xmlelement(name e, xmlattributes(a AS x)) IS NOT NULL))"))
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((xmlforest(a AS x) IS NOT NULL))"))
}

// An element name, an attribute alias and an argument name are written where
// a column name could be, so a walk that picked them up would name a
// constraint after the wrong one. Each of these has a column of that name on
// the same table.
func TestWalkExprColumnRefs_NamesAreNotColumns(t *testing.T) {
	assert.Equal(t, []string{"b"}, collectRefs(t, "CHECK ((xmlelement(name a, b) IS NOT NULL))"))
	assert.Equal(t, []string{"b"}, collectRefs(t, "CHECK ((xmlelement(name e, xmlattributes(b AS a)) IS NOT NULL))"))
	assert.Equal(t, []string{"b"}, collectRefs(t, "CHECK ((xmlforest(b AS a) IS NOT NULL))"))
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((f(x => a, y => 1) > 0))"))
}

func TestWalkExprColumnRefs_XmlSerialize(t *testing.T) {
	// xmlserialize is its own node kind, not an XmlExpr. TypeName is a type,
	// so only the serialized expression is walked.
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((xmlserialize(content a AS text) > ''))"))
}

// PostgreSQL 16 added the SQL/JSON constructors, and PostgreSQL 17 the query
// functions. Each of these forms reaches a column, so the server names the
// constraint after it.
func TestWalkExprColumnRefs_JsonObjectConstructor(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((JSON_OBJECT('k': a) IS NOT NULL))"))
	// A key is an expression as well, and the server counts a column there.
	assert.Equal(t, []string{"a", "b"}, collectRefs(t, "CHECK ((JSON_OBJECT(a: b) IS NOT NULL))"))
}

func TestWalkExprColumnRefs_JsonArrayConstructor(t *testing.T) {
	assert.Equal(t, []string{"a", "b"}, collectRefs(t, "CHECK ((JSON_ARRAY(a, b) IS NOT NULL))"))
	// RETURNING holds a type, so it contributes no column.
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((JSON_ARRAY(a RETURNING jsonb) IS NOT NULL))"))
}

func TestWalkExprColumnRefs_JsonIsPredicate(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((a IS JSON))"))
}

func TestWalkExprColumnRefs_JsonParseExpr(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((JSON(a) IS NOT NULL))"))
}

func TestWalkExprColumnRefs_JsonScalarExpr(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((JSON_SCALAR(a) IS NOT NULL))"))
}

func TestWalkExprColumnRefs_JsonSerializeExpr(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((JSON_SERIALIZE(a) > ''))"))
}

func TestWalkExprColumnRefs_JsonFuncExpr(t *testing.T) {
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((JSON_EXISTS(a, '$.x')))"))
	assert.Equal(t, []string{"a"}, collectRefs(t, "CHECK ((JSON_QUERY(a, '$.x') IS NOT NULL))"))
	// The context item, a PASSING argument and an ON EMPTY / ON ERROR default
	// are all expressions the server reads.
	assert.Equal(t, []string{"a", "b", "c"}, collectRefs(t,
		"CHECK ((JSON_VALUE(a, '$.x' PASSING b AS v DEFAULT c ON ERROR) > ''))"))
	assert.Equal(t, []string{"a", "b", "c", "d"}, collectRefs(t,
		"CHECK ((JSON_VALUE(a, '$.x' PASSING b AS v DEFAULT c ON EMPTY DEFAULT d ON ERROR) > ''))"))
}

func TestWalkExprColumnRefs_JsonNested(t *testing.T) {
	assert.Equal(t, []string{"a", "b"}, collectRefs(t,
		"CHECK ((JSON_OBJECT('k': JSON_ARRAY(a, b)) IS NOT NULL))"))
}

// A PASSING argument is named where a column name could be, so a walk that
// picked the name up would name the constraint after the wrong one.
func TestWalkExprColumnRefs_JsonArgumentNameIsNotAColumn(t *testing.T) {
	assert.Equal(t, []string{"b", "c"}, collectRefs(t,
		"CHECK ((JSON_VALUE(b, '$.x' PASSING c AS a) > ''))"))
}

// collectRefNodes returns every column reference in a CHECK definition,
// qualified ones included, and counts the sub-queries handed back.
func collectRefNodes(t *testing.T, def string, enter bool) (refs []string, subs int) {
	t.Helper()
	con := pgast.ParseConstraintDef(def)
	require.NotNil(t, con)
	var visitSubLink func(*pg_query.SubLink)
	if enter {
		visitSubLink = func(*pg_query.SubLink) { subs++ }
	}
	pgast.WalkExprColumnRefNodes(con.RawExpr, func(cr *pg_query.ColumnRef) {
		var parts []string
		for _, f := range cr.Fields {
			parts = append(parts, f.GetString_().GetSval())
		}
		refs = append(refs, strings.Join(parts, "."))
	}, visitSubLink)
	return refs, subs
}

// collectSelectRefs returns every column reference in one SELECT target. A
// view body is where the walker meets an aggregate or a window function; a
// CHECK constraint cannot hold either.
func collectSelectRefs(t *testing.T, target string) []string {
	t.Helper()
	result, err := pg_query.Parse("SELECT " + target + " FROM t")
	require.NoError(t, err)
	rt := result.Stmts[0].Stmt.GetSelectStmt().TargetList[0].GetResTarget()
	require.NotNil(t, rt)
	var refs []string
	pgast.WalkExprColumnRefNodes(rt.Val, func(cr *pg_query.ColumnRef) {
		refs = append(refs, cr.Fields[0].GetString_().GetSval())
	}, nil)
	return refs
}

func TestWalkExprColumnRefNodes_AggregateAndWindow(t *testing.T) {
	assert.Equal(t, []string{"a", "a"}, collectSelectRefs(t, "string_agg(a, ',' ORDER BY a)"))
	assert.Equal(t, []string{"b"}, collectSelectRefs(t, "count(*) FILTER (WHERE b)"))
	assert.Equal(t, []string{"n", "a", "n"}, collectSelectRefs(t, "sum(n) OVER (PARTITION BY a ORDER BY n)"))
}

// The SQL/JSON aggregates carry their argument in a shape of their own, and
// the ORDER BY, FILTER and OVER clauses in a constructor beside it.
func TestWalkExprColumnRefNodes_JsonAggregates(t *testing.T) {
	assert.Equal(t, []string{"a", "n"}, collectSelectRefs(t, "JSON_OBJECTAGG(a: n)"))
	assert.Equal(t, []string{"n", "a"}, collectSelectRefs(t, "JSON_ARRAYAGG(n ORDER BY a)"))
	assert.Equal(t, []string{"n", "b"}, collectSelectRefs(t, "JSON_ARRAYAGG(n) FILTER (WHERE b)"))
	assert.Equal(t, []string{"a", "n", "b"}, collectSelectRefs(t, "JSON_OBJECTAGG(a: n) FILTER (WHERE b)"))
	assert.Equal(t, []string{"n", "a"}, collectSelectRefs(t, "JSON_ARRAYAGG(n) OVER (PARTITION BY a)"))
}

func TestWalkExprColumnRefNodes_VisitsQualifiedRefs(t *testing.T) {
	refs, _ := collectRefNodes(t, "CHECK ((t.a > b))", false)
	assert.Equal(t, []string{"t.a", "b"}, refs)
}

func TestWalkExprColumnRefNodes_SubLink(t *testing.T) {
	def := "CHECK ((a > (SELECT max(b) FROM other)))"
	// Without a visitor the sub-query is left alone.
	refs, subs := collectRefNodes(t, def, false)
	assert.Equal(t, []string{"a"}, refs)
	assert.Equal(t, 0, subs)

	refs, subs = collectRefNodes(t, def, true)
	assert.Equal(t, []string{"a"}, refs)
	assert.Equal(t, 1, subs)
}

func TestWalkExprColumnRefs_NilSafe(t *testing.T) {
	pgast.WalkExprColumnRefs(nil, func(s *pg_query.String) {
		t.Fatal("visitor should not be invoked on nil node")
	})
}

func TestWalkExprColumnRefs_SkipsQualifiedRefs(t *testing.T) {
	// `t.col` should not be visited because the diff scope is local.
	con := pgast.ParseConstraintDef("CHECK ((t.col > 0))")
	require.NotNil(t, con)
	called := false
	pgast.WalkExprColumnRefs(con.RawExpr, func(s *pg_query.String) {
		called = true
	})
	assert.False(t, called)
}
