package pgast_test

import (
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
