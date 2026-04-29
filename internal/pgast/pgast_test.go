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
