package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Each "catalog" string below is what a PostgreSQL 17 server printed for the
// matching "written" one; 18 prints the same. The plan fixtures cover the same
// ground against a live server, and these pin the rules on every server so a
// 15 or 16 run still catches a regression.
func TestEqualViewDef_jsonQueryFunctionDefaultsDropped(t *testing.T) {
	for _, tt := range []struct{ catalog, written string }{
		{
			`SELECT JSON_VALUE(a, '$."x"' RETURNING text) AS c FROM t`,
			`SELECT JSON_VALUE(t.a, '$.x') AS c FROM public.t`,
		},
		{
			`SELECT JSON_QUERY(a, '$."x"' RETURNING jsonb WITHOUT WRAPPER KEEP QUOTES) AS c FROM t`,
			`SELECT JSON_QUERY(t.a, '$.x') AS c FROM public.t`,
		},
		{
			`SELECT JSON_EXISTS(a, '$."x"') AS c FROM t`,
			`SELECT JSON_EXISTS(t.a, '$.x') AS c FROM public.t`,
		},
		{
			`SELECT JSON_SERIALIZE(a RETURNING text) AS c FROM t`,
			`SELECT JSON_SERIALIZE(t.a) AS c FROM public.t`,
		},
		{
			`SELECT JSON_SCALAR(n) AS c FROM t`,
			`SELECT JSON_SCALAR(t.n) AS c FROM public.t`,
		},
		// The defaults spelled out on the written side mean the same thing.
		{
			`SELECT JSON_VALUE(a, '$."x"' RETURNING text) AS c FROM t`,
			`SELECT JSON_VALUE(t.a, '$.x' RETURNING text NULL ON ERROR) AS c FROM public.t`,
		},
		{
			`SELECT JSON_QUERY(a, '$."x"' RETURNING jsonb WITHOUT WRAPPER KEEP QUOTES) AS c FROM t`,
			`SELECT JSON_QUERY(t.a, '$.x' RETURNING jsonb WITHOUT WRAPPER KEEP QUOTES) AS c FROM public.t`,
		},
		{
			`SELECT JSON_EXISTS(a, '$."x"') AS c FROM t`,
			`SELECT JSON_EXISTS(t.a, '$.x' FALSE ON ERROR) AS c FROM public.t`,
		},
	} {
		assert.True(t, equalViewDef(tt.catalog, tt.written), tt.written)
	}
}

// The server prints these whether or not they hold the default, so dropping
// them from the catalog side wherever the written side left them off would
// report a settled schema for a view that behaves differently at runtime.
func TestEqualViewDef_jsonQueryFunctionNonDefaultsSurface(t *testing.T) {
	written := `SELECT JSON_QUERY(t.a, '$.x') AS c FROM public.t`
	for _, catalog := range []string{
		`SELECT JSON_QUERY(a, '$."x"' RETURNING jsonb WITH UNCONDITIONAL WRAPPER KEEP QUOTES) AS c FROM t`,
		`SELECT JSON_QUERY(a, '$."x"' RETURNING jsonb WITH CONDITIONAL WRAPPER KEEP QUOTES) AS c FROM t`,
		`SELECT JSON_QUERY(a, '$."x"' RETURNING jsonb WITHOUT WRAPPER OMIT QUOTES) AS c FROM t`,
		`SELECT JSON_QUERY(a, '$."x"' RETURNING json WITHOUT WRAPPER KEEP QUOTES) AS c FROM t`,
		`SELECT JSON_QUERY(a, '$."x"' RETURNING jsonb WITHOUT WRAPPER KEEP QUOTES EMPTY ARRAY ON EMPTY) AS c FROM t`,
	} {
		assert.False(t, equalViewDef(catalog, written), catalog)
	}

	written = `SELECT JSON_VALUE(t.a, '$.x') AS c FROM public.t`
	for _, catalog := range []string{
		`SELECT JSON_VALUE(a, '$."x"' RETURNING character varying(10)) AS c FROM t`,
		`SELECT JSON_VALUE(a, '$."x"' RETURNING integer) AS c FROM t`,
		`SELECT JSON_VALUE(a, '$."x"' RETURNING text ERROR ON ERROR) AS c FROM t`,
		`SELECT JSON_VALUE(a, '$."x"' RETURNING text DEFAULT 'z'::text ON EMPTY) AS c FROM t`,
		`SELECT JSON_VALUE(a, '$."y"' RETURNING text) AS c FROM t`,
	} {
		assert.False(t, equalViewDef(catalog, written), catalog)
	}

	assert.False(t, equalViewDef(
		`SELECT JSON_EXISTS(a, '$."x"' UNKNOWN ON ERROR) AS c FROM t`,
		`SELECT JSON_EXISTS(t.a, '$.x') AS c FROM public.t`))
}

// JSON_OBJECT and friends resolve json or jsonb from their argument types,
// which pg_query cannot work out, so the catalog's clause is dropped only
// where the written side left it off.
func TestEqualConstraintDef_jsonConstructorReturningDropped(t *testing.T) {
	for _, tt := range []struct{ catalog, written string }{
		{`CHECK ((JSON_OBJECT('k' : a RETURNING jsonb) IS NOT NULL))`, `CHECK (JSON_OBJECT('k': a) IS NOT NULL)`},
		{`CHECK ((JSON_ARRAY(b RETURNING json) IS NOT NULL))`, `CHECK (JSON_ARRAY(b) IS NOT NULL)`},
		{`CHECK ((JSON_OBJECTAGG(b : a RETURNING jsonb) IS NOT NULL))`, `CHECK (JSON_OBJECTAGG(b: a) IS NOT NULL)`},
		{`CHECK ((JSON_ARRAYAGG(a RETURNING jsonb) IS NOT NULL))`, `CHECK (JSON_ARRAYAGG(a) IS NOT NULL)`},
		// The server never prints FORMAT on a constructor's RETURNING clause.
		{`CHECK ((JSON_OBJECT('k' : a RETURNING jsonb) IS NOT NULL))`, `CHECK (JSON_OBJECT('k': a RETURNING jsonb FORMAT JSON) IS NOT NULL)`},
	} {
		assert.True(t, equalConstraintDef(tt.catalog, tt.written), tt.written)
	}
}

// A clause the written side did spell out is compared as written, and a type
// the server would never resolve to stays a difference.
func TestEqualConstraintDef_jsonConstructorWrittenReturningKept(t *testing.T) {
	for _, tt := range []struct{ catalog, written string }{
		{`CHECK ((JSON_OBJECT('k' : a RETURNING jsonb) IS NOT NULL))`, `CHECK (JSON_OBJECT('k': a RETURNING json) IS NOT NULL)`},
		{`CHECK ((JSON_OBJECT('k' : a RETURNING text) IS NOT NULL))`, `CHECK (JSON_OBJECT('k': a) IS NOT NULL)`},
		{`CHECK ((JSON_ARRAY(b RETURNING json) IS NOT NULL))`, `CHECK (JSON_ARRAY(b RETURNING jsonb) IS NOT NULL)`},
	} {
		assert.False(t, equalConstraintDef(tt.catalog, tt.written), tt.written)
	}
}

// Outside the query functions a path is an ordinary argument and only the
// catalog spells out the ::jsonpath cast that identifies it.
func TestEqualViewDef_jsonPathOutsideQueryFunctions(t *testing.T) {
	assert.True(t, equalViewDef(
		`SELECT a @? '$."x"'::jsonpath AS c FROM t`,
		`SELECT (t.a @? '$.x') AS c FROM public.t`))
	assert.True(t, equalViewDef(
		`SELECT jsonb_path_query_first(a, '$."x"'::jsonpath) AS c FROM t`,
		`SELECT jsonb_path_query_first(t.a, '$.x') AS c FROM public.t`))
	assert.False(t, equalViewDef(
		`SELECT a @? '$."x"'::jsonpath AS c FROM t`,
		`SELECT (t.a @? '$.y') AS c FROM public.t`))
}

// A path the canonicaliser does not recognise is left alone on both sides, so
// the two spellings still differ. Recorded in TODO.md.
func TestEqualViewDef_jsonPathFilterStillDrifts(t *testing.T) {
	assert.False(t, equalViewDef(
		`SELECT JSON_VALUE(a, '$."x"?(@ > 1)' RETURNING text) AS c FROM t`,
		`SELECT JSON_VALUE(t.a, '$.x ? (@ > 1)') AS c FROM public.t`))
}

// JSON_ARRAY over a sub-query and JSON() are their own node kinds, and both
// carry a RETURNING clause the catalog prints back.
func TestEqualViewDef_jsonArrayQueryAndParse(t *testing.T) {
	assert.True(t, equalViewDef(
		`SELECT JSON_ARRAY(SELECT a FROM other RETURNING jsonb) AS c FROM t`,
		`SELECT JSON_ARRAY(SELECT other.a FROM public.other) AS c FROM public.t`))
	assert.True(t, equalViewDef(
		`SELECT JSON(b) AS c FROM t`,
		`SELECT JSON(t.b) AS c FROM public.t`))
}

// FORMAT on a JSON_QUERY RETURNING clause is printed back, so the clause is
// kept and the two sides only agree when both carry it.
func TestEqualViewDef_jsonQueryReturningFormatKept(t *testing.T) {
	withFormat := `SELECT JSON_QUERY(a, '$."x"' RETURNING jsonb FORMAT JSON WITHOUT WRAPPER KEEP QUOTES) AS c FROM t`
	assert.True(t, equalViewDef(withFormat,
		`SELECT JSON_QUERY(t.a, '$.x' RETURNING jsonb FORMAT JSON) AS c FROM public.t`))
	assert.False(t, equalViewDef(withFormat,
		`SELECT JSON_QUERY(t.a, '$.x') AS c FROM public.t`))
}

// The written side of a path outside the query functions is only recognised
// through the catalog's ::jsonpath cast, so anything that is not a string
// constant there is left alone rather than guessed at.
func TestEqualViewDef_jsonPathWrittenSideNotALiteral(t *testing.T) {
	assert.True(t, equalViewDef(
		`SELECT a @? b::jsonpath AS c FROM t`,
		`SELECT (t.a @? t.b::jsonpath) AS c FROM public.t`))
	assert.False(t, equalViewDef(
		`SELECT a @? '$."x"'::jsonpath AS c FROM t`,
		`SELECT (t.a @? t.b) AS c FROM public.t`))
}
