package parser_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

func TestParseSQL_Function(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.add(a integer, b integer DEFAULT 1) RETURNS integer
		    LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT a + b $$;
	`)
	require.NoError(t, err)
	require.Equal(t, 1, result.Routines.Len())

	r, ok := result.Routines.GetOk("public.add(integer, integer)")
	require.True(t, ok)
	assert.Equal(t, "public", r.Schema)
	assert.Equal(t, "add", r.Name)
	assert.False(t, r.Procedure)
	assert.Equal(t, "integer", r.ReturnType)
	assert.False(t, r.ReturnsSet)
	assert.Equal(t, "sql", r.Language)
	assert.Equal(t, " SELECT a + b ", r.Body)
	assert.Equal(t, "IMMUTABLE", r.Volatility)
	assert.True(t, r.Strict)
	assert.Equal(t, model.ParallelUnsafe, r.Parallel)
	require.Len(t, r.Args, 2)
	assert.Equal(t, &model.RoutineArg{Name: "a", Type: "integer"}, r.Args[0])
	assert.Equal(t, &model.RoutineArg{Name: "b", Type: "integer", Default: "1"}, r.Args[1])
}

func TestParseSQL_Procedure(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE PROCEDURE public.bump(INOUT x integer)
		    LANGUAGE plpgsql AS $$ BEGIN x := x + 1; END $$;
	`)
	require.NoError(t, err)

	r, ok := result.Routines.GetOk("public.bump(integer)")
	require.True(t, ok)
	assert.True(t, r.Procedure)
	assert.Equal(t, "PROCEDURE", r.Kind())
	assert.Empty(t, r.ReturnType)
	assert.Equal(t, "INOUT", r.Args[0].Mode)
}

func TestParseSQL_FunctionDefaultSchema(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`CREATE FUNCTION f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;`)
	require.NoError(t, err)

	r, ok := result.Routines.GetOk("public.f()")
	require.True(t, ok)
	assert.Equal(t, "public", r.Schema)
}

func TestParseSQL_FunctionReturnsTable(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.rows_of() RETURNS TABLE(id integer, name text)
		    LANGUAGE sql AS $$ SELECT 1, 'x' $$;
	`)
	require.NoError(t, err)

	r, ok := result.Routines.GetOk("public.rows_of()")
	require.True(t, ok)
	// The TABLE columns are parameters, and the result type is a set of record.
	assert.Equal(t, "record", r.ReturnType)
	assert.True(t, r.ReturnsSet)
	require.Len(t, r.TableArgs(), 2)
	assert.Equal(t, "id", r.TableArgs()[0].Name)
}

// A function with OUT parameters and no RETURNS clause takes its result type
// from those parameters, the way PostgreSQL derives it.
func TestParseSQL_FunctionOutParamsWithoutReturns(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sql        string
		key        string
		returnType string
	}{
		{"single", `CREATE FUNCTION public.one(OUT a integer) LANGUAGE sql AS $$ SELECT 1 $$;`, "public.one()", "integer"},
		{"multiple", `CREATE FUNCTION public.two(OUT a integer, OUT b text) LANGUAGE sql AS $$ SELECT 1, 'x' $$;`, "public.two()", "record"},
		{"inout counts as input", `CREATE FUNCTION public.io(INOUT a integer) LANGUAGE sql AS $$ SELECT 1 $$;`, "public.io(integer)", "integer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseSQLWithPublicSchema(tc.sql)
			require.NoError(t, err)
			r, ok := result.Routines.GetOk(tc.key)
			require.True(t, ok)
			assert.Equal(t, tc.returnType, r.ReturnType)
		})
	}
}

// PostgreSQL discards a type modifier on a parameter, so the parser drops it
// too: keeping it would differ from what the catalog reports back.
func TestParseSQL_FunctionArgTypeModifierDropped(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f(a varchar(10), b numeric(10,2), c timestamp(3) with time zone, d numeric(10,2)[])
		    RETURNS void LANGUAGE sql AS $$ SELECT $$;
	`)
	require.NoError(t, err)

	r, ok := result.Routines.GetOk("public.f(character varying, numeric, timestamp with time zone, numeric[])")
	require.True(t, ok)
	assert.Equal(t, "character varying", r.Args[0].Type)
	assert.Equal(t, "numeric", r.Args[1].Type)
	assert.Equal(t, "timestamp with time zone", r.Args[2].Type)
	assert.Equal(t, "numeric[]", r.Args[3].Type)
}

// pg_get_functiondef leaves out an attribute set to its default, so a desired
// schema that spells one out has to normalize to the same model.
func TestParseSQL_FunctionDefaultAttributesNormalized(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f() RETURNS integer
		    LANGUAGE sql VOLATILE PARALLEL UNSAFE COST 100 AS $$ SELECT 1 $$;
		CREATE FUNCTION public.g() RETURNS SETOF integer
		    LANGUAGE sql ROWS 1000 AS $$ SELECT 1 $$;
		CREATE FUNCTION public.h() RETURNS integer
		    LANGUAGE c AS 'obj', 'sym';
	`)
	require.NoError(t, err)

	f, _ := result.Routines.GetOk("public.f()")
	assert.Nil(t, f.Cost)
	assert.Equal(t, model.VolatilityVolatile, f.Volatility)
	assert.Equal(t, model.ParallelUnsafe, f.Parallel)

	g, _ := result.Routines.GetOk("public.g()")
	assert.Nil(t, g.Rows)

	// LANGUAGE c defaults to COST 1, and its AS clause names two strings.
	h, _ := result.Routines.GetOk("public.h()")
	assert.Nil(t, h.Cost)
	assert.Equal(t, "obj", h.ObjFile)
	assert.Equal(t, "sym", h.Body)
}

func TestParseSQL_FunctionNonDefaultCostAndRows(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f() RETURNS SETOF integer
		    LANGUAGE sql COST 5 ROWS 7 AS $$ SELECT 1 $$;
	`)
	require.NoError(t, err)

	r, _ := result.Routines.GetOk("public.f()")
	require.NotNil(t, r.Cost)
	require.NotNil(t, r.Rows)
	assert.InDelta(t, 5, *r.Cost, 0)
	assert.InDelta(t, 7, *r.Rows, 0)
}

// Either spelling of a SET value parses to the same thing, so a desired schema
// is free to quote it or not.
func TestParseSQL_FunctionSetConfig(t *testing.T) {
	for _, sql := range []string{
		`CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql SET search_path TO public, pg_temp AS $$ SELECT 1 $$;`,
		`CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql SET search_path TO 'public', 'pg_temp' AS $$ SELECT 1 $$;`,
		`CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql SET search_path = public, pg_temp AS $$ SELECT 1 $$;`,
	} {
		result, err := parseSQLWithPublicSchema(sql)
		require.NoError(t, err)
		r, _ := result.Routines.GetOk("public.f()")
		require.Len(t, r.Config, 1)
		assert.Equal(t, &model.RoutineConfig{Name: "search_path", Args: []string{"public", "pg_temp"}}, r.Config[0])
	}
}

func TestParseSQL_FunctionOverload(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f(a integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$;
		CREATE FUNCTION public.f(a text) RETURNS text LANGUAGE sql AS $$ SELECT a $$;
	`)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Routines.Len())
}

func TestParseSQL_DuplicateRoutine(t *testing.T) {
	_, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f(a integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$;
		CREATE FUNCTION public.f(a integer) RETURNS bigint LANGUAGE sql AS $$ SELECT a $$;
	`)
	assert.ErrorContains(t, err, "duplicate routine: public.f(integer)")
}

func TestParseSQL_FunctionIgnoreDirective(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		-- pista:ignore
		CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
	`)
	require.NoError(t, err)

	r, ok := result.Routines.GetOk("public.f()")
	require.True(t, ok)
	assert.True(t, r.Ignore)
}

func TestParseSQL_FunctionRenameNotSupported(t *testing.T) {
	_, err := parseSQLWithPublicSchema(`
		-- pista:renamed-from public.old
		CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
	`)
	assert.ErrorContains(t, err, "pista:renamed-from is not supported for routines")
}

// A routine pistachio does not manage is warned about and dropped, the same as
// any other unsupported statement. The catalog leaves out the same ones, so
// neither side of the diff sees them.
func TestParseSQL_UnmanagedRoutinesWarn(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"sql standard body", `CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql BEGIN ATOMIC SELECT 1; END;`},
		{"window function", `CREATE FUNCTION public.w(x integer) RETURNS integer WINDOW LANGUAGE c AS 'obj', 'sym';`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			restore := parser.SetWarnWriter(&buf)
			defer restore()

			result, err := parseSQLWithPublicSchema(tc.sql)
			require.NoError(t, err)
			assert.Equal(t, 0, result.Routines.Len())
			assert.Contains(t, buf.String(), "ignored unsupported statement:")
		})
	}
}

func TestParseSQL_CommentOnRoutine(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f(a integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$;
		CREATE FUNCTION public.f(a text) RETURNS text LANGUAGE sql AS $$ SELECT a $$;
		CREATE PROCEDURE public.p() LANGUAGE sql AS $$ SELECT $$;
		COMMENT ON FUNCTION public.f(integer) IS 'int one';
		COMMENT ON PROCEDURE public.p() IS 'a procedure';
	`)
	require.NoError(t, err)

	fInt, _ := result.Routines.GetOk("public.f(integer)")
	require.NotNil(t, fInt.Comment)
	assert.Equal(t, "int one", *fInt.Comment)

	fText, _ := result.Routines.GetOk("public.f(text)")
	assert.Nil(t, fText.Comment, "the comment must land on the named overload only")

	p, _ := result.Routines.GetOk("public.p()")
	require.NotNil(t, p.Comment)
	assert.Equal(t, "a procedure", *p.Comment)
}

// PostgreSQL lets the argument list be left out when the name is unambiguous.
func TestParseSQL_CommentOnRoutineWithoutArgs(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f(a integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$;
		COMMENT ON FUNCTION public.f IS 'only one';
	`)
	require.NoError(t, err)

	r, _ := result.Routines.GetOk("public.f(integer)")
	require.NotNil(t, r.Comment)
	assert.Equal(t, "only one", *r.Comment)
}

func TestParseSQL_CommentOnAmbiguousRoutineIsSkipped(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f(a integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$;
		CREATE FUNCTION public.f(a text) RETURNS text LANGUAGE sql AS $$ SELECT a $$;
		COMMENT ON FUNCTION public.f IS 'ambiguous';
	`)
	require.NoError(t, err)

	for _, r := range result.Routines.CollectValues() {
		assert.Nil(t, r.Comment)
	}
}

func TestParseRoutineDef_Errors(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{"not parseable", "CREATE FUNCTION (", "failed to parse routine definition"},
		{"several statements", "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$; SELECT 1;", "expected one statement"},
		{"not a routine", "SELECT 1;", "not a CREATE FUNCTION statement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.ParseRoutineDef(tc.sql, "public")
			assert.ErrorContains(t, err, tc.want)
		})
	}
}

// PostgreSQL rejects a function with neither a RETURNS clause nor an OUT
// parameter, but pg_query parses it, so the parser has to say so itself.
func TestParseSQL_FunctionWithoutReturnType(t *testing.T) {
	_, err := parseSQLWithPublicSchema(`CREATE FUNCTION public.f() LANGUAGE sql AS $$ SELECT $$;`)
	assert.ErrorContains(t, err, "has no RETURNS clause and no OUT parameter")
}

// An option pistachio does not handle leaves the routine unmanaged rather than
// failing the parse. A desired routine is read whether or not --manage-routine
// is set, so an error here would reach someone who never asked for routines.
func TestParseSQL_FunctionUnsupportedOption(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"transform", `CREATE FUNCTION public.f(a integer) RETURNS integer
		    LANGUAGE c TRANSFORM FOR TYPE integer AS 'obj', 'sym';`},
		{"support", `CREATE FUNCTION public.f(a integer) RETURNS integer
		    LANGUAGE sql SUPPORT public.f_supp AS $$ SELECT a $$;`},
		{"set from current", `CREATE FUNCTION public.f() RETURNS text
		    LANGUAGE sql SET search_path FROM CURRENT AS $$ SELECT 'x' $$;`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			restore := parser.SetWarnWriter(&buf)
			defer restore()

			result, err := parseSQLWithPublicSchema(tc.sql)
			require.NoError(t, err)
			assert.Equal(t, 0, result.Routines.Len())
			assert.Contains(t, buf.String(), "ignored unsupported statement:")
		})
	}
}

// The same statements must not fail a plan for someone who never opted in.
func TestParseSQL_UnsupportedRoutineDoesNotFailTheFile(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	result, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.t1 (id integer);
		CREATE FUNCTION public.f(a integer) RETURNS integer
		    LANGUAGE sql SUPPORT public.f_supp AS $$ SELECT a $$;
	`)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Tables.Len())
	assert.Equal(t, 0, result.Routines.Len())
}

func TestParseSQL_FunctionNameTooManyParts(t *testing.T) {
	_, err := parseSQLWithPublicSchema(`CREATE FUNCTION a.b.c() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;`)
	assert.ErrorContains(t, err, "unexpected routine name: a.b.c")
}

// A SET value can be a number as well as a string, and COST can be
// fractional. Each lands in a different node type.
func TestParseSQL_FunctionNumericValues(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f() RETURNS integer
		    LANGUAGE sql
		    COST 1.5
		    SET statement_timeout TO 100
		    SET seq_page_cost TO 1.5
		    AS $$ SELECT 1 $$;
	`)
	require.NoError(t, err)

	r, _ := result.Routines.GetOk("public.f()")
	require.NotNil(t, r.Cost)
	assert.InDelta(t, 1.5, *r.Cost, 0)
	assert.Equal(t, []*model.RoutineConfig{
		{Name: "statement_timeout", Args: []string{"100"}},
		{Name: "seq_page_cost", Args: []string{"1.5"}},
	}, r.Config)
}

// LANGUAGE c defaults to COST 1 rather than 100, so spelling that out is not
// a change.
func TestParseSQL_FunctionLanguageCDefaultCost(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f() RETURNS integer LANGUAGE c COST 1 AS 'obj', 'sym';
	`)
	require.NoError(t, err)

	r, _ := result.Routines.GetOk("public.f()")
	assert.Nil(t, r.Cost)
}

func TestParseSQL_CommentOnRoutineIsNull(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
		COMMENT ON FUNCTION public.f() IS 'v1';
		COMMENT ON FUNCTION public.f() IS NULL;
	`)
	require.NoError(t, err)

	r, _ := result.Routines.GetOk("public.f()")
	assert.Nil(t, r.Comment)
}

// A bare name has to skip the routines it does not name before it can tell
// whether the one it does name is unique.
func TestParseSQL_CommentOnRoutineWithoutArgsAmongOthers(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.other(a integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$;
		CREATE FUNCTION public.f(a integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$;
		COMMENT ON FUNCTION public.f IS 'only one';
	`)
	require.NoError(t, err)

	r, _ := result.Routines.GetOk("public.f(integer)")
	require.NotNil(t, r.Comment)
	assert.Equal(t, "only one", *r.Comment)

	other, _ := result.Routines.GetOk("public.other(integer)")
	assert.Nil(t, other.Comment)
}

func TestParseSQL_CommentOnRoutineWithBadName(t *testing.T) {
	// The comment names no routine the parse result holds, so it is dropped
	// rather than failing the parse.
	result, err := parseSQLWithPublicSchema(`
		CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
		COMMENT ON FUNCTION a.b.c() IS 'nope';
	`)
	require.NoError(t, err)

	r, _ := result.Routines.GetOk("public.f()")
	assert.Nil(t, r.Comment)
}
