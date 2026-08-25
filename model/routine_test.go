package model_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func TestDollarQuote(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"plain", " SELECT 1 ", "$$ SELECT 1 $$"},
		{"empty", "", "$$$$"},
		{"contains the default tag", " SELECT '$$' ", "$_$ SELECT '$$' $_$"},
		{"contains the next tag too", " $$ $_$ ", "$__$ $$ $_$ $__$"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, model.DollarQuote(tc.body))
		})
	}
}

// The identity argument list leaves out OUT and TABLE parameters, the way
// pg_get_function_identity_arguments does.
func TestRoutine_FQRN(t *testing.T) {
	r := model.Routine{
		Schema: "public",
		Name:   "f",
		Args: []*model.RoutineArg{
			{Name: "a", Type: "integer"},
			{Mode: "INOUT", Name: "b", Type: "text"},
			{Mode: "VARIADIC", Name: "c", Type: "integer[]"},
			{Mode: "OUT", Name: "d", Type: "bigint"},
			{Mode: "TABLE", Name: "e", Type: "boolean"},
		},
	}
	assert.Equal(t, "public.f(integer, text, integer[])", r.FQRN())
}

func TestRoutine_SQL(t *testing.T) {
	cost := 5.0
	rows := 7.0
	r := model.Routine{
		Schema:          "public",
		Name:            "f",
		Args:            []*model.RoutineArg{{Name: "a", Type: "integer", Default: "1"}},
		ReturnType:      "integer",
		ReturnsSet:      true,
		Language:        "sql",
		Body:            " SELECT a ",
		Volatility:      "IMMUTABLE",
		Strict:          true,
		SecurityDefiner: true,
		Leakproof:       true,
		Parallel:        "SAFE",
		Cost:            &cost,
		Rows:            &rows,
		Config:          []*model.RoutineConfig{{Name: "search_path", Args: []string{"public"}}},
	}
	assert.Equal(t, strings.Join([]string{
		"CREATE OR REPLACE FUNCTION public.f(a integer DEFAULT 1)",
		"    RETURNS SETOF integer",
		"    LANGUAGE sql",
		"    IMMUTABLE STRICT SECURITY DEFINER LEAKPROOF PARALLEL SAFE COST 5 ROWS 7",
		"    SET search_path TO public",
		"    AS $$ SELECT a $$;",
	}, "\n"), r.SQL())
}

// The attributes PostgreSQL treats as defaults are left out, matching what
// pg_get_functiondef prints.
func TestRoutine_SQLOmitsDefaultAttributes(t *testing.T) {
	r := model.Routine{
		Schema: "public", Name: "f", ReturnType: "integer", Language: "sql", Body: " SELECT 1 ",
		Volatility: model.VolatilityVolatile,
		Parallel:   model.ParallelUnsafe,
	}
	assert.NotContains(t, r.SQL(), "VOLATILE")
	assert.NotContains(t, r.SQL(), "PARALLEL")
}

func TestRoutine_SQLProcedure(t *testing.T) {
	r := model.Routine{
		Schema: "public", Name: "p", Procedure: true, Language: "sql", Body: " SELECT ",
		Args: []*model.RoutineArg{{Mode: "INOUT", Name: "x", Type: "integer"}},
	}
	assert.Equal(t, strings.Join([]string{
		"CREATE OR REPLACE PROCEDURE public.p(INOUT x integer)",
		"    LANGUAGE sql",
		"    AS $$ SELECT $$;",
	}, "\n"), r.SQL())
	assert.Equal(t, "DROP PROCEDURE public.p(integer);", r.DropSQL())
}

func TestRoutine_SQLReturnsTable(t *testing.T) {
	r := model.Routine{
		Schema: "public", Name: "f", ReturnType: "record", ReturnsSet: true,
		Language: "sql", Body: " SELECT 1, 'x' ",
		Args: []*model.RoutineArg{
			{Mode: "TABLE", Name: "id", Type: "integer"},
			{Mode: "TABLE", Name: "name", Type: "text"},
		},
	}
	assert.Contains(t, r.SQL(), "CREATE OR REPLACE FUNCTION public.f()\n    RETURNS TABLE(id integer, name text)")
}

// LANGUAGE c names an object file and a link symbol rather than a body.
func TestRoutine_SQLLanguageC(t *testing.T) {
	r := model.Routine{
		Schema: "public", Name: "f", ReturnType: "integer",
		Language: "c", ObjFile: "obj", Body: "sym",
	}
	assert.Contains(t, r.SQL(), "AS 'obj', 'sym';")
}

// A SET value is written bare when it is an identifier and as a literal
// otherwise. Double quotes would read as an identifier, which 64MB is not.
func TestRoutineConfig_SQL(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config model.RoutineConfig
		want   string
	}{
		{"identifiers", model.RoutineConfig{Name: "search_path", Args: []string{"public", "pg_temp"}}, "SET search_path TO public, pg_temp"},
		{"literal", model.RoutineConfig{Name: "work_mem", Args: []string{"64MB"}}, "SET work_mem TO '64MB'"},
		{"number", model.RoutineConfig{Name: "statement_timeout", Args: []string{"100"}}, "SET statement_timeout TO '100'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.config.SQL())
		})
	}
}

func TestRoutineToSQL(t *testing.T) {
	comment := "v1"
	r := &model.Routine{
		Schema: "public", Name: "f", ReturnType: "integer", Language: "sql", Body: " SELECT 1 ",
		Comment: &comment,
	}
	out := model.RoutineToSQL(r)
	assert.True(t, strings.HasPrefix(out, "-- public.f()\n"))
	assert.Contains(t, out, "COMMENT ON FUNCTION public.f() IS 'v1';")
}

// A procedure has no result, and neither does a routine whose model carries
// no return type.
func TestRoutine_SQLWithoutReturnType(t *testing.T) {
	r := model.Routine{Schema: "public", Name: "f", Language: "sql", Body: " SELECT 1 "}
	assert.NotContains(t, r.SQL(), "RETURNS")
}

func TestRoutine_String(t *testing.T) {
	r := model.Routine{Schema: "public", Name: "f", ReturnType: "integer"}
	assert.Contains(t, r.String(), `Name:"f"`)
}

// A routine with no comment renders none, so the header and the CREATE are
// all RoutineToSQL writes.
func TestRoutineToSQL_NoComment(t *testing.T) {
	r := &model.Routine{
		Schema: "public", Name: "f", ReturnType: "integer", Language: "sql", Body: " SELECT 1 ",
	}
	assert.Empty(t, r.CommentSQL())
	assert.NotContains(t, model.RoutineToSQL(r), "COMMENT ON")
}

func TestRoutinesToSQL(t *testing.T) {
	routines := orderedmap.New[string, *model.Routine]()
	for _, r := range []*model.Routine{
		{
			Schema: "public", Name: "f", ReturnType: "integer", Language: "sql", Body: " SELECT a ",
			Args: []*model.RoutineArg{{Name: "a", Type: "integer"}},
		},
		{
			Schema: "public", Name: "f", ReturnType: "text", Language: "sql", Body: " SELECT a ",
			Args: []*model.RoutineArg{{Name: "a", Type: "text"}},
		},
		{Schema: "public", Name: "p", Procedure: true, Language: "sql", Body: " SELECT "},
	} {
		routines.Set(r.FQRN(), r)
	}

	got := model.RoutinesToSQL(routines)
	// Overloads are separate objects, each with its own header.
	assert.Contains(t, got, "-- public.f(integer)\nCREATE OR REPLACE FUNCTION public.f(a integer)")
	assert.Contains(t, got, "-- public.f(text)\nCREATE OR REPLACE FUNCTION public.f(a text)")
	assert.Contains(t, got, "-- public.p()\nCREATE OR REPLACE PROCEDURE public.p()")
	assert.Equal(t, 2, strings.Count(got, "\n\n"), "objects are separated by a blank line")
}
