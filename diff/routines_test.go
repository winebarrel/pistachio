package diff_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/model"
)

func newRoutineMap(routines ...*model.Routine) *orderedmap.Map[string, *model.Routine] {
	m := orderedmap.New[string, *model.Routine]()
	for _, r := range routines {
		m.Set(r.FQRN(), r)
	}
	return m
}

// newRoutine builds a minimal function: one integer parameter, an integer
// result and a SQL body. Each test mutates the fields it cares about.
func newRoutine(mutate ...func(*model.Routine)) *model.Routine {
	r := &model.Routine{
		Schema:     "public",
		Name:       "f",
		Args:       []*model.RoutineArg{{Name: "a", Type: "integer"}},
		ReturnType: "integer",
		Language:   "sql",
		Body:       " SELECT a ",
		Volatility: model.VolatilityVolatile,
		Parallel:   model.ParallelUnsafe,
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

func ptr[T any](v T) *T { return &v }

func TestDiffRoutines_CreateNew(t *testing.T) {
	result, err := diff.DiffRoutines(newRoutineMap(), newRoutineMap(newRoutine()), diff.AllowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE OR REPLACE FUNCTION public.f(a integer)")
	assert.Empty(t, result.DropStmts)
}

func TestDiffRoutines_CreateNewWithComment(t *testing.T) {
	desired := newRoutine(func(r *model.Routine) { r.Comment = ptr("hi") })
	result, err := diff.DiffRoutines(newRoutineMap(), newRoutineMap(desired), diff.AllowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 2)
	assert.Equal(t, "COMMENT ON FUNCTION public.f(integer) IS 'hi';", result.Stmts[1])
}

func TestDiffRoutines_Unchanged(t *testing.T) {
	result, err := diff.DiffRoutines(newRoutineMap(newRoutine()), newRoutineMap(newRoutine()), diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
	assert.Empty(t, result.DisallowedDropStmts)
}

// A change PostgreSQL can apply in place replaces without a drop, even when
// the drop policy would have allowed one.
func TestDiffRoutines_ReplaceInPlace(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*model.Routine)
	}{
		{"body", func(r *model.Routine) { r.Body = " SELECT a + 1 " }},
		{"language", func(r *model.Routine) { r.Language = "plpgsql"; r.Body = " BEGIN RETURN a; END " }},
		{"volatility", func(r *model.Routine) { r.Volatility = "IMMUTABLE" }},
		{"strict", func(r *model.Routine) { r.Strict = true }},
		{"security definer", func(r *model.Routine) { r.SecurityDefiner = true }},
		{"leakproof", func(r *model.Routine) { r.Leakproof = true }},
		{"parallel", func(r *model.Routine) { r.Parallel = "SAFE" }},
		{"cost", func(r *model.Routine) { r.Cost = ptr(5.0) }},
		{"set config", func(r *model.Routine) {
			r.Config = []*model.RoutineConfig{{Name: "search_path", Args: []string{"public"}}}
		}},
		{"default added", func(r *model.Routine) { r.Args[0].Default = "1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := diff.DiffRoutines(newRoutineMap(newRoutine()), newRoutineMap(newRoutine(tc.mutate)), diff.AllowAllDrops{})
			require.NoError(t, err)
			require.Len(t, result.Stmts, 1)
			assert.Contains(t, result.Stmts[0], "CREATE OR REPLACE FUNCTION")
			assert.Empty(t, result.DropStmts)
			assert.Empty(t, result.DisallowedDropStmts)
		})
	}
}

// A change PostgreSQL refuses in place runs as a drop and a create.
func TestDiffRoutines_DropAndCreate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current func(*model.Routine)
		desired func(*model.Routine)
	}{
		{"return type", func(*model.Routine) {}, func(r *model.Routine) { r.ReturnType = "bigint" }},
		{"returns set", func(*model.Routine) {}, func(r *model.Routine) { r.ReturnsSet = true }},
		{"kind", func(*model.Routine) {}, func(r *model.Routine) { r.Procedure = true; r.ReturnType = "" }},
		{"parameter name", func(*model.Routine) {}, func(r *model.Routine) { r.Args[0].Name = "b" }},
		{"default removed", func(r *model.Routine) { r.Args[0].Default = "1" }, func(*model.Routine) {}},
		{"out parameter added", func(*model.Routine) {}, func(r *model.Routine) {
			r.Args = append(r.Args, &model.RoutineArg{Mode: "OUT", Name: "o", Type: "text"})
		}},
		{"table column renamed", func(r *model.Routine) {
			r.ReturnType = "record"
			r.ReturnsSet = true
			r.Args = append(r.Args, &model.RoutineArg{Mode: "TABLE", Name: "x", Type: "integer"})
		}, func(r *model.Routine) {
			r.ReturnType = "record"
			r.ReturnsSet = true
			r.Args = append(r.Args, &model.RoutineArg{Mode: "TABLE", Name: "y", Type: "integer"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := newRoutineMap(newRoutine(tc.current))
			desired := newRoutineMap(newRoutine(tc.desired))
			result, err := diff.DiffRoutines(current, desired, diff.AllowAllDrops{})
			require.NoError(t, err)
			require.Len(t, result.Stmts, 2)
			assert.Equal(t, "DROP FUNCTION public.f(integer);", result.Stmts[0])
			assert.Contains(t, result.Stmts[1], "CREATE OR REPLACE ")
			assert.Empty(t, result.DisallowedDropStmts)
		})
	}
}

// A recreate loses the routine's comment, so the comment is re-applied with it.
func TestDiffRoutines_DropAndCreateReappliesComment(t *testing.T) {
	current := newRoutineMap(newRoutine(func(r *model.Routine) { r.Comment = ptr("v1") }))
	desired := newRoutineMap(newRoutine(func(r *model.Routine) {
		r.ReturnType = "bigint"
		r.Comment = ptr("v1")
	}))
	result, err := diff.DiffRoutines(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 3)
	assert.Equal(t, "COMMENT ON FUNCTION public.f(integer) IS 'v1';", result.Stmts[2])
}

// Without the drop policy a recreate is reported and the current definition
// is left in place.
func TestDiffRoutines_DropAndCreateDisallowed(t *testing.T) {
	current := newRoutineMap(newRoutine())
	desired := newRoutineMap(newRoutine(func(r *model.Routine) { r.ReturnType = "bigint" }))
	result, err := diff.DiffRoutines(current, desired, diff.DenyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Equal(t, []string{"-- skipped: DROP FUNCTION public.f(integer);"}, result.DisallowedDropStmts)
}

func TestDiffRoutines_CommentChanges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current *string
		desired *string
		want    string
	}{
		{"added", nil, ptr("v1"), "COMMENT ON FUNCTION public.f(integer) IS 'v1';"},
		{"changed", ptr("v1"), ptr("v2"), "COMMENT ON FUNCTION public.f(integer) IS 'v2';"},
		{"removed", ptr("v1"), nil, "COMMENT ON FUNCTION public.f(integer) IS NULL;"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := newRoutineMap(newRoutine(func(r *model.Routine) { r.Comment = tc.current }))
			desired := newRoutineMap(newRoutine(func(r *model.Routine) { r.Comment = tc.desired }))
			result, err := diff.DiffRoutines(current, desired, diff.AllowAllDrops{})
			require.NoError(t, err)
			assert.Equal(t, []string{tc.want}, result.Stmts)
		})
	}
}

func TestDiffRoutines_Drop(t *testing.T) {
	current := newRoutineMap(
		newRoutine(),
		newRoutine(func(r *model.Routine) { r.Name = "p"; r.Procedure = true; r.ReturnType = ""; r.Args = nil }),
	)
	result, err := diff.DiffRoutines(current, newRoutineMap(), diff.AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP FUNCTION public.f(integer);", "DROP PROCEDURE public.p();"}, result.DropStmts)
	assert.Empty(t, result.DisallowedDropStmts)
}

func TestDiffRoutines_DropDisallowed(t *testing.T) {
	result, err := diff.DiffRoutines(newRoutineMap(newRoutine()), newRoutineMap(), diff.DenyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Equal(t, []string{"-- skipped: DROP FUNCTION public.f(integer);"}, result.DisallowedDropStmts)
}

// A nil DropChecker denies every drop rather than panicking.
func TestDiffRoutines_NilDropChecker(t *testing.T) {
	result, err := diff.DiffRoutines(newRoutineMap(newRoutine()), newRoutineMap(), nil)
	require.NoError(t, err)
	assert.Empty(t, result.DropStmts)
	assert.Len(t, result.DisallowedDropStmts, 1)
}

// An overload is an independent object: changing one argument type drops the
// old routine and creates a new one.
func TestDiffRoutines_ArgTypeChangeIsDropAndCreate(t *testing.T) {
	current := newRoutineMap(newRoutine())
	desired := newRoutineMap(newRoutine(func(r *model.Routine) {
		r.Args[0].Type = "bigint"
	}))
	result, err := diff.DiffRoutines(current, desired, diff.AllowAllDrops{})
	require.NoError(t, err)
	require.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE OR REPLACE FUNCTION public.f(a bigint)")
	assert.Equal(t, []string{"DROP FUNCTION public.f(integer);"}, result.DropStmts)
}
