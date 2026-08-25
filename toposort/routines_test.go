package toposort_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/toposort"
)

func routineMap(routines ...*model.Routine) *orderedmap.Map[string, *model.Routine] {
	m := orderedmap.New[string, *model.Routine]()
	for _, r := range routines {
		m.Set(r.FQRN(), r)
	}
	return m
}

func orderWithRoutines(
	t *testing.T,
	enums *orderedmap.Map[string, *model.Enum],
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
	routines *orderedmap.Map[string, *model.Routine],
) []string {
	t.Helper()
	order, err := toposort.OrderFromSchema(
		enums,
		orderedmap.New[string, *model.Domain](),
		orderedmap.New[string, *model.CompositeType](),
		tables, views,
		orderedmap.New[string, *model.Sequence](),
		routines,
	)
	require.NoError(t, err)
	return order
}

func indexOf(t *testing.T, order []string, name string) int {
	t.Helper()
	i := slices.Index(order, name)
	require.GreaterOrEqual(t, i, 0, "%q not in %v", name, order)
	return i
}

// A routine is created after the types its signature names and before every
// table, because a CHECK constraint, a GENERATED expression, an index
// expression, a policy or a trigger can call it.
func TestOrderFromSchema_RoutineBetweenTypesAndTables(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	enums.Set("public.zzz_status", &model.Enum{Schema: "public", Name: "zzz_status", Values: []string{"active"}})

	tables := orderedmap.New[string, *model.Table]()
	tables.Set("public.aaa_users", &model.Table{Schema: "public", Name: "aaa_users", Columns: orderedmap.New[string, *model.Column]()})

	routines := routineMap(&model.Routine{
		Schema: "public", Name: "mmm_label", ReturnType: "text", Language: "sql",
		Args: []*model.RoutineArg{{Name: "s", Type: "public.zzz_status"}},
	})

	order := orderWithRoutines(t, enums, tables, orderedmap.New[string, *model.View](), routines)

	enumPos := indexOf(t, order, "public.zzz_status")
	routinePos := indexOf(t, order, "routine:public.mmm_label")
	tablePos := indexOf(t, order, "public.aaa_users")

	// The names are chosen so that plain alphabetical order would put the
	// table first and the enum last: only the edges give the right order.
	assert.Less(t, enumPos, routinePos, "the enum must come before the routine")
	assert.Less(t, routinePos, tablePos, "the routine must come before the table")
}

func TestOrderFromSchema_RoutineBeforeViews(t *testing.T) {
	views := orderedmap.New[string, *model.View]()
	views.Set("public.aaa_v", &model.View{Schema: "public", Name: "aaa_v", Definition: "SELECT 1"})

	routines := routineMap(&model.Routine{Schema: "public", Name: "zzz_f", ReturnType: "integer", Language: "sql"})

	order := orderWithRoutines(t, orderedmap.New[string, *model.Enum](), orderedmap.New[string, *model.Table](), views, routines)
	assert.Less(t, indexOf(t, order, "routine:public.zzz_f"), indexOf(t, order, "public.aaa_v"))
}

// Overloads share one node, so an overload set is ordered as a unit.
func TestOrderFromSchema_OverloadsShareANode(t *testing.T) {
	routines := routineMap(
		&model.Routine{Schema: "public", Name: "f", ReturnType: "integer", Language: "sql", Args: []*model.RoutineArg{{Name: "a", Type: "integer"}}},
		&model.Routine{Schema: "public", Name: "f", ReturnType: "text", Language: "sql", Args: []*model.RoutineArg{{Name: "a", Type: "text"}}},
	)

	order := orderWithRoutines(t, orderedmap.New[string, *model.Enum](), orderedmap.New[string, *model.Table](), orderedmap.New[string, *model.View](), routines)
	assert.Equal(t, []string{"routine:public.f"}, order)
}

// A table and a function may share a name. The prefix keeps them apart, so
// the graph does not fold them into one node and read as a cycle.
func TestOrderFromSchema_RoutineAndTableMayShareAName(t *testing.T) {
	tables := orderedmap.New[string, *model.Table]()
	tables.Set("public.f", &model.Table{Schema: "public", Name: "f", Columns: orderedmap.New[string, *model.Column]()})

	routines := routineMap(&model.Routine{Schema: "public", Name: "f", ReturnType: "integer", Language: "sql"})

	order := orderWithRoutines(t, orderedmap.New[string, *model.Enum](), tables, orderedmap.New[string, *model.View](), routines)
	assert.Equal(t, []string{"routine:public.f", "public.f"}, order)
}

func TestRoutineNode(t *testing.T) {
	assert.Equal(t, "routine:public.f", toposort.RoutineNode("public.f"))
	assert.Empty(t, toposort.RoutineNode(""))
}

// A routine's return type is an edge too, not only its parameters.
func TestOrderFromSchema_RoutineDependsOnReturnType(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	enums.Set("public.zzz_status", &model.Enum{Schema: "public", Name: "zzz_status", Values: []string{"active"}})

	routines := routineMap(&model.Routine{
		Schema: "public", Name: "aaa_f", ReturnType: "public.zzz_status", Language: "sql",
	})

	order := orderWithRoutines(t, enums, orderedmap.New[string, *model.Table](), orderedmap.New[string, *model.View](), routines)
	assert.Less(t, indexOf(t, order, "public.zzz_status"), indexOf(t, order, "routine:public.aaa_f"))
}
