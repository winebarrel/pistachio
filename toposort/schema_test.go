package toposort_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/toposort"
)

func TestOrderFromSchema_Basic(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	enums.Set("public.status", &model.Enum{Schema: "public", Name: "status", Values: []string{"active", "inactive"}})

	domains := orderedmap.New[string, *model.Domain]()
	domains.Set("public.user_status", &model.Domain{Schema: "public", Name: "user_status", BaseType: "public.status"})

	tables := orderedmap.New[string, *model.Table]()
	users := &model.Table{Schema: "public", Name: "users"}
	users.Columns = orderedmap.New[string, *model.Column]()
	users.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	users.Columns.Set("status", &model.Column{Name: "status", TypeName: "public.user_status"})
	users.Indexes = orderedmap.New[string, *model.Index]()
	users.Constraints = orderedmap.New[string, *model.Constraint]()
	users.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set("public.users", users)

	views := orderedmap.New[string, *model.View]()
	views.Set("public.active_users", &model.View{
		Schema:     "public",
		Name:       "active_users",
		Definition: "SELECT id, status FROM public.users WHERE status = 'active'",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.status"], idx["public.user_status"], "enum before domain")
	assert.Less(t, idx["public.user_status"], idx["public.users"], "domain before table")
	assert.Less(t, idx["public.users"], idx["public.active_users"], "table before view")
}

func TestOrderFromSchema_ForeignKey(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()
	views := orderedmap.New[string, *model.View]()

	tables := orderedmap.New[string, *model.Table]()

	refTable := "users"
	refSchema := "public"

	posts := &model.Table{Schema: "public", Name: "posts"}
	posts.Columns = orderedmap.New[string, *model.Column]()
	posts.Indexes = orderedmap.New[string, *model.Index]()
	posts.Constraints = orderedmap.New[string, *model.Constraint]()
	posts.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	posts.ForeignKeys.Set("posts_user_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "posts_user_fk"},
		RefSchema:  &refSchema,
		RefTable:   &refTable,
	})
	tables.Set("public.posts", posts)

	users := &model.Table{Schema: "public", Name: "users"}
	users.Columns = orderedmap.New[string, *model.Column]()
	users.Indexes = orderedmap.New[string, *model.Index]()
	users.Constraints = orderedmap.New[string, *model.Constraint]()
	users.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set("public.users", users)

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.posts"], "FK target before FK source")
}

func TestOrderFromSchema_ViewToView(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	users := &model.Table{Schema: "public", Name: "users"}
	users.Columns = orderedmap.New[string, *model.Column]()
	users.Indexes = orderedmap.New[string, *model.Index]()
	users.Constraints = orderedmap.New[string, *model.Constraint]()
	users.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set("public.users", users)

	views := orderedmap.New[string, *model.View]()
	views.Set("public.active_users", &model.View{
		Schema:     "public",
		Name:       "active_users",
		Definition: "SELECT id FROM public.users",
	})
	views.Set("public.active_user_count", &model.View{
		Schema:     "public",
		Name:       "active_user_count",
		Definition: "SELECT count(*) FROM public.active_users",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.active_users"], "table before view")
	assert.Less(t, idx["public.active_users"], idx["public.active_user_count"], "view before dependent view")
}

func TestOrderFromSchema_Empty(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()
	tables := orderedmap.New[string, *model.Table]()
	views := orderedmap.New[string, *model.View]()

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)
	assert.Empty(t, order)
}

func TestOrderFromSchema_Independent(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()
	views := orderedmap.New[string, *model.View]()

	tables := orderedmap.New[string, *model.Table]()
	for _, name := range []string{"c_table", "a_table", "b_table"} {
		tbl := &model.Table{Schema: "public", Name: name}
		tbl.Columns = orderedmap.New[string, *model.Column]()
		tbl.Indexes = orderedmap.New[string, *model.Index]()
		tbl.Constraints = orderedmap.New[string, *model.Constraint]()
		tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
		tables.Set("public."+name, tbl)
	}

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)
	// Independent objects should be sorted alphabetically
	assert.Equal(t, []string{"public.a_table", "public.b_table", "public.c_table"}, order)
}

func TestOrderFromSchema_ArrayColumnType(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	enums.Set("public.status", &model.Enum{Schema: "public", Name: "status", Values: []string{"a", "b"}})

	domains := orderedmap.New[string, *model.Domain]()
	views := orderedmap.New[string, *model.View]()

	tables := orderedmap.New[string, *model.Table]()
	tbl := &model.Table{Schema: "public", Name: "users"}
	tbl.Columns = orderedmap.New[string, *model.Column]()
	tbl.Columns.Set("statuses", &model.Column{Name: "statuses", TypeName: "public.status[]"})
	tbl.Indexes = orderedmap.New[string, *model.Index]()
	tbl.Constraints = orderedmap.New[string, *model.Constraint]()
	tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set("public.users", tbl)

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.status"], idx["public.users"], "enum before table with array type dep")
}

func TestOrderFromSchema_CyclicFK(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()
	views := orderedmap.New[string, *model.View]()

	refA := "a"
	refB := "b"
	schemaPublic := "public"

	tables := orderedmap.New[string, *model.Table]()

	tblA := &model.Table{Schema: "public", Name: "a"}
	tblA.Columns = orderedmap.New[string, *model.Column]()
	tblA.Indexes = orderedmap.New[string, *model.Index]()
	tblA.Constraints = orderedmap.New[string, *model.Constraint]()
	tblA.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tblA.ForeignKeys.Set("a_b_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "a_b_fk"},
		RefSchema:  &schemaPublic,
		RefTable:   &refB,
	})
	tables.Set("public.a", tblA)

	tblB := &model.Table{Schema: "public", Name: "b"}
	tblB.Columns = orderedmap.New[string, *model.Column]()
	tblB.Indexes = orderedmap.New[string, *model.Index]()
	tblB.Constraints = orderedmap.New[string, *model.Constraint]()
	tblB.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tblB.ForeignKeys.Set("b_a_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "b_a_fk"},
		RefSchema:  &schemaPublic,
		RefTable:   &refA,
	})
	tables.Set("public.b", tblB)

	_, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle detected")
}

func TestOrderFromSchema_UnqualifiedDomainBaseType(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	enums.Set("public.status", &model.Enum{Schema: "public", Name: "status", Values: []string{"a", "b"}})

	domains := orderedmap.New[string, *model.Domain]()
	// Domain with unqualified base type "status" (not "public.status")
	domains.Set("public.user_status", &model.Domain{Schema: "public", Name: "user_status", BaseType: "status"})

	tables := orderedmap.New[string, *model.Table]()
	views := orderedmap.New[string, *model.View]()

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.status"], idx["public.user_status"], "enum before domain with unqualified base type")
}

func TestOrderFromSchema_PartitionChild(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()
	views := orderedmap.New[string, *model.View]()

	parentFQTN := "public.events"

	tables := orderedmap.New[string, *model.Table]()

	parent := &model.Table{Schema: "public", Name: "events", Partitioned: true}
	parent.Columns = orderedmap.New[string, *model.Column]()
	parent.Indexes = orderedmap.New[string, *model.Index]()
	parent.Constraints = orderedmap.New[string, *model.Constraint]()
	parent.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set("public.events", parent)

	child := &model.Table{Schema: "public", Name: "events_2024", PartitionOf: &parentFQTN}
	child.Columns = orderedmap.New[string, *model.Column]()
	child.Indexes = orderedmap.New[string, *model.Index]()
	child.Constraints = orderedmap.New[string, *model.Constraint]()
	child.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set("public.events_2024", child)

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.events"], idx["public.events_2024"], "partition parent before child")
}

func TestOrderFromSchema_FKWithDefaultSchema(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()
	views := orderedmap.New[string, *model.View]()

	// FK with nil RefSchema (should default to "public")
	refTable := "users"

	tables := orderedmap.New[string, *model.Table]()
	users := &model.Table{Schema: "public", Name: "users"}
	users.Columns = orderedmap.New[string, *model.Column]()
	users.Indexes = orderedmap.New[string, *model.Index]()
	users.Constraints = orderedmap.New[string, *model.Constraint]()
	users.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set("public.users", users)

	posts := &model.Table{Schema: "public", Name: "posts"}
	posts.Columns = orderedmap.New[string, *model.Column]()
	posts.Indexes = orderedmap.New[string, *model.Index]()
	posts.Constraints = orderedmap.New[string, *model.Constraint]()
	posts.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	posts.ForeignKeys.Set("posts_user_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "posts_user_fk"},
		RefSchema:  nil, // nil schema → defaults to "public"
		RefTable:   &refTable,
	})
	tables.Set("public.posts", posts)

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.posts"], "FK target before source with nil schema")
}

func TestOrderFromSchema_NonPublicSchema(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	enums.Set("myapp.status", &model.Enum{Schema: "myapp", Name: "status", Values: []string{"a", "b"}})

	domains := orderedmap.New[string, *model.Domain]()
	// Unqualified base type "status" should resolve using domain's schema "myapp"
	domains.Set("myapp.user_status", &model.Domain{Schema: "myapp", Name: "user_status", BaseType: "status"})

	tables := orderedmap.New[string, *model.Table]()
	views := orderedmap.New[string, *model.View]()

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["myapp.status"], idx["myapp.user_status"], "enum before domain in non-public schema")
}

func TestOrderFromSchema_ViewWithSchemalessTableRef(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	users := &model.Table{Schema: "public", Name: "users"}
	users.Columns = orderedmap.New[string, *model.Column]()
	users.Indexes = orderedmap.New[string, *model.Index]()
	users.Constraints = orderedmap.New[string, *model.Constraint]()
	users.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set("public.users", users)

	views := orderedmap.New[string, *model.View]()
	// View definition without schema prefix (as PostgreSQL catalog returns)
	views.Set("public.v", &model.View{
		Schema:     "public",
		Name:       "v",
		Definition: "SELECT users.id FROM users",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.v"], "table before view with schemaless ref")
}

func TestOrderFromSchema_ViewWithCTE(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	users := &model.Table{Schema: "public", Name: "users"}
	users.Columns = orderedmap.New[string, *model.Column]()
	users.Indexes = orderedmap.New[string, *model.Index]()
	users.Constraints = orderedmap.New[string, *model.Constraint]()
	users.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set("public.users", users)

	views := orderedmap.New[string, *model.View]()
	views.Set("public.cte_view", &model.View{
		Schema:     "public",
		Name:       "cte_view",
		Definition: "WITH active AS (SELECT id FROM users WHERE id > 0) SELECT id FROM active",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.cte_view"], "table before view with CTE")
}

func TestOrderFromSchema_ViewWithHavingSubquery(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	for _, name := range []string{"orders", "thresholds"} {
		tbl := &model.Table{Schema: "public", Name: name}
		tbl.Columns = orderedmap.New[string, *model.Column]()
		tbl.Indexes = orderedmap.New[string, *model.Index]()
		tbl.Constraints = orderedmap.New[string, *model.Constraint]()
		tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
		tables.Set("public."+name, tbl)
	}

	views := orderedmap.New[string, *model.View]()
	views.Set("public.big_orders", &model.View{
		Schema:     "public",
		Name:       "big_orders",
		Definition: "SELECT user_id, count(*) FROM orders GROUP BY user_id HAVING count(*) > (SELECT min(val) FROM thresholds)",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.orders"], idx["public.big_orders"], "orders before view")
	assert.Less(t, idx["public.thresholds"], idx["public.big_orders"], "thresholds before view (HAVING subquery)")
}

func TestOrderFromSchema_ViewWithScalarSubquery(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	for _, name := range []string{"users", "settings"} {
		tbl := &model.Table{Schema: "public", Name: name}
		tbl.Columns = orderedmap.New[string, *model.Column]()
		tbl.Indexes = orderedmap.New[string, *model.Index]()
		tbl.Constraints = orderedmap.New[string, *model.Constraint]()
		tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
		tables.Set("public."+name, tbl)
	}

	views := orderedmap.New[string, *model.View]()
	views.Set("public.user_with_setting", &model.View{
		Schema:     "public",
		Name:       "user_with_setting",
		Definition: "SELECT id, (SELECT val FROM settings LIMIT 1) AS setting FROM users",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.user_with_setting"], "users before view")
	assert.Less(t, idx["public.settings"], idx["public.user_with_setting"], "settings before view (scalar subquery)")
}

func TestOrderFromSchema_ViewWithExistsSubquery(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	for _, name := range []string{"users", "orders"} {
		tbl := &model.Table{Schema: "public", Name: name}
		tbl.Columns = orderedmap.New[string, *model.Column]()
		tbl.Indexes = orderedmap.New[string, *model.Index]()
		tbl.Constraints = orderedmap.New[string, *model.Constraint]()
		tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
		tables.Set("public."+name, tbl)
	}

	views := orderedmap.New[string, *model.View]()
	views.Set("public.users_with_orders", &model.View{
		Schema:     "public",
		Name:       "users_with_orders",
		Definition: "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.users_with_orders"])
	assert.Less(t, idx["public.orders"], idx["public.users_with_orders"])
}

func TestOrderFromSchema_ViewWithCoalesce(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	for _, name := range []string{"users", "defaults"} {
		tbl := &model.Table{Schema: "public", Name: name}
		tbl.Columns = orderedmap.New[string, *model.Column]()
		tbl.Indexes = orderedmap.New[string, *model.Index]()
		tbl.Constraints = orderedmap.New[string, *model.Constraint]()
		tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
		tables.Set("public."+name, tbl)
	}

	views := orderedmap.New[string, *model.View]()
	views.Set("public.v", &model.View{
		Schema:     "public",
		Name:       "v",
		Definition: "SELECT id, COALESCE(name, (SELECT val FROM defaults LIMIT 1)) FROM users",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.v"])
	assert.Less(t, idx["public.defaults"], idx["public.v"])
}

func TestOrderFromSchema_ViewWithCaseSubquery(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	for _, name := range []string{"users", "config"} {
		tbl := &model.Table{Schema: "public", Name: name}
		tbl.Columns = orderedmap.New[string, *model.Column]()
		tbl.Indexes = orderedmap.New[string, *model.Index]()
		tbl.Constraints = orderedmap.New[string, *model.Constraint]()
		tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
		tables.Set("public."+name, tbl)
	}

	views := orderedmap.New[string, *model.View]()
	views.Set("public.v", &model.View{
		Schema:     "public",
		Name:       "v",
		Definition: "SELECT id, CASE WHEN active THEN (SELECT val FROM config LIMIT 1) ELSE 'none' END FROM users",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.v"])
	assert.Less(t, idx["public.config"], idx["public.v"])
}

func TestOrderFromSchema_ViewWithFuncCallSubquery(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	for _, name := range []string{"users", "roles"} {
		tbl := &model.Table{Schema: "public", Name: name}
		tbl.Columns = orderedmap.New[string, *model.Column]()
		tbl.Indexes = orderedmap.New[string, *model.Index]()
		tbl.Constraints = orderedmap.New[string, *model.Constraint]()
		tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
		tables.Set("public."+name, tbl)
	}

	views := orderedmap.New[string, *model.View]()
	views.Set("public.v", &model.View{
		Schema:     "public",
		Name:       "v",
		Definition: "SELECT id, array_agg((SELECT name FROM roles WHERE roles.user_id = users.id)) FROM users",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.v"])
	assert.Less(t, idx["public.roles"], idx["public.v"])
}

func TestOrderFromSchema_ViewWithUnparseableDefinition(t *testing.T) {
	// View definition that pg_query can't parse triggers fallback
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	users := &model.Table{Schema: "public", Name: "users"}
	users.Columns = orderedmap.New[string, *model.Column]()
	users.Indexes = orderedmap.New[string, *model.Index]()
	users.Constraints = orderedmap.New[string, *model.Constraint]()
	users.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set("public.users", users)

	views := orderedmap.New[string, *model.View]()
	// Invalid SQL that can't be parsed but contains "public.users"
	views.Set("public.bad_view", &model.View{
		Schema:     "public",
		Name:       "bad_view",
		Definition: "NOT VALID SQL BUT MENTIONS public.users",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	// Fallback substring matching should still detect the dependency
	assert.Less(t, idx["public.users"], idx["public.bad_view"])
}
