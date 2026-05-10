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

func newTable(schema, name string, columns ...*model.Column) *model.Table {
	t := &model.Table{Schema: schema, Name: name}
	t.Columns = orderedmap.New[string, *model.Column]()
	for _, c := range columns {
		t.Columns.Set(c.Name, c)
	}
	t.Indexes = orderedmap.New[string, *model.Index]()
	t.Constraints = orderedmap.New[string, *model.Constraint]()
	t.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	return t
}

func TestOrderFromSchema_UnqualifiedTypeInPublicFromOtherSchema(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()

	domains := orderedmap.New[string, *model.Domain]()
	domains.Set(`public."Name"`, &model.Domain{Schema: "public", Name: `"Name"`, BaseType: "character varying(50)"})

	tables := orderedmap.New[string, *model.Table]()
	tables.Set("humanresources.department", newTable("humanresources", "department",
		&model.Column{Name: "name", TypeName: `"Name"`}))

	order, err := toposort.OrderFromSchema(enums, domains, tables, orderedmap.New[string, *model.View]())
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	// PostgreSQL's default search_path includes public, so an unqualified type
	// reference from a non-public schema resolves to public. The toposort must
	// model this so CREATE DOMAIN is emitted before the CREATE TABLE that uses it.
	assert.Less(t, idx[`public."Name"`], idx["humanresources.department"], "domain in public before table in other schema referencing it unqualified")
}

func TestOrderFromSchema_UnqualifiedEnumInPublicFromOtherSchema(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	enums.Set("public.status", &model.Enum{Schema: "public", Name: "status", Values: []string{"a", "b"}})

	tables := orderedmap.New[string, *model.Table]()
	tables.Set("app.users", newTable("app", "users",
		&model.Column{Name: "s", TypeName: "status"}))

	order, err := toposort.OrderFromSchema(enums, orderedmap.New[string, *model.Domain](),
		tables, orderedmap.New[string, *model.View]())
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}
	assert.Less(t, idx["public.status"], idx["app.users"], "enum in public before table in other schema")
}

func TestOrderFromSchema_UnqualifiedDomainBaseTypeFromPublic(t *testing.T) {
	domains := orderedmap.New[string, *model.Domain]()
	// app.short_name's base type "name_t" is defined in public.
	domains.Set("public.name_t", &model.Domain{Schema: "public", Name: "name_t", BaseType: "varchar(50)"})
	domains.Set("app.short_name", &model.Domain{Schema: "app", Name: "short_name", BaseType: "name_t"})

	order, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](), domains,
		orderedmap.New[string, *model.Table](), orderedmap.New[string, *model.View]())
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}
	assert.Less(t, idx["public.name_t"], idx["app.short_name"], "public domain before non-public domain that uses it as unqualified base type")
}

func TestOrderFromSchema_UnqualifiedArrayTypeInPublicFromOtherSchema(t *testing.T) {
	domains := orderedmap.New[string, *model.Domain]()
	domains.Set("public.tag", &model.Domain{Schema: "public", Name: "tag", BaseType: "text"})

	tables := orderedmap.New[string, *model.Table]()
	tables.Set("app.posts", newTable("app", "posts",
		&model.Column{Name: "tags", TypeName: "tag[]"}))

	order, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](), domains,
		tables, orderedmap.New[string, *model.View]())
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}
	assert.Less(t, idx["public.tag"], idx["app.posts"], "array of public domain resolved when used unqualified from other schema")
}

func TestOrderFromSchema_UnqualifiedTypeShadowedByDefaultSchema(t *testing.T) {
	// Same type name exists in both default schema and public; the default
	// schema's definition must win, matching PostgreSQL's search_path order.
	domains := orderedmap.New[string, *model.Domain]()
	domains.Set("public.name_t", &model.Domain{Schema: "public", Name: "name_t", BaseType: "varchar(10)"})
	domains.Set("app.name_t", &model.Domain{Schema: "app", Name: "name_t", BaseType: "varchar(20)"})

	tables := orderedmap.New[string, *model.Table]()
	tables.Set("app.t", newTable("app", "t",
		&model.Column{Name: "n", TypeName: "name_t"}))

	order, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](), domains,
		tables, orderedmap.New[string, *model.View]())
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}
	assert.Less(t, idx["app.name_t"], idx["app.t"], "default schema wins over public when both define the same type")
}

func TestOrderFromSchema_UnqualifiedFKInPublicFromOtherSchema(t *testing.T) {
	tables := orderedmap.New[string, *model.Table]()
	users := newTable("public", "users", &model.Column{Name: "id", TypeName: "integer"})
	tables.Set("public.users", users)

	refTable := "users"
	posts := newTable("app", "posts", &model.Column{Name: "user_id", TypeName: "integer"})
	posts.ForeignKeys.Set("fk_user", &model.ForeignKey{Constraint: model.Constraint{Name: "fk_user"}, RefTable: &refTable})
	tables.Set("app.posts", posts)

	order, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](),
		orderedmap.New[string, *model.Domain](), tables, orderedmap.New[string, *model.View]())
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}
	assert.Less(t, idx["public.users"], idx["app.posts"], "public table referenced by unqualified FK from other schema is ordered first")
}

func TestOrderFromSchema_ViewInOtherSchemaReferencesUnqualifiedPublicTable(t *testing.T) {
	tables := orderedmap.New[string, *model.Table]()
	tables.Set("public.users", newTable("public", "users", &model.Column{Name: "id", TypeName: "integer"}))

	views := orderedmap.New[string, *model.View]()
	views.Set("app.user_summary", &model.View{
		Schema:     "app",
		Name:       "user_summary",
		Definition: "SELECT id FROM users",
	})

	order, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](),
		orderedmap.New[string, *model.Domain](), tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}
	assert.Less(t, idx["public.users"], idx["app.user_summary"], "view in other schema referencing unqualified public table is ordered after it")
}

// FK with a nil RefTable can occur when a model.ForeignKey lacks the
// reference target metadata (e.g. partial parse output). The toposort must
// skip it without panicking and without adding a spurious edge.
func TestOrderFromSchema_FKWithNilRefTable(t *testing.T) {
	tables := orderedmap.New[string, *model.Table]()
	tbl := newTable("public", "events", &model.Column{Name: "id", TypeName: "integer"})
	tbl.ForeignKeys.Set("orphan_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "orphan_fk"},
		// RefTable intentionally nil
	})
	tables.Set("public.events", tbl)

	require.NotPanics(t, func() {
		_, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](),
			orderedmap.New[string, *model.Domain](), tables, orderedmap.New[string, *model.View]())
		require.NoError(t, err)
	})
}

// A view body referencing an explicitly schema-qualified table that is not in
// `defined` (e.g. a system table or a table outside the configured schemas)
// must produce no dependency edge — qualifyRangeVar returns "".
func TestOrderFromSchema_ViewBodyExplicitRefToUndefinedTable(t *testing.T) {
	views := orderedmap.New[string, *model.View]()
	views.Set("app.from_pg_catalog", &model.View{
		Schema:     "app",
		Name:       "from_pg_catalog",
		Definition: "SELECT relname FROM pg_catalog.pg_class",
	})

	order, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](),
		orderedmap.New[string, *model.Domain](),
		orderedmap.New[string, *model.Table](), views)
	require.NoError(t, err)
	assert.Equal(t, []string{"app.from_pg_catalog"}, order)
}

// `defined` is keyed by model.Ident(schema, name), which quotes identifiers
// that need it (uppercase, reserved, etc.). The search_path-fallback helpers
// must construct lookup keys the same way; raw "schema.name" concat would
// miss any schema that requires quoting.

func TestOrderFromSchema_UnqualifiedTypeInQuoteRequiringSchema(t *testing.T) {
	d := &model.Domain{Schema: "MySchema", Name: "name_t", BaseType: "varchar(50)"}
	domains := orderedmap.New[string, *model.Domain]()
	domains.Set(d.FQDN(), d)

	tbl := newTable("MySchema", "department", &model.Column{Name: "name", TypeName: "name_t"})
	tables := orderedmap.New[string, *model.Table]()
	tables.Set(tbl.FQTN(), tbl)

	order, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](), domains, tables, orderedmap.New[string, *model.View]())
	require.NoError(t, err)
	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}
	assert.Less(t, idx[d.FQDN()], idx[tbl.FQTN()], "domain in quote-requiring schema before table referencing its type unqualified")
}

func TestOrderFromSchema_UnqualifiedFKInQuoteRequiringSchema(t *testing.T) {
	users := newTable("MySchema", "users", &model.Column{Name: "id", TypeName: "integer"})
	refTable := "users"
	posts := newTable("MySchema", "posts", &model.Column{Name: "user_id", TypeName: "integer"})
	posts.ForeignKeys.Set("fk_user", &model.ForeignKey{Constraint: model.Constraint{Name: "fk_user"}, RefTable: &refTable})

	tables := orderedmap.New[string, *model.Table]()
	tables.Set(users.FQTN(), users)
	tables.Set(posts.FQTN(), posts)

	order, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](),
		orderedmap.New[string, *model.Domain](), tables, orderedmap.New[string, *model.View]())
	require.NoError(t, err)
	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}
	assert.Less(t, idx[users.FQTN()], idx[posts.FQTN()], "FK target in quote-requiring schema resolved when referenced unqualified")
}

func TestOrderFromSchema_ViewBodyTableRefInQuoteRequiringSchema(t *testing.T) {
	users := newTable("MySchema", "users", &model.Column{Name: "id", TypeName: "integer"})
	tables := orderedmap.New[string, *model.Table]()
	tables.Set(users.FQTN(), users)

	v := &model.View{
		Schema:     "MySchema",
		Name:       "user_summary",
		Definition: "SELECT id FROM users",
	}
	views := orderedmap.New[string, *model.View]()
	views.Set(model.Ident(v.Schema, v.Name), v)

	order, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](),
		orderedmap.New[string, *model.Domain](), tables, views)
	require.NoError(t, err)
	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}
	assert.Less(t, idx[users.FQTN()], idx[model.Ident(v.Schema, v.Name)], "table in quote-requiring schema before view referencing it unqualified")
}

func TestOrderFromSchema_ViewBodyFallbackInQuoteRequiringSchema(t *testing.T) {
	// extractViewDepsFallback path: trigger by giving a definition that
	// pg_query cannot parse standalone, while still containing the table name.
	users := newTable("MySchema", "users", &model.Column{Name: "id", TypeName: "integer"})
	tables := orderedmap.New[string, *model.Table]()
	tables.Set(users.FQTN(), users)

	v := &model.View{
		Schema:     "MySchema",
		Name:       "user_summary",
		Definition: "this is not parseable but mentions users somewhere",
	}
	views := orderedmap.New[string, *model.View]()
	views.Set(model.Ident(v.Schema, v.Name), v)

	order, err := toposort.OrderFromSchema(orderedmap.New[string, *model.Enum](),
		orderedmap.New[string, *model.Domain](), tables, views)
	require.NoError(t, err)
	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}
	assert.Less(t, idx[users.FQTN()], idx[model.Ident(v.Schema, v.Name)], "fallback substring matcher recognizes table in quote-requiring schema")
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

func TestOrderFromSchema_FKWithQuotedRefTable(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()
	views := orderedmap.New[string, *model.View]()

	// Both names require quoting (mixed case). Source ("Comments") sorts
	// before target ("Users") alphabetically, so without the FK edge being
	// registered, the toposort fallback would emit source first — which is
	// wrong. The edge can only be registered when the FK lookup uses the
	// same quoted form as the map keys.
	refTable := "Users"
	refSchema := "public"

	tables := orderedmap.New[string, *model.Table]()

	users := &model.Table{Schema: "public", Name: "Users"}
	users.Columns = orderedmap.New[string, *model.Column]()
	users.Indexes = orderedmap.New[string, *model.Index]()
	users.Constraints = orderedmap.New[string, *model.Constraint]()
	users.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set(model.Ident("public", "Users"), users)

	comments := &model.Table{Schema: "public", Name: "Comments"}
	comments.Columns = orderedmap.New[string, *model.Column]()
	comments.Indexes = orderedmap.New[string, *model.Index]()
	comments.Constraints = orderedmap.New[string, *model.Constraint]()
	comments.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	comments.ForeignKeys.Set("comments_user_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "comments_user_fk"},
		RefSchema:  &refSchema,
		RefTable:   &refTable,
	})
	tables.Set(model.Ident("public", "Comments"), comments)

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx[model.Ident("public", "Users")], idx[model.Ident("public", "Comments")], "FK target before source for quoted ref table")
}

func TestOrderFromSchema_FKWithReservedWordRefTable(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()
	views := orderedmap.New[string, *model.View]()

	// "order" is a reserved word; both names quoted so alphabetical fallback
	// puts source ("Items") before target ("order").
	refTable := "order"
	refSchema := "public"

	tables := orderedmap.New[string, *model.Table]()

	orderTbl := &model.Table{Schema: "public", Name: "order"}
	orderTbl.Columns = orderedmap.New[string, *model.Column]()
	orderTbl.Indexes = orderedmap.New[string, *model.Index]()
	orderTbl.Constraints = orderedmap.New[string, *model.Constraint]()
	orderTbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set(model.Ident("public", "order"), orderTbl)

	items := &model.Table{Schema: "public", Name: "Items"}
	items.Columns = orderedmap.New[string, *model.Column]()
	items.Indexes = orderedmap.New[string, *model.Index]()
	items.Constraints = orderedmap.New[string, *model.Constraint]()
	items.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	items.ForeignKeys.Set("items_order_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "items_order_fk"},
		RefSchema:  &refSchema,
		RefTable:   &refTable,
	})
	tables.Set(model.Ident("public", "Items"), items)

	sorted, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range sorted {
		idx[name] = i
	}

	assert.Less(t, idx[model.Ident("public", "order")], idx[model.Ident("public", "Items")], "FK target before source for reserved-word ref table")
}

func TestOrderFromSchema_FKWithQuotedRefSchema(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()
	views := orderedmap.New[string, *model.View]()

	// Quoted schema name. Source schema "AppPublic" sorts before target
	// schema "Refs" alphabetically, so without the edge, source would come first.
	refTable := "users"
	refSchema := "Refs"

	tables := orderedmap.New[string, *model.Table]()

	users := &model.Table{Schema: "Refs", Name: "users"}
	users.Columns = orderedmap.New[string, *model.Column]()
	users.Indexes = orderedmap.New[string, *model.Index]()
	users.Constraints = orderedmap.New[string, *model.Constraint]()
	users.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tables.Set(model.Ident("Refs", "users"), users)

	posts := &model.Table{Schema: "AppPublic", Name: "posts"}
	posts.Columns = orderedmap.New[string, *model.Column]()
	posts.Indexes = orderedmap.New[string, *model.Index]()
	posts.Constraints = orderedmap.New[string, *model.Constraint]()
	posts.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	posts.ForeignKeys.Set("posts_user_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "posts_user_fk"},
		RefSchema:  &refSchema,
		RefTable:   &refTable,
	})
	tables.Set(model.Ident("AppPublic", "posts"), posts)

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx[model.Ident("Refs", "users")], idx[model.Ident("AppPublic", "posts")], "FK target before source across quoted schemas")
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

func TestOrderFromSchema_ViewWithJoinOnSubquery(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	domains := orderedmap.New[string, *model.Domain]()

	tables := orderedmap.New[string, *model.Table]()
	for _, name := range []string{"users", "posts", "config"} {
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
		Definition: "SELECT u.id FROM users u JOIN posts p ON u.id = p.user_id AND p.id > (SELECT val FROM config LIMIT 1)",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	assert.Less(t, idx["public.users"], idx["public.v"])
	assert.Less(t, idx["public.posts"], idx["public.v"])
	assert.Less(t, idx["public.config"], idx["public.v"], "config referenced in JOIN ON subquery")
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

func TestOrderFromSchema_ViewWithUnparseableDefinition_SchemalessRef(t *testing.T) {
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
	// Invalid SQL with schemaless reference "users" (not "public.users")
	views.Set("public.bad_view", &model.View{
		Schema:     "public",
		Name:       "bad_view",
		Definition: "NOT VALID SQL BUT MENTIONS users TABLE",
	})

	order, err := toposort.OrderFromSchema(enums, domains, tables, views)
	require.NoError(t, err)

	idx := make(map[string]int)
	for i, name := range order {
		idx[name] = i
	}

	// Fallback should match "users" part of "public.users" using defaultSchema
	assert.Less(t, idx["public.users"], idx["public.bad_view"])
}
