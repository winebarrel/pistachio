package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func newTable(schema, name string) *model.Table {
	return &model.Table{
		Schema:      schema,
		Name:        name,
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		ForeignKeys: orderedmap.New[string, *model.ForeignKey](),
		Indexes:     orderedmap.New[string, *model.Index](),
	}
}

func ptr[T any](v T) *T {
	return &v
}

func TestDiffTables_newTable(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	tbl := newTable("public", "users")
	tbl.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	tbl.Constraints.Set("users_pkey", &model.Constraint{Name: "users_pkey", Definition: "PRIMARY KEY (id)"})
	desired.Set("public.users", tbl)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "CREATE TABLE public.users")
}

func TestDiffTables_newTable_withExtras(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	tbl := newTable("public", "users")
	tbl.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	tbl.Indexes.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Table: "users", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	tbl.Comment = ptr("Users table")
	desired.Set("public.users", tbl)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 3)
	assert.Contains(t, stmts[0], "CREATE TABLE")
	assert.Contains(t, stmts[1], "CREATE INDEX idx_name")
	assert.Contains(t, stmts[2], "COMMENT ON TABLE")
}

func TestDiffTables_dropTable(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	current.Set("public.users", newTable("public", "users"))

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP TABLE public.users;"}, stmts)
}

func TestDiffTables_noChange(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	tbl1 := newTable("public", "users")
	tbl1.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	current.Set("public.users", tbl1)

	tbl2 := newTable("public", "users")
	tbl2.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	desired.Set("public.users", tbl2)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffColumns_addColumn(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN name text NOT NULL;", stmts[0])
}

func TestDiffColumns_dropColumn(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users DROP COLUMN name;", stmts[0])
}

func TestDiffColumns_alterType(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "varchar(100)"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text"})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN name SET DATA TYPE text;", stmts[0])
}

func TestDiffColumns_alterType_withCollation(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "varchar(100)"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", Collation: ptr("en_US")})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], `COLLATE "en_US"`)
}

func TestDiffColumns_alterDefault_set(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("age", &model.Column{Name: "age", TypeName: "integer"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("age", &model.Column{Name: "age", TypeName: "integer", Default: ptr("0")})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN age SET DEFAULT 0;", stmts[0])
}

func TestDiffColumns_alterDefault_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("age", &model.Column{Name: "age", TypeName: "integer", Default: ptr("0")})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("age", &model.Column{Name: "age", TypeName: "integer"})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN age DROP DEFAULT;", stmts[0])
}

func TestDiffColumns_alterNotNull_set(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN name SET NOT NULL;", stmts[0])
}

func TestDiffColumns_alterNotNull_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text"})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN name DROP NOT NULL;", stmts[0])
}

func TestDiffColumns_identitySkipsNotNull(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", Identity: model.ColumnIdentity('a')})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestAddColumnSQL_basic(t *testing.T) {
	col := &model.Column{Name: "name", TypeName: "text", NotNull: true}
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN name text NOT NULL;", addColumnSQL("public.users", col))
}

func TestAddColumnSQL_withDefault(t *testing.T) {
	col := &model.Column{Name: "active", TypeName: "boolean", Default: ptr("true")}
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN active boolean DEFAULT true;", addColumnSQL("public.users", col))
}

func TestAddColumnSQL_withCollation(t *testing.T) {
	col := &model.Column{Name: "name", TypeName: "text", Collation: ptr("en_US")}
	assert.Contains(t, addColumnSQL("public.users", col), `COLLATE "en_US"`)
}

func TestAddColumnSQL_identityAlways(t *testing.T) {
	col := &model.Column{Name: "id", TypeName: "integer", Identity: model.ColumnIdentity('a')}
	sql := addColumnSQL("public.users", col)
	assert.Contains(t, sql, "GENERATED ALWAYS AS IDENTITY")
	assert.NotContains(t, sql, "NOT NULL")
}

func TestAddColumnSQL_identityByDefault(t *testing.T) {
	col := &model.Column{Name: "id", TypeName: "integer", Identity: model.ColumnIdentity('d')}
	assert.Contains(t, addColumnSQL("public.users", col), "GENERATED BY DEFAULT AS IDENTITY")
}

func TestAddColumnSQL_generatedStored(t *testing.T) {
	col := &model.Column{Name: "full", TypeName: "text", Generated: model.ColumnGenerated('s'), Default: ptr("first || last")}
	assert.Contains(t, addColumnSQL("public.users", col), "GENERATED ALWAYS AS (first || last) STORED")
}

func TestDiffConstraints_add(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)"})

	stmts, err := diffConstraints("public.users", current, desired)
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age > 0);"}, stmts)
}

func TestDiffConstraints_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)"})
	desired := orderedmap.New[string, *model.Constraint]()

	stmts, err := diffConstraints("public.users", current, desired)
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users DROP CONSTRAINT chk_age;"}, stmts)
}

func TestDiffConstraints_change(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)"})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age >= 18)"})

	stmts, err := diffConstraints("public.users", current, desired)
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.users DROP CONSTRAINT chk_age;", stmts[0])
	assert.Equal(t, "ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age >= 18);", stmts[1])
}

func TestDiffIndexes_add(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})

	stmts, err := diffIndexes(current, desired)
	require.NoError(t, err)
	assert.Equal(t, []string{"CREATE INDEX idx_name ON public.users USING btree (name);"}, stmts)
}

func TestDiffIndexes_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	desired := orderedmap.New[string, *model.Index]()

	stmts, err := diffIndexes(current, desired)
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP INDEX public.idx_name;"}, stmts)
}

func TestDiffIndexes_change(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING hash (name)"})

	stmts, err := diffIndexes(current, desired)
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "DROP INDEX public.idx_name;", stmts[0])
	assert.Equal(t, "CREATE INDEX idx_name ON public.users USING hash (name);", stmts[1])
}

func TestDiffForeignKeys_add(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})

	stmts, err := diffForeignKeys("public.orders", "public", current, desired)
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "ADD CONSTRAINT fk_user")
}

func TestDiffForeignKeys_drop(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()

	stmts, err := diffForeignKeys("public.orders", "public", current, desired)
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.orders DROP CONSTRAINT fk_user;"}, stmts)
}

func TestDiffComments_tableComment_add(t *testing.T) {
	current := newTable("public", "users")
	desired := newTable("public", "users")
	desired.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Comment = ptr("Users table")

	stmts := diffComments(current, desired)
	assert.Equal(t, []string{"COMMENT ON TABLE public.users IS 'Users table';"}, stmts)
}

func TestDiffComments_tableComment_drop(t *testing.T) {
	current := newTable("public", "users")
	current.Comment = ptr("Users table")
	current.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired := newTable("public", "users")
	desired.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	stmts := diffComments(current, desired)
	assert.Equal(t, []string{"COMMENT ON TABLE public.users IS NULL;"}, stmts)
}

func TestDiffComments_columnComment_add(t *testing.T) {
	current := newTable("public", "users")
	current.Columns.Set("name", &model.Column{Name: "name", TypeName: "text"})
	desired := newTable("public", "users")
	desired.Columns.Set("name", &model.Column{Name: "name", TypeName: "text", Comment: ptr("User name")})

	stmts := diffComments(current, desired)
	assert.Equal(t, []string{"COMMENT ON COLUMN public.users.name IS 'User name';"}, stmts)
}

func TestDiffComments_columnComment_drop(t *testing.T) {
	current := newTable("public", "users")
	current.Columns.Set("name", &model.Column{Name: "name", TypeName: "text", Comment: ptr("User name")})
	desired := newTable("public", "users")
	desired.Columns.Set("name", &model.Column{Name: "name", TypeName: "text"})

	stmts := diffComments(current, desired)
	assert.Equal(t, []string{"COMMENT ON COLUMN public.users.name IS NULL;"}, stmts)
}

func TestDiffComments_newColumn(t *testing.T) {
	current := newTable("public", "users")
	desired := newTable("public", "users")
	desired.Columns.Set("name", &model.Column{Name: "name", TypeName: "text", Comment: ptr("new col comment")})

	stmts := diffComments(current, desired)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "COMMENT ON COLUMN")
}

func TestEqualPtr(t *testing.T) {
	assert.True(t, equalPtr[string](nil, nil))
	assert.False(t, equalPtr(ptr("a"), nil))
	assert.False(t, equalPtr(nil, ptr("a")))
	assert.True(t, equalPtr(ptr("a"), ptr("a")))
	assert.False(t, equalPtr(ptr("a"), ptr("b")))
}

func TestEqualTypeName(t *testing.T) {
	assert.True(t, equalTypeName("integer", "integer"))
	assert.True(t, equalTypeName("serial", "integer"))
	assert.True(t, equalTypeName("integer", "serial"))
	assert.True(t, equalTypeName("bigserial", "bigint"))
	assert.True(t, equalTypeName("smallserial", "smallint"))
	assert.False(t, equalTypeName("integer", "text"))
}

func TestEqualDefault(t *testing.T) {
	assert.True(t, equalDefault(nil, nil))
	assert.False(t, equalDefault(ptr("0"), nil))
	assert.False(t, equalDefault(nil, ptr("0")))
	assert.True(t, equalDefault(ptr("0"), ptr("0")))
	assert.True(t, equalDefault(ptr("'hello'::text"), ptr("'hello'")))
	assert.False(t, equalDefault(ptr("0"), ptr("1")))
}

func TestEqualFKDef(t *testing.T) {
	a := "FOREIGN KEY (user_id) REFERENCES users(id)"
	b := "FOREIGN KEY (user_id) REFERENCES users (id)"
	assert.True(t, equalFKDef(a, b, "public"))
}

func TestEqualFKDef_different(t *testing.T) {
	a := "FOREIGN KEY (user_id) REFERENCES users(id)"
	b := "FOREIGN KEY (user_id) REFERENCES orders(id)"
	assert.False(t, equalFKDef(a, b, "public"))
}

func TestDiffTables_newTable_withForeignKey(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	tbl := newTable("public", "orders")
	tbl.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	tbl.ForeignKeys.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired.Set("public.orders", tbl)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Contains(t, stmts[0], "CREATE TABLE")
	assert.Contains(t, stmts[1], "ADD CONSTRAINT fk_user")
}

func TestEqualFKDef_implicitPublicSchema(t *testing.T) {
	a := "FOREIGN KEY (user_id) REFERENCES users(id)"
	b := "FOREIGN KEY (user_id) REFERENCES public.users(id)"
	assert.True(t, equalFKDef(a, b, "public"))
}

func TestEqualFKDef_implicitNonPublicSchema(t *testing.T) {
	a := "FOREIGN KEY (item_id) REFERENCES items(id)"
	b := "FOREIGN KEY (item_id) REFERENCES myapp.items(id)"
	assert.True(t, equalFKDef(a, b, "myapp"))
}

func TestEqualFKDef_implicitNonPublicSchema_different(t *testing.T) {
	a := "FOREIGN KEY (item_id) REFERENCES items(id)"
	b := "FOREIGN KEY (item_id) REFERENCES other.items(id)"
	assert.False(t, equalFKDef(a, b, "myapp"))
}

func TestEqualFKDef_parseError(t *testing.T) {
	// When both fail to parse, falls back to string comparison
	assert.True(t, equalFKDef("not sql", "not sql", "public"))
	assert.False(t, equalFKDef("not sql", "other", "public"))
}

func TestEqualDefault_parseError(t *testing.T) {
	// When both fail to parse, falls back to string comparison
	assert.True(t, equalDefault(ptr(")))invalid"), ptr(")))invalid")))
	assert.False(t, equalDefault(ptr(")))invalid"), ptr(")))other")))
}

func TestEqualViewDef_parseError(t *testing.T) {
	// When normalization fails, falls back to string comparison
	assert.True(t, equalViewDef(")))invalid", ")))invalid"))
	assert.False(t, equalViewDef(")))invalid", ")))other"))
}

func TestDiffForeignKeys_change(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})

	stmts, err := diffForeignKeys("public.orders", "public", current, desired)
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.orders DROP CONSTRAINT fk_user;", stmts[0])
	assert.Contains(t, stmts[1], "ADD CONSTRAINT fk_user")
}

func TestDiffTable_partitionChild(t *testing.T) {
	parent := "public.events"
	bound := "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"

	current := newTable("public", "events_2024")
	current.PartitionOf = &parent
	current.PartitionBound = &bound

	desired := newTable("public", "events_2024")
	desired.PartitionOf = &parent
	desired.PartitionBound = &bound
	desired.Indexes.Set("idx_new", &model.Index{Schema: "public", Name: "idx_new", Definition: "CREATE INDEX idx_new ON public.events_2024 (id)"})

	stmts, err := diffTable(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "CREATE INDEX idx_new")
}

func TestDiffTable_partitionChild_indexRenameError(t *testing.T) {
	parent := "public.events"
	bound := "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"

	current := newTable("public", "events_2024")
	current.PartitionOf = &parent
	current.PartitionBound = &bound

	desired := newTable("public", "events_2024")
	desired.PartitionOf = &parent
	desired.PartitionBound = &bound
	oldName := "nonexistent"
	desired.Indexes.Set("idx_new", &model.Index{Schema: "public", Name: "idx_new", RenameFrom: &oldName, Definition: "CREATE INDEX idx_new ON public.events_2024 (id)"})

	_, err := diffTable(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source index")
}

func TestDiffTable_partitionChild_fkRenameError(t *testing.T) {
	parent := "public.events"
	bound := "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"

	current := newTable("public", "events_2024")
	current.PartitionOf = &parent
	current.PartitionBound = &bound

	desired := newTable("public", "events_2024")
	desired.PartitionOf = &parent
	desired.PartitionBound = &bound
	oldName := "nonexistent"
	desired.ForeignKeys.Set("fk_new", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_new", RenameFrom: &oldName, Definition: "FOREIGN KEY (id) REFERENCES public.events(id)"},
		Schema:     "public",
		Table:      "events_2024",
	})

	_, err := diffTable(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source foreign key")
}

func TestDiffTable_constraintRenameError(t *testing.T) {
	current := newTable("public", "users")
	current.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	desired := newTable("public", "users")
	desired.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	oldName := "nonexistent"
	desired.Constraints.Set("new_con", &model.Constraint{Name: "new_con", RenameFrom: &oldName, Definition: "UNIQUE (id)"})

	_, err := diffTable(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source constraint")
}

func TestDiffTable_indexRenameError(t *testing.T) {
	current := newTable("public", "users")
	current.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	desired := newTable("public", "users")
	desired.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	oldName := "nonexistent"
	desired.Indexes.Set("idx_new", &model.Index{Schema: "public", Name: "idx_new", RenameFrom: &oldName, Definition: "CREATE INDEX idx_new ON public.users (id)"})

	_, err := diffTable(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source index")
}

func TestDiffTable_fkRenameError(t *testing.T) {
	current := newTable("public", "users")
	current.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	desired := newTable("public", "users")
	desired.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	oldName := "nonexistent"
	desired.ForeignKeys.Set("fk_new", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_new", RenameFrom: &oldName, Definition: "FOREIGN KEY (id) REFERENCES public.other(id)"},
		Schema:     "public",
		Table:      "users",
	})

	_, err := diffTable(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source foreign key")
}

func TestEqualConstraintDef_same(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"PRIMARY KEY (id)",
		"PRIMARY KEY (id)",
	))
}

func TestEqualConstraintDef_formattingDiff(t *testing.T) {
	// pg_get_constraintdef may return extra parentheses compared to pg_query deparse
	assert.True(t, equalConstraintDef(
		"CHECK ((kind = ANY (ARRAY['a'::text, 'b'::text])))",
		"CHECK (kind = ANY(ARRAY['a'::text, 'b'::text]))",
	))
}

func TestEqualConstraintDef_castFormattingDiff(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK (((color)::text = ANY ((ARRAY['red'::character varying, 'blue'::character varying])::text[])))",
		"CHECK (color::text = ANY(ARRAY['red'::varchar, 'blue'::varchar]::text[]))",
	))
}

func TestEqualConstraintDef_different(t *testing.T) {
	assert.False(t, equalConstraintDef(
		"CHECK (age > 0)",
		"CHECK (age >= 18)",
	))
}

func TestEqualConstraintDef_nonTextCastPreserved(t *testing.T) {
	// ::integer cast is semantically meaningful and must not be stripped
	assert.False(t, equalConstraintDef(
		"CHECK (val > '0'::integer)",
		"CHECK (val > '0')",
	))
}

func TestEqualConstraintDef_textCastOnRegex(t *testing.T) {
	// pg_get_constraintdef adds ::text to string literals
	assert.True(t, equalConstraintDef(
		"CHECK ((code ~ '^[0-9a-f]{64}$'::text))",
		"CHECK (code ~ '^[0-9a-f]{64}$')",
	))
}

func TestEqualConstraintDef_textCastOnNotEmpty(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK ((name <> ''::text))",
		"CHECK (name <> '')",
	))
}

func TestEqualConstraintDef_inVsAnyArray(t *testing.T) {
	// pg_get_constraintdef returns = ANY(ARRAY[...]) for IN (...)
	assert.True(t, equalConstraintDef(
		"CHECK ((status = ANY (ARRAY['active'::text, 'pending'::text])))",
		"CHECK (status IN ('active', 'pending'))",
	))
}

func TestEqualConstraintDef_varcharCastInAnyArray(t *testing.T) {
	// pg_get_constraintdef may use ::character varying and ::text[] casts
	assert.True(t, equalConstraintDef(
		"CHECK ((status::text = ANY (ARRAY['active'::character varying, 'pending'::character varying]::text[])))",
		"CHECK (status IN ('active', 'pending'))",
	))
}

func TestEqualConstraintDef_varcharCastWithoutArrayCast(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK ((status = ANY (ARRAY['active'::character varying, 'pending'::character varying])))",
		"CHECK (status IN ('active', 'pending'))",
	))
}

func TestEqualConstraintDef_inVsAnyArray_different(t *testing.T) {
	assert.False(t, equalConstraintDef(
		"CHECK ((status = ANY (ARRAY['active'::text, 'pending'::text])))",
		"CHECK (status IN ('active', 'closed'))",
	))
}

func TestEqualConstraintDef_textCastInBoolExpr(t *testing.T) {
	// AND/OR combining multiple conditions with ::text casts
	assert.True(t, equalConstraintDef(
		"CHECK (((name <> ''::text) AND (code ~ '^[0-9]+$'::text)))",
		"CHECK (name <> '' AND code ~ '^[0-9]+$')",
	))
}

func TestEqualConstraintDef_textCastInFuncCall(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK ((length((name)::text) > 0))",
		"CHECK (length(name::text) > 0)",
	))
}

func TestEqualConstraintDef_textCastInCoalesce(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK ((COALESCE(name, ''::text) <> ''::text))",
		"CHECK (COALESCE(name, '') <> '')",
	))
}

func TestEqualConstraintDef_textCastInNullTest(t *testing.T) {
	// NullTest recurse - ensure it doesn't break
	assert.True(t, equalConstraintDef(
		"CHECK ((name IS NOT NULL))",
		"CHECK (name IS NOT NULL)",
	))
}

func TestEqualConstraintDef_textCastInCase(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK (((CASE WHEN (kind = 'a'::text) THEN 1 ELSE 0 END) > 0))",
		"CHECK ((CASE WHEN kind = 'a' THEN 1 ELSE 0 END) > 0)",
	))
}

func TestEqualConstraintDef_parseError(t *testing.T) {
	assert.True(t, equalConstraintDef(")))invalid", ")))invalid"))
	assert.False(t, equalConstraintDef(")))invalid", ")))other"))
}

func TestDiffConstraints_noChangeWithTextCast(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_name", &model.Constraint{
		Name:       "chk_name",
		Definition: "CHECK ((name <> ''::text))",
	})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_name", &model.Constraint{
		Name:       "chk_name",
		Definition: "CHECK (name <> '')",
	})

	stmts, err := diffConstraints("public.items", current, desired)
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffConstraints_noChangeWithInVsAny(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_status", &model.Constraint{
		Name:       "chk_status",
		Definition: "CHECK ((status = ANY (ARRAY['active'::text, 'pending'::text])))",
	})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_status", &model.Constraint{
		Name:       "chk_status",
		Definition: "CHECK (status IN ('active', 'pending'))",
	})

	stmts, err := diffConstraints("public.items", current, desired)
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffConstraints_noChangeWithFormattingDiff(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_kind", &model.Constraint{
		Name:       "chk_kind",
		Definition: "CHECK ((kind = ANY (ARRAY['x'::text, 'y'::text])))",
	})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_kind", &model.Constraint{
		Name:       "chk_kind",
		Definition: "CHECK (kind = ANY(ARRAY['x'::text, 'y'::text]))",
	})

	stmts, err := diffConstraints("public.items", current, desired)
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestEqualIndexDef_sameSchema(t *testing.T) {
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON public.users USING btree (id)",
		"CREATE INDEX idx ON public.users USING btree (id)",
	))
}

func TestEqualIndexDef_schemaVsNoSchema(t *testing.T) {
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON public.users USING btree (id)",
		"CREATE INDEX idx ON users USING btree (id)",
	))
}

func TestEqualIndexDef_customSchemaVsNoSchema(t *testing.T) {
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON myschema.users USING btree (id)",
		"CREATE INDEX idx ON users USING btree (id)",
	))
}

func TestEqualIndexDef_differentSchemas(t *testing.T) {
	// Different schemas should still be equal — schema is ignored in comparison
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON myschema.users USING btree (id)",
		"CREATE INDEX idx ON public.users USING btree (id)",
	))
}

func TestEqualIndexDef_whereClauseSchemaVsNoSchema(t *testing.T) {
	assert.True(t, equalIndexDef(
		"CREATE UNIQUE INDEX idx ON myschema.products USING btree (sku) WHERE removed_at IS NULL AND sku IS NOT NULL",
		"CREATE UNIQUE INDEX idx ON products USING btree (sku) WHERE removed_at IS NULL AND sku IS NOT NULL",
	))
}

func TestEqualIndexDef_whereClauseFormattingDiff(t *testing.T) {
	// pg_get_indexdef may return extra parentheses in WHERE clause
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON myschema.products USING btree (group_id) WHERE ((group_id IS NOT NULL))",
		"CREATE INDEX idx ON products USING btree (group_id) WHERE group_id IS NOT NULL",
	))
}

func TestEqualIndexDef_whereClauseWithCast(t *testing.T) {
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON myschema.events USING btree (created_at) WHERE ((kind)::text = 'done'::text)",
		"CREATE INDEX idx ON events USING btree (created_at) WHERE kind::text = 'done'::text",
	))
}

func TestEqualIndexDef_whereClauseBooleanCondition(t *testing.T) {
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON myschema.tasks USING btree (priority) WHERE ((visible = true))",
		"CREATE INDEX idx ON tasks USING btree (priority) WHERE visible = true",
	))
}

func TestEqualIndexDef_whereClauseMultipleConditions(t *testing.T) {
	assert.True(t, equalIndexDef(
		"CREATE UNIQUE INDEX idx ON myschema.tasks USING btree (kind, seq) WHERE ((visible = true) AND (seq > 0))",
		"CREATE UNIQUE INDEX idx ON tasks USING btree (kind, seq) WHERE visible = true AND seq > 0",
	))
}

func TestDiffTables_indexWhereClauseSchemaInsensitive(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct := newTable("public", "products")
	ct.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	ct.Indexes.Set("idx", &model.Index{
		Schema:     "public",
		Name:       "idx",
		Table:      "products",
		Definition: "CREATE UNIQUE INDEX idx ON public.products USING btree (sku) WHERE ((removed_at IS NULL) AND (sku IS NOT NULL))",
	})
	current.Set("public.products", ct)

	dt := newTable("public", "products")
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	dt.Indexes.Set("idx", &model.Index{
		Schema:     "",
		Name:       "idx",
		Table:      "products",
		Definition: "CREATE UNIQUE INDEX idx ON products USING btree (sku) WHERE removed_at IS NULL AND sku IS NOT NULL",
	})
	desired.Set("public.products", dt)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestEqualIndexDef_explicitAscVsDefault(t *testing.T) {
	// ASC is the default sort order; pg_get_indexdef omits it
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON t USING btree (col1 DESC, col2)",
		"CREATE INDEX idx ON t USING btree (col1 DESC, col2 ASC)",
	))
}

func TestEqualIndexDef_allDefault(t *testing.T) {
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON t USING btree (col1)",
		"CREATE INDEX idx ON t USING btree (col1 ASC)",
	))
}

func TestEqualIndexDef_descNullsFirst(t *testing.T) {
	// NULLS FIRST is the default for DESC; pg_get_indexdef omits it
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON t USING btree (col1 DESC)",
		"CREATE INDEX idx ON t USING btree (col1 DESC NULLS FIRST)",
	))
}

func TestEqualIndexDef_ascNullsLast(t *testing.T) {
	// NULLS LAST is the default for ASC; pg_get_indexdef omits it
	assert.True(t, equalIndexDef(
		"CREATE INDEX idx ON t USING btree (col1)",
		"CREATE INDEX idx ON t USING btree (col1 ASC NULLS LAST)",
	))
}

func TestEqualIndexDef_descNullsLast_notEqual(t *testing.T) {
	// NULLS LAST for DESC is non-default, must not be treated as equal
	assert.False(t, equalIndexDef(
		"CREATE INDEX idx ON t USING btree (col1 DESC)",
		"CREATE INDEX idx ON t USING btree (col1 DESC NULLS LAST)",
	))
}

func TestEqualIndexDef_ascNullsFirst_notEqual(t *testing.T) {
	// NULLS FIRST for ASC is non-default, must not be treated as equal
	assert.False(t, equalIndexDef(
		"CREATE INDEX idx ON t USING btree (col1)",
		"CREATE INDEX idx ON t USING btree (col1 ASC NULLS FIRST)",
	))
}

func TestEqualIndexDef_different(t *testing.T) {
	assert.False(t, equalIndexDef(
		"CREATE INDEX idx ON public.users USING btree (id)",
		"CREATE INDEX idx ON public.users USING btree (name)",
	))
}

func TestEqualIndexDef_parseError(t *testing.T) {
	assert.False(t, equalIndexDef("NOT VALID SQL", "CREATE INDEX idx ON users (id)"))
	assert.True(t, equalIndexDef("NOT VALID SQL", "NOT VALID SQL"))
}

func TestEqualIndexDef_notIndexStmt(t *testing.T) {
	// Valid SQL but not an INDEX statement — falls back to string comparison
	assert.False(t, equalIndexDef("SELECT 1", "CREATE INDEX idx ON users (id)"))
	assert.True(t, equalIndexDef("SELECT 1", "SELECT 1"))
}

func TestDiffTables_indexSchemaInsensitive(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct := newTable("public", "users")
	ct.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	ct.Indexes.Set("idx", &model.Index{Schema: "public", Name: "idx", Table: "users", Definition: "CREATE INDEX idx ON public.users USING btree (id)"})
	current.Set("public.users", ct)

	dt := newTable("public", "users")
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	dt.Indexes.Set("idx", &model.Index{Schema: "", Name: "idx", Table: "users", Definition: "CREATE INDEX idx ON users USING btree (id)"})
	desired.Set("public.users", dt)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffTables_renameTable(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct := newTable("public", "users")
	ct.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Set("public.users", ct)

	oldName := "public.users"
	dt := newTable("public", "accounts")
	dt.RenameFrom = &oldName
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Set("public.accounts", dt)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users RENAME TO accounts;"}, stmts)
}

func TestDiffTables_renameTable_selfRename_skipped(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct := newTable("public", "users")
	ct.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Set("public.users", ct)

	oldName := "public.users"
	dt := newTable("public", "users")
	dt.RenameFrom = &oldName
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Set("public.users", dt)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffColumns_renameColumn_selfRename_skipped(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	oldName := "name"
	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", RenameFrom: &oldName, TypeName: "text"})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffConstraints_rename_selfRename_skipped(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("con", &model.Constraint{Name: "con", Definition: "UNIQUE (code)"})

	oldName := "con"
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("con", &model.Constraint{Name: "con", RenameFrom: &oldName, Definition: "UNIQUE (code)"})

	stmts, err := diffConstraints("public.users", current, desired)
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffIndexes_rename_selfRename_skipped(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx", &model.Index{Schema: "public", Name: "idx", Table: "users", Definition: "CREATE INDEX idx ON public.users USING btree (name)"})

	oldName := "idx"
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx", &model.Index{Schema: "public", Name: "idx", RenameFrom: &oldName, Table: "users", Definition: "CREATE INDEX idx ON public.users USING btree (name)"})

	stmts, err := diffIndexes(current, desired)
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffForeignKeys_rename_selfRename_skipped(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk", Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "orders",
	})

	oldName := "fk"
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk", RenameFrom: &oldName, Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "orders",
	})

	stmts, err := diffForeignKeys("public.orders", "public", current, desired)
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffTables_renameTable_alreadyApplied(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct := newTable("public", "accounts")
	ct.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Set("public.accounts", ct)

	oldName := "public.users"
	dt := newTable("public", "accounts")
	dt.RenameFrom = &oldName
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Set("public.accounts", dt)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffTables_renameTable_withIndex(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct := newTable("public", "users")
	ct.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	ct.Columns.Set("name", &model.Column{Name: "name", TypeName: "text"})
	ct.Indexes.Set("idx_users_name", &model.Index{Schema: "public", Name: "idx_users_name", Table: "users", Definition: "CREATE INDEX idx_users_name ON public.users USING btree (name)"})
	current.Set("public.users", ct)

	oldName := "public.users"
	dt := newTable("public", "accounts")
	dt.RenameFrom = &oldName
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	dt.Columns.Set("name", &model.Column{Name: "name", TypeName: "text"})
	dt.Indexes.Set("idx_users_name", &model.Index{Schema: "public", Name: "idx_users_name", Table: "accounts", Definition: "CREATE INDEX idx_users_name ON public.accounts USING btree (name)"})
	desired.Set("public.accounts", dt)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	// Should only rename the table, no DROP/CREATE index
	assert.Equal(t, []string{"ALTER TABLE public.users RENAME TO accounts;"}, stmts)
}

func TestDiffTables_renameTable_withFK(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct := newTable("public", "orders")
	ct.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	ct.Columns.Set("user_id", &model.Column{Name: "user_id", TypeName: "integer"})
	ct.ForeignKeys.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "orders",
	})
	current.Set("public.orders", ct)

	oldName := "public.orders"
	dt := newTable("public", "purchases")
	dt.RenameFrom = &oldName
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	dt.Columns.Set("user_id", &model.Column{Name: "user_id", TypeName: "integer"})
	dt.ForeignKeys.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "purchases",
	})
	desired.Set("public.purchases", dt)

	stmts, err := DiffTables(current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.orders RENAME TO purchases;"}, stmts)
}

func TestDiffTables_renameTable_destinationExists_error(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct1 := newTable("public", "users")
	ct1.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Set("public.users", ct1)

	ct2 := newTable("public", "accounts")
	ct2.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Set("public.accounts", ct2)

	oldName := "public.users"
	dt := newTable("public", "accounts")
	dt.RenameFrom = &oldName
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Set("public.accounts", dt)

	_, err := DiffTables(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffColumns_renameColumn_destinationExists_error(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})
	current.Set("display_name", &model.Column{Name: "display_name", TypeName: "text"})

	oldName := "name"
	desired := orderedmap.New[string, *model.Column]()
	desired.Set("display_name", &model.Column{Name: "display_name", RenameFrom: &oldName, TypeName: "text"})

	_, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffTables_renameTable_crossSchema_error(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct := newTable("public", "users")
	ct.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Set("public.users", ct)

	oldName := "public.users"
	dt := newTable("other", "users")
	dt.RenameFrom = &oldName
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Set("other.users", dt)

	_, err := DiffTables(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cross-schema rename")
}

func TestDiffColumns_renameColumn(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	oldName := "name"
	desired := orderedmap.New[string, *model.Column]()
	desired.Set("display_name", &model.Column{Name: "display_name", RenameFrom: &oldName, TypeName: "text"})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users RENAME COLUMN name TO display_name;"}, stmts)
}

func TestDiffColumns_renameColumn_alreadyApplied(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("display_name", &model.Column{Name: "display_name", TypeName: "text"})

	oldName := "name"
	desired := orderedmap.New[string, *model.Column]()
	desired.Set("display_name", &model.Column{Name: "display_name", RenameFrom: &oldName, TypeName: "text"})

	stmts, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffConstraints_rename(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("old_con", &model.Constraint{Name: "old_con", Definition: "UNIQUE (code)"})

	oldName := "old_con"
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("new_con", &model.Constraint{Name: "new_con", RenameFrom: &oldName, Definition: "UNIQUE (code)"})

	stmts, err := diffConstraints("public.users", current, desired)
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users RENAME CONSTRAINT old_con TO new_con;"}, stmts)
}

func TestDiffIndexes_rename(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("old_idx", &model.Index{Schema: "public", Name: "old_idx", Table: "users", Definition: "CREATE INDEX old_idx ON public.users USING btree (name)"})

	oldName := "old_idx"
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", RenameFrom: &oldName, Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (name)"})

	stmts, err := diffIndexes(current, desired)
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER INDEX public.old_idx RENAME TO new_idx;"}, stmts)
}

func TestDiffConstraints_rename_alreadyApplied(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("new_con", &model.Constraint{Name: "new_con", Definition: "UNIQUE (code)"})

	oldName := "old_con"
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("new_con", &model.Constraint{Name: "new_con", RenameFrom: &oldName, Definition: "UNIQUE (code)"})

	stmts, err := diffConstraints("public.users", current, desired)
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffConstraints_rename_sourceNotFound(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()

	oldName := "nonexistent"
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("new_con", &model.Constraint{Name: "new_con", RenameFrom: &oldName, Definition: "UNIQUE (code)"})

	_, err := diffConstraints("public.users", current, desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source constraint")
}

func TestDiffIndexes_rename_alreadyApplied(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (name)"})

	oldName := "old_idx"
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", RenameFrom: &oldName, Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (name)"})

	stmts, err := diffIndexes(current, desired)
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffIndexes_rename_sourceNotFound(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()

	oldName := "nonexistent"
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", RenameFrom: &oldName, Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (name)"})

	_, err := diffIndexes(current, desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source index")
}

func TestDiffForeignKeys_rename(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("old_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "old_fk", Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "orders",
	})

	oldName := "old_fk"
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("new_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "new_fk", RenameFrom: &oldName, Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "orders",
	})

	stmts, err := diffForeignKeys("public.orders", "public", current, desired)
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.orders RENAME CONSTRAINT old_fk TO new_fk;"}, stmts)
}

func TestDiffForeignKeys_rename_alreadyApplied(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("new_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "new_fk", Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "orders",
	})

	oldName := "old_fk"
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("new_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "new_fk", RenameFrom: &oldName, Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "orders",
	})

	stmts, err := diffForeignKeys("public.orders", "public", current, desired)
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffForeignKeys_rename_sourceNotFound(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()

	oldName := "nonexistent"
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("new_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "new_fk", RenameFrom: &oldName, Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "orders",
	})

	_, err := diffForeignKeys("public.orders", "public", current, desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source foreign key")
}

func TestDiffConstraints_rename_destinationExists_error(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("old_con", &model.Constraint{Name: "old_con", Definition: "UNIQUE (code)"})
	current.Set("new_con", &model.Constraint{Name: "new_con", Definition: "UNIQUE (name)"})

	oldName := "old_con"
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("new_con", &model.Constraint{Name: "new_con", RenameFrom: &oldName, Definition: "UNIQUE (code)"})

	_, err := diffConstraints("public.users", current, desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffIndexes_rename_destinationExists_error(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("old_idx", &model.Index{Schema: "public", Name: "old_idx", Table: "users", Definition: "CREATE INDEX old_idx ON public.users USING btree (name)"})
	current.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (id)"})

	oldName := "old_idx"
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", RenameFrom: &oldName, Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (name)"})

	_, err := diffIndexes(current, desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffForeignKeys_rename_destinationExists_error(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("old_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "old_fk", Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "orders",
	})
	current.Set("new_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "new_fk", Definition: "FOREIGN KEY (item_id) REFERENCES public.items(id)"},
		Schema:     "public",
		Table:      "orders",
	})

	oldName := "old_fk"
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("new_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "new_fk", RenameFrom: &oldName, Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)"},
		Schema:     "public",
		Table:      "orders",
	})

	_, err := diffForeignKeys("public.orders", "public", current, desired)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination already exists")
}

func TestDiffTables_renameTable_sourceNotFound(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	oldName := "public.nonexistent"
	dt := newTable("public", "accounts")
	dt.RenameFrom = &oldName
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Set("public.accounts", dt)

	_, err := DiffTables(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source")
}

func TestDiffTables_renameColumn_error_propagates(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct := newTable("public", "users")
	ct.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Set("public.users", ct)

	oldName := "nonexistent"
	dt := newTable("public", "users")
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	dt.Columns.Set("new_col", &model.Column{Name: "new_col", RenameFrom: &oldName, TypeName: "text"})
	desired.Set("public.users", dt)

	_, err := DiffTables(current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source column")
}

func TestDiffColumns_renameColumn_sourceNotFound(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()

	oldName := "nonexistent"
	desired := orderedmap.New[string, *model.Column]()
	desired.Set("display_name", &model.Column{Name: "display_name", RenameFrom: &oldName, TypeName: "text"})

	_, err := diffColumns("public.users", current, desired, AllowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source column")
}
