package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

	stmts := DiffTables(current, desired)
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

	stmts := DiffTables(current, desired)
	assert.Len(t, stmts, 3)
	assert.Contains(t, stmts[0], "CREATE TABLE")
	assert.Contains(t, stmts[1], "CREATE INDEX idx_name")
	assert.Contains(t, stmts[2], "COMMENT ON TABLE")
}

func TestDiffTables_dropTable(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	current.Set("public.users", newTable("public", "users"))

	stmts := DiffTables(current, desired)
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

	stmts := DiffTables(current, desired)
	assert.Empty(t, stmts)
}

func TestDiffColumns_addColumn(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	stmts := diffColumns("public.users", current, desired)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN name text NOT NULL;", stmts[0])
}

func TestDiffColumns_dropColumn(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	stmts := diffColumns("public.users", current, desired)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users DROP COLUMN name;", stmts[0])
}

func TestDiffColumns_alterType(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "varchar(100)"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text"})

	stmts := diffColumns("public.users", current, desired)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN name SET DATA TYPE text;", stmts[0])
}

func TestDiffColumns_alterType_withCollation(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "varchar(100)"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", Collation: ptr("en_US")})

	stmts := diffColumns("public.users", current, desired)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], `COLLATE "en_US"`)
}

func TestDiffColumns_alterDefault_set(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("age", &model.Column{Name: "age", TypeName: "integer"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("age", &model.Column{Name: "age", TypeName: "integer", Default: ptr("0")})

	stmts := diffColumns("public.users", current, desired)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN age SET DEFAULT 0;", stmts[0])
}

func TestDiffColumns_alterDefault_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("age", &model.Column{Name: "age", TypeName: "integer", Default: ptr("0")})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("age", &model.Column{Name: "age", TypeName: "integer"})

	stmts := diffColumns("public.users", current, desired)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN age DROP DEFAULT;", stmts[0])
}

func TestDiffColumns_alterNotNull_set(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	stmts := diffColumns("public.users", current, desired)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN name SET NOT NULL;", stmts[0])
}

func TestDiffColumns_alterNotNull_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text"})

	stmts := diffColumns("public.users", current, desired)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN name DROP NOT NULL;", stmts[0])
}

func TestDiffColumns_identitySkipsNotNull(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", Identity: model.ColumnIdentity('a')})

	stmts := diffColumns("public.users", current, desired)
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

	stmts := diffConstraints("public.users", current, desired)
	assert.Equal(t, []string{"ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age > 0);"}, stmts)
}

func TestDiffConstraints_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)"})
	desired := orderedmap.New[string, *model.Constraint]()

	stmts := diffConstraints("public.users", current, desired)
	assert.Equal(t, []string{"ALTER TABLE public.users DROP CONSTRAINT chk_age;"}, stmts)
}

func TestDiffConstraints_change(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)"})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age >= 18)"})

	stmts := diffConstraints("public.users", current, desired)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.users DROP CONSTRAINT chk_age;", stmts[0])
	assert.Equal(t, "ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age >= 18);", stmts[1])
}

func TestDiffIndexes_add(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})

	stmts := diffIndexes(current, desired)
	assert.Equal(t, []string{"CREATE INDEX idx_name ON public.users USING btree (name);"}, stmts)
}

func TestDiffIndexes_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	desired := orderedmap.New[string, *model.Index]()

	stmts := diffIndexes(current, desired)
	assert.Equal(t, []string{"DROP INDEX public.idx_name;"}, stmts)
}

func TestDiffIndexes_change(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING hash (name)"})

	stmts := diffIndexes(current, desired)
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

	stmts := diffForeignKeys("public.orders", current, desired)
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

	stmts := diffForeignKeys("public.orders", current, desired)
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
	assert.True(t, equalFKDef(a, b))
}

func TestEqualFKDef_different(t *testing.T) {
	a := "FOREIGN KEY (user_id) REFERENCES users(id)"
	b := "FOREIGN KEY (user_id) REFERENCES orders(id)"
	assert.False(t, equalFKDef(a, b))
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

	stmts := DiffTables(current, desired)
	assert.Len(t, stmts, 2)
	assert.Contains(t, stmts[0], "CREATE TABLE")
	assert.Contains(t, stmts[1], "ADD CONSTRAINT fk_user")
}

func TestEqualFKDef_implicitPublicSchema(t *testing.T) {
	a := "FOREIGN KEY (user_id) REFERENCES users(id)"
	b := "FOREIGN KEY (user_id) REFERENCES public.users(id)"
	assert.True(t, equalFKDef(a, b))
}

func TestEqualFKDef_parseError(t *testing.T) {
	// When both fail to parse, falls back to string comparison
	assert.True(t, equalFKDef("not sql", "not sql"))
	assert.False(t, equalFKDef("not sql", "other"))
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

	stmts := diffForeignKeys("public.orders", current, desired)
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

	stmts := diffTable(current, desired)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "CREATE INDEX idx_new")
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

	stmts := DiffTables(current, desired)
	assert.Empty(t, stmts)
}
