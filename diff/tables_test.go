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
		Policies:    orderedmap.New[string, *model.Policy](),
	}
}

func TestDiffTables_newTable(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	tbl := newTable("public", "users")
	tbl.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	tbl.Constraints.Set("users_pkey", &model.Constraint{Name: "users_pkey", Definition: "PRIMARY KEY (id)"})
	desired.Set("public.users", tbl)

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE TABLE public.users")
}

func TestDiffTables_newTable_withExtras(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	tbl := newTable("public", "users")
	tbl.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	tbl.Indexes.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Table: "users", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	tbl.Comment = new("Users table")
	desired.Set("public.users", tbl)

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 3)
	assert.Contains(t, result.Stmts[0], "CREATE TABLE")
	assert.Contains(t, result.Stmts[1], "CREATE INDEX idx_name")
	assert.Contains(t, result.Stmts[2], "COMMENT ON TABLE")
}

func TestDiffTables_newTable_withExtras_perDirective(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	tbl := newTable("public", "users")
	tbl.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	tbl.Indexes.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Table: "users", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)", Concurrently: true})
	desired.Set("public.users", tbl)

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 2)
	assert.Contains(t, result.Stmts[0], "CREATE TABLE")
	assert.Equal(t, "CREATE INDEX CONCURRENTLY idx_name ON public.users USING btree (name);", result.Stmts[1])
	assert.True(t, result.HasConcurrently)
}

func TestDiffTables_modifyTable_addIndex_perDirective(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	ct := newTable("public", "users")
	ct.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	current.Set("public.users", ct)

	dt := newTable("public", "users")
	dt.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	dt.Indexes.Set("idx_id", &model.Index{Schema: "public", Name: "idx_id", Table: "users", Definition: "CREATE INDEX idx_id ON public.users USING btree (id)", Concurrently: true})
	desired.Set("public.users", dt)

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE INDEX CONCURRENTLY")
	assert.True(t, result.HasConcurrently)
}

func TestDiffTables_dropTable(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	current.Set("public.users", newTable("public", "users"))

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP TABLE public.users;"}, result.DropStmts)
}

func TestDiffTables_dropTable_withFK(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	users := newTable("public", "users")
	current.Set("public.users", users)

	posts := newTable("public", "posts")
	posts.ForeignKeys.Set("posts_user_id_fkey", &model.ForeignKey{
		Constraint: model.Constraint{Name: "posts_user_id_fkey", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)"},
		Schema:     "public",
		Table:      "posts",
	})
	current.Set("public.posts", posts)

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.posts DROP CONSTRAINT posts_user_id_fkey;"}, result.FKDropStmts)
	assert.Equal(t, []string{"DROP TABLE public.users;", "DROP TABLE public.posts;"}, result.DropStmts)
}

func TestDiffTables_dropTable_withFK_denied(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	users := newTable("public", "users")
	current.Set("public.users", users)

	posts := newTable("public", "posts")
	posts.ForeignKeys.Set("posts_user_id_fkey", &model.ForeignKey{
		Constraint: model.Constraint{Name: "posts_user_id_fkey", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)"},
		Schema:     "public",
		Table:      "posts",
	})
	current.Set("public.posts", posts)

	result, err := DiffTables(current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.FKDropStmts)
	assert.Empty(t, result.DropStmts)
	assert.Equal(t, []string{
		"-- skipped: DROP TABLE public.users;",
		"-- skipped: ALTER TABLE public.posts DROP CONSTRAINT posts_user_id_fkey;",
		"-- skipped: DROP TABLE public.posts;",
	}, result.DisallowedDropStmts)
}

func TestNormalizeDropChecker_nil(t *testing.T) {
	dc := normalizeDropChecker(nil)
	assert.False(t, dc.IsDropAllowed("table"))
}

func TestNormalizeDropChecker_nonNil(t *testing.T) {
	dc := normalizeDropChecker(allowAllDrops{})
	assert.True(t, dc.IsDropAllowed("table"))
}

func TestDiffTables_nilDropChecker(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	current.Set("public.users", newTable("public", "users"))

	// nil DropChecker should not panic, drops should be denied
	result, err := DiffTables(current, desired, nil)
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
}

func TestDiffTables_dropTable_denied(t *testing.T) {
	current := orderedmap.New[string, *model.Table]()
	desired := orderedmap.New[string, *model.Table]()

	current.Set("public.users", newTable("public", "users"))

	result, err := DiffTables(current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
	assert.Empty(t, result.DropStmts)
	assert.Equal(t, []string{"-- skipped: DROP TABLE public.users;"}, result.DisallowedDropStmts)
}

func TestDiffColumns_dropColumn_denied(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	stmts, disallowed, err := diffColumns("public.users", current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
	assert.Equal(t, []string{"-- skipped: ALTER TABLE public.users DROP COLUMN name;"}, disallowed)
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

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
}

func TestDiffColumns_addColumn(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
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

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users DROP COLUMN name;", stmts[0])
}

func TestDiffColumns_alterType(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "varchar(100)"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text"})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN name SET DATA TYPE text;", stmts[0])
}

func TestDiffColumns_alterType_withCollation(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "varchar(100)"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", Collation: new("en_US")})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], `COLLATE "en_US"`)
}

func TestDiffColumns_alterCollation_change(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", Collation: new("en_US")})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", Collation: new("fr_FR")})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{`ALTER TABLE public.users ALTER COLUMN name SET DATA TYPE text COLLATE "fr_FR";`}, stmts)
}

func TestDiffColumns_alterCollation_add(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", Collation: new("en_US")})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{`ALTER TABLE public.users ALTER COLUMN name SET DATA TYPE text COLLATE "en_US";`}, stmts)
}

func TestDiffColumns_alterCollation_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", Collation: new("en_US")})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text"})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users ALTER COLUMN name SET DATA TYPE text;"}, stmts)
}

func TestDiffColumns_alterCollation_unchanged(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", Collation: new("en_US")})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", Collation: new("en_US")})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffColumns_alterDefault_set(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("age", &model.Column{Name: "age", TypeName: "integer"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("age", &model.Column{Name: "age", TypeName: "integer", Default: new("0")})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN age SET DEFAULT 0;", stmts[0])
}

func TestDiffColumns_alterDefault_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("age", &model.Column{Name: "age", TypeName: "integer", Default: new("0")})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("age", &model.Column{Name: "age", TypeName: "integer"})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN age DROP DEFAULT;", stmts[0])
}

func TestDiffColumns_alterNotNull_set(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN name SET NOT NULL;", stmts[0])
}

func TestDiffColumns_alterNotNull_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text"})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ALTER COLUMN name DROP NOT NULL;", stmts[0])
}

func TestDiffColumns_renameNotNullConstraint(t *testing.T) {
	oldName := "users_name_nn_old"
	newName := "users_name_nn_new"

	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true, NotNullName: &oldName})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true, NotNullName: &newName})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users RENAME CONSTRAINT users_name_nn_old TO users_name_nn_new;"}, stmts)
}

func TestDiffColumns_notNullName_sameName_isNoOp(t *testing.T) {
	name := "users_name_nn"

	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true, NotNullName: &name})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true, NotNullName: &name})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffColumns_notNullName_addOrDrop_isNoOp(t *testing.T) {
	name := "users_name_nn"

	// Adding a name to an existing unnamed NOT NULL: no-op in v1.
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true, NotNullName: &name})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)

	// Dropping a name from a named NOT NULL: also no-op in v1.
	current = orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true, NotNullName: &name})

	desired = orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true})

	stmts, _, err = diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffColumns_renameNotNullConstraint_skipsIdentityColumn(t *testing.T) {
	// Identity columns are implicitly NOT NULL, and Table.SQL / addColumnSQL
	// intentionally do not render "CONSTRAINT <name> NOT NULL" on identity
	// columns. The rename branch must therefore also skip them — emitting a
	// RENAME CONSTRAINT here would surface a name the dumper hides.
	oldName := "users_id_nn_old"
	newName := "users_id_nn_new"

	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{
		Name: "id", TypeName: "integer", NotNull: true,
		NotNullName: &oldName, Identity: model.ColumnIdentity('a'),
	})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{
		Name: "id", TypeName: "integer", NotNull: true,
		NotNullName: &newName, Identity: model.ColumnIdentity('a'),
	})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffColumns_setNotNull_withDesiredName_ignoresName(t *testing.T) {
	// Nullable → NOT NULL with explicit desired name. v1 emits SET NOT NULL only;
	// the name is not applied (it would require PG18's standalone ADD CONSTRAINT
	// NOT NULL syntax).
	name := "users_name_nn"

	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true, NotNullName: &name})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users ALTER COLUMN name SET NOT NULL;"}, stmts)
}

func TestDiffColumns_dropNotNull_loseCurrentName(t *testing.T) {
	// NOT NULL with explicit current name → nullable. DROP NOT NULL implicitly
	// drops the constraint (and the name with it).
	name := "users_name_nn"

	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true, NotNullName: &name})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", TypeName: "text"})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users ALTER COLUMN name DROP NOT NULL;"}, stmts)
}

func TestDiffColumns_addColumn_withNotNullName(t *testing.T) {
	name := "users_email_nn"

	current := orderedmap.New[string, *model.Column]()
	desired := orderedmap.New[string, *model.Column]()
	desired.Set("email", &model.Column{Name: "email", TypeName: "text", NotNull: true, NotNullName: &name})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users ADD COLUMN email text CONSTRAINT users_email_nn NOT NULL;"}, stmts)
}

func TestDiffColumns_identitySkipsNotNull(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", Identity: model.ColumnIdentity('a')})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestAddColumnSQL_basic(t *testing.T) {
	col := &model.Column{Name: "name", TypeName: "text", NotNull: true}
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN name text NOT NULL;", addColumnSQL("public.users", col))
}

func TestAddColumnSQL_withDefault(t *testing.T) {
	col := &model.Column{Name: "active", TypeName: "boolean", Default: new("true")}
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN active boolean DEFAULT true;", addColumnSQL("public.users", col))
}

func TestAddColumnSQL_withCollation(t *testing.T) {
	col := &model.Column{Name: "name", TypeName: "text", Collation: new("en_US")}
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
	col := &model.Column{Name: "full", TypeName: "text", Generated: model.ColumnGenerated('s'), Default: new("first || last")}
	assert.Contains(t, addColumnSQL("public.users", col), "GENERATED ALWAYS AS (first || last) STORED")
}

func TestDiffConstraints_add(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: true})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age > 0);"}, stmts)
}

func TestDiffConstraints_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users DROP CONSTRAINT chk_age;"}, stmts)
}

func TestDiffConstraints_drop_denied(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()

	stmts, disallowed, err := diffConstraints("public.users", current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
	assert.Equal(t, []string{"-- skipped: ALTER TABLE public.users DROP CONSTRAINT chk_age;"}, disallowed)
}

func TestDiffConstraints_change_denied_alwaysExecutes(t *testing.T) {
	// Definition changes go through DROP+ADD regardless of policy because
	// PostgreSQL has no ALTER CONSTRAINT for definitions.
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age >= 18)", Validated: true})

	stmts, disallowed, err := diffConstraints("public.users", current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, disallowed)
	assert.Equal(t, []string{
		"ALTER TABLE public.users DROP CONSTRAINT chk_age;",
		"ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age >= 18);",
	}, stmts)
}

func TestDiffConstraints_change(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age >= 18)", Validated: true})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.users DROP CONSTRAINT chk_age;", stmts[0])
	assert.Equal(t, "ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age >= 18);", stmts[1])
}

func TestDiffConstraints_addNotValid(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: false})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age > 0) NOT VALID;", stmts[0])
}

func TestDiffConstraints_validatedToNotValid(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: false})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.users DROP CONSTRAINT chk_age;", stmts[0])
	assert.Equal(t, "ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age > 0) NOT VALID;", stmts[1])
}

func TestDiffConstraints_notValidToValidated(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: false})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: true})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users VALIDATE CONSTRAINT chk_age;", stmts[0])
}

func TestDiffConstraints_bothNotValid_noChange(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: false})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: false})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffConstraints_changeDefinitionAndValidated(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_age", &model.Constraint{Name: "chk_age", Definition: "CHECK (age >= 18)", Validated: false})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.users DROP CONSTRAINT chk_age;", stmts[0])
	assert.Equal(t, "ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age >= 18) NOT VALID;", stmts[1])
}

func TestDiffConstraints_renameAndNotValid(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_old", &model.Constraint{Name: "chk_old", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_new", &model.Constraint{Name: "chk_new", Definition: "CHECK (age > 0)", Validated: false, RenameFrom: new("chk_old")})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.users DROP CONSTRAINT chk_old;", stmts[0])
	assert.Equal(t, "ALTER TABLE public.users ADD CONSTRAINT chk_new CHECK (age > 0) NOT VALID;", stmts[1])
}

func TestDiffConstraints_renameAndChangeDefinition(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_old", &model.Constraint{Name: "chk_old", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_new", &model.Constraint{Name: "chk_new", Definition: "CHECK (age >= 18)", Validated: true, RenameFrom: new("chk_old")})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.users DROP CONSTRAINT chk_old;", stmts[0])
	assert.Equal(t, "ALTER TABLE public.users ADD CONSTRAINT chk_new CHECK (age >= 18);", stmts[1])
}

func TestDiffConstraints_renameAndValidate(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_old", &model.Constraint{Name: "chk_old", Definition: "CHECK (age > 0)", Validated: false})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_new", &model.Constraint{Name: "chk_new", Definition: "CHECK (age > 0)", Validated: true, RenameFrom: new("chk_old")})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.users RENAME CONSTRAINT chk_old TO chk_new;", stmts[0])
	assert.Equal(t, "ALTER TABLE public.users VALIDATE CONSTRAINT chk_new;", stmts[1])
}

func TestDiffConstraints_renameOnly(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_old", &model.Constraint{Name: "chk_old", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_new", &model.Constraint{Name: "chk_new", Definition: "CHECK (age > 0)", Validated: true, RenameFrom: new("chk_old")})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users RENAME CONSTRAINT chk_old TO chk_new;", stmts[0])
}

func TestDiffConstraints_renameAlreadyAppliedAndNotValid(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_new", &model.Constraint{Name: "chk_new", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_new", &model.Constraint{Name: "chk_new", Definition: "CHECK (age > 0)", Validated: false, RenameFrom: new("chk_old")})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.users DROP CONSTRAINT chk_new;", stmts[0])
	assert.Equal(t, "ALTER TABLE public.users ADD CONSTRAINT chk_new CHECK (age > 0) NOT VALID;", stmts[1])
}

func TestDiffIndexes_add(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"CREATE INDEX idx_name ON public.users USING btree (name);"}, idxResult.Stmts)
}

func TestDiffIndexes_drop(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	desired := orderedmap.New[string, *model.Index]()

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP INDEX public.idx_name;"}, idxResult.Stmts)
}

func TestDiffIndexes_drop_denied(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	desired := orderedmap.New[string, *model.Index]()

	idxResult, err := diffIndexes(current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, idxResult.Stmts)
	assert.Equal(t, []string{"-- skipped: DROP INDEX public.idx_name;"}, idxResult.DisallowedDropStmts)
}

func TestDiffIndexes_change_denied_alwaysExecutes(t *testing.T) {
	// Definition changes go through DROP+CREATE regardless of policy because
	// PostgreSQL has no ALTER INDEX for definitions.
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING hash (name)"})

	idxResult, err := diffIndexes(current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, idxResult.DisallowedDropStmts)
	assert.Len(t, idxResult.Stmts, 2)
	assert.Equal(t, "DROP INDEX public.idx_name;", idxResult.Stmts[0])
}

func TestDiffIndexes_change(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING hash (name)"})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, idxResult.Stmts, 2)
	assert.Equal(t, "DROP INDEX public.idx_name;", idxResult.Stmts[0])
	assert.Equal(t, "CREATE INDEX idx_name ON public.users USING hash (name);", idxResult.Stmts[1])
}

func TestDiffIndexes_add_uniqueConcurrently_perDirective(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE UNIQUE INDEX idx_name ON public.users USING btree (name)", Concurrently: true})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"CREATE UNIQUE INDEX CONCURRENTLY idx_name ON public.users USING btree (name);"}, idxResult.Stmts)
}

func TestDiffIndexes_add_perIndexConcurrently(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)", Concurrently: true})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"CREATE INDEX CONCURRENTLY idx_name ON public.users USING btree (name);"}, idxResult.Stmts)
}

func TestDiffIndexes_drop_pureDrop_neverConcurrently(t *testing.T) {
	// Pure drops (index removed from desired) never use CONCURRENTLY because
	// catalog-derived indexes don't carry the Concurrently directive.
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	desired := orderedmap.New[string, *model.Index]()

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"DROP INDEX public.idx_name;"}, idxResult.Stmts)
}

func TestDiffIndexes_change_perIndexConcurrently(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)"})
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING hash (name)", Concurrently: true})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, idxResult.Stmts, 2)
	assert.Equal(t, "DROP INDEX CONCURRENTLY public.idx_name;", idxResult.Stmts[0])
	assert.Equal(t, "CREATE INDEX CONCURRENTLY idx_name ON public.users USING hash (name);", idxResult.Stmts[1])
}

func TestDiffIndexes_mixedConcurrently(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)", Concurrently: true})
	desired.Set("idx_email", &model.Index{Schema: "public", Name: "idx_email", Definition: "CREATE INDEX idx_email ON public.users USING btree (email)"})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, idxResult.Stmts, 2)
	assert.Equal(t, "CREATE INDEX CONCURRENTLY idx_name ON public.users USING btree (name);", idxResult.Stmts[0])
	assert.Equal(t, "CREATE INDEX idx_email ON public.users USING btree (email);", idxResult.Stmts[1])
}

func TestDiffIndexes_rename_perIndexConcurrently(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("old_idx", &model.Index{Schema: "public", Name: "old_idx", Table: "users", Definition: "CREATE INDEX old_idx ON public.users USING btree (name)"})

	oldName := "old_idx"
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", RenameFrom: &oldName, Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (name)", Concurrently: true})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	// Rename should NOT use CONCURRENTLY even with per-index directive
	assert.Equal(t, []string{"ALTER INDEX public.old_idx RENAME TO new_idx;"}, idxResult.Stmts)
	assert.False(t, idxResult.HasConcurrently)
}

func TestDiffIndexes_partialIndex_concurrently(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_active", &model.Index{Schema: "public", Name: "idx_active", Definition: "CREATE INDEX idx_active ON public.users USING btree (name) WHERE (active = true)", Concurrently: true})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, idxResult.Stmts, 1)
	assert.Contains(t, idxResult.Stmts[0], "CREATE INDEX CONCURRENTLY")
	assert.Contains(t, idxResult.Stmts[0], "WHERE")
	assert.True(t, idxResult.HasConcurrently)
}

func TestDiffIndexes_expressionIndex_concurrently(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_lower", &model.Index{Schema: "public", Name: "idx_lower", Definition: "CREATE INDEX idx_lower ON public.users USING btree (lower(name))", Concurrently: true})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, idxResult.Stmts, 1)
	assert.Contains(t, idxResult.Stmts[0], "CREATE INDEX CONCURRENTLY")
	assert.Contains(t, idxResult.Stmts[0], "lower")
	assert.True(t, idxResult.HasConcurrently)
}

func TestDiffIndexes_hasConcurrently_directive(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx_name", &model.Index{Schema: "public", Name: "idx_name", Definition: "CREATE INDEX idx_name ON public.users USING btree (name)", Concurrently: true})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.True(t, idxResult.HasConcurrently)
}

func TestCreateIndexSQL_parseError(t *testing.T) {
	_, err := createIndexSQL("NOT VALID SQL {{{{", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse index definition")
}

func TestCreateIndexSQL_notIndexStmt(t *testing.T) {
	_, err := createIndexSQL("SELECT 1", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected IndexStmt")
}

func TestDropIndexSQL_concurrently(t *testing.T) {
	stmt, err := dropIndexSQL("public", "idx_name", true)
	require.NoError(t, err)
	assert.Equal(t, "DROP INDEX CONCURRENTLY public.idx_name;", stmt)
}

func TestDropIndexSQL_noConcurrently(t *testing.T) {
	stmt, err := dropIndexSQL("public", "idx_name", false)
	require.NoError(t, err)
	assert.Equal(t, "DROP INDEX public.idx_name;", stmt)
}

func TestDiffForeignKeys_add(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})

	_, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, addStmts, 1)
	assert.Contains(t, addStmts[0], "ADD CONSTRAINT fk_user")
	assert.NotContains(t, addStmts[0], "NOT VALID")
}

func TestDiffForeignKeys_addNotValid(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: false},
		Schema:     "public",
		Table:      "orders",
	})

	_, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, addStmts, 1)
	assert.Contains(t, addStmts[0], "ADD CONSTRAINT fk_user")
	assert.Contains(t, addStmts[0], "NOT VALID")
}

func TestDiffForeignKeys_drop(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.orders DROP CONSTRAINT fk_user;"}, dropStmts)
	assert.Empty(t, addStmts)
}

func TestDiffForeignKeys_drop_denied(t *testing.T) {
	// Pure FK removals (FK absent from desired while the owning table stays)
	// honor --allow-drop=foreign_key.
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()

	dropStmts, addStmts, disallowed, err := diffForeignKeys("public.orders", "public", current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, dropStmts)
	assert.Empty(t, addStmts)
	assert.Equal(t, []string{"-- skipped: ALTER TABLE public.orders DROP CONSTRAINT fk_user;"}, disallowed)
}

func TestDiffForeignKeys_change_denied_alwaysExecutes(t *testing.T) {
	// FK definition changes still go through DROP+ADD even when foreign_key
	// drops are denied.
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

	dropStmts, addStmts, disallowed, err := diffForeignKeys("public.orders", "public", current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, disallowed)
	assert.Equal(t, []string{"ALTER TABLE public.orders DROP CONSTRAINT fk_user;"}, dropStmts)
	require.Len(t, addStmts, 1)
	assert.Contains(t, addStmts[0], "ON DELETE CASCADE")
}

func TestDiffForeignKeys_renamedAndChanged_denied_alwaysExecutes(t *testing.T) {
	// Renamed FK with definition change → DROP (old name) + ADD (new name).
	// The new name is in desired so it's not a "pure removal" — deny does not
	// suppress it.
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_old", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_old", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_new", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_new", Definition: "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE", Validated: true, RenameFrom: new("fk_old")},
		Schema:     "public",
		Table:      "orders",
	})

	dropStmts, addStmts, disallowed, err := diffForeignKeys("public.orders", "public", current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, disallowed)
	assert.Equal(t, []string{"ALTER TABLE public.orders DROP CONSTRAINT fk_old;"}, dropStmts)
	require.Len(t, addStmts, 1)
	assert.Contains(t, addStmts[0], "ADD CONSTRAINT fk_new")
	assert.Contains(t, addStmts[0], "ON DELETE CASCADE")
}

func TestDiffConstraints_renamedAndChanged_denied_alwaysExecutes(t *testing.T) {
	// Renamed-with-definition-change goes through DROP (old name) + ADD (new
	// name). Since the new name still exists in desired, it is not a "pure
	// removal" and is not suppressed by the deny policy.
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("chk_old", &model.Constraint{Name: "chk_old", Definition: "CHECK (age > 0)", Validated: true})
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("chk_new", &model.Constraint{Name: "chk_new", Definition: "CHECK (age >= 18)", Validated: true, RenameFrom: new("chk_old")})

	stmts, disallowed, err := diffConstraints("public.users", current, desired, denyAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, disallowed)
	require.Len(t, stmts, 2)
	assert.Equal(t, "ALTER TABLE public.users DROP CONSTRAINT chk_old;", stmts[0])
	assert.Contains(t, stmts[1], "ADD CONSTRAINT chk_new")
}

func TestDiffComments_tableComment_add(t *testing.T) {
	current := newTable("public", "users")
	desired := newTable("public", "users")
	desired.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	current.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer"})
	desired.Comment = new("Users table")

	stmts := diffComments(current, desired)
	assert.Equal(t, []string{"COMMENT ON TABLE public.users IS 'Users table';"}, stmts)
}

func TestDiffComments_tableComment_drop(t *testing.T) {
	current := newTable("public", "users")
	current.Comment = new("Users table")
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
	desired.Columns.Set("name", &model.Column{Name: "name", TypeName: "text", Comment: new("User name")})

	stmts := diffComments(current, desired)
	assert.Equal(t, []string{"COMMENT ON COLUMN public.users.name IS 'User name';"}, stmts)
}

func TestDiffComments_columnComment_drop(t *testing.T) {
	current := newTable("public", "users")
	current.Columns.Set("name", &model.Column{Name: "name", TypeName: "text", Comment: new("User name")})
	desired := newTable("public", "users")
	desired.Columns.Set("name", &model.Column{Name: "name", TypeName: "text"})

	stmts := diffComments(current, desired)
	assert.Equal(t, []string{"COMMENT ON COLUMN public.users.name IS NULL;"}, stmts)
}

func TestDiffComments_newColumn(t *testing.T) {
	current := newTable("public", "users")
	desired := newTable("public", "users")
	desired.Columns.Set("name", &model.Column{Name: "name", TypeName: "text", Comment: new("new col comment")})

	stmts := diffComments(current, desired)
	assert.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "COMMENT ON COLUMN")
}

func TestEqualPtr(t *testing.T) {
	assert.True(t, equalPtr[string](nil, nil))
	assert.False(t, equalPtr(new("a"), nil))
	assert.False(t, equalPtr(nil, new("a")))
	assert.True(t, equalPtr(new("a"), new("a")))
	assert.False(t, equalPtr(new("a"), new("b")))
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
	assert.False(t, equalDefault(new("0"), nil))
	assert.False(t, equalDefault(nil, new("0")))
	assert.True(t, equalDefault(new("0"), new("0")))
	assert.True(t, equalDefault(new("'hello'::text"), new("'hello'")))
	assert.False(t, equalDefault(new("0"), new("1")))
}

func TestEqualDefault_currentTimeCastStripped(t *testing.T) {
	// pg_get_expr emits `'00:00:00'::time without time zone` on a time column
	// DEFAULT; user typically writes the bare literal.
	assert.True(t, equalDefault(
		new("'00:00:00'::time without time zone"),
		new("'00:00:00'"),
	))
}

func TestEqualDefault_currentDateCastStripped(t *testing.T) {
	assert.True(t, equalDefault(
		new("'2020-01-01'::date"),
		new("'2020-01-01'"),
	))
}

func TestEqualDefault_currentTimestampCastStripped(t *testing.T) {
	assert.True(t, equalDefault(
		new("'2020-01-01 00:00:00'::timestamp without time zone"),
		new("'2020-01-01 00:00:00'"),
	))
}

func TestEqualDefault_currentNegativeIntCastStripped(t *testing.T) {
	// Negative integer DEFAULT: DB emits `'-40'::integer`, user writes `-40`.
	// Requires the numeric Sval→Ival coercion alongside the cast strip.
	assert.True(t, equalDefault(
		new("'-40'::integer"),
		new("-40"),
	))
}

func TestEqualDefault_currentNumericFloatCastStripped(t *testing.T) {
	assert.True(t, equalDefault(
		new("'12.34'::numeric"),
		new("12.34"),
	))
}

func TestEqualDefault_currentNestedCastStripped(t *testing.T) {
	// Cast nested inside an expression (e.g. `(now() - '18 years'::interval)`):
	// top-level is AExpr, not TypeCast, so the old parseDefault top-level strip
	// didn't reach this position.
	assert.True(t, equalDefault(
		new("now() - '18 years'::interval"),
		new("now() - '18 years'"),
	))
}

func TestEqualDefault_bothExplicitCastsMatch(t *testing.T) {
	assert.True(t, equalDefault(
		new("'0'::integer"),
		new("'0'::integer"),
	))
}

func TestEqualDefault_currentStringCastVsDesiredNumericCast(t *testing.T) {
	// Asymmetric A_Const kind under matching top-level casts: current's
	// cast wraps an Sval ("0"), desired's cast wraps an Ival (0). Both
	// casts get stripped symmetrically; the Sval→numeric coercion must
	// look through the peer's still-present TypeCast to decide that the
	// peer "will be" numeric after its own strip — otherwise the surviving
	// Sval `'0'` diffs against the desired bare `0`.
	assert.True(t, equalDefault(
		new("'0'::integer"),
		new("0::integer"),
	))
}

func TestEqualDefault_castsDifferTypesStillEqual(t *testing.T) {
	// Unlike equalConstraintDef, equalDefault treats `'0'::bigint` and
	// `'0'::integer` as equal: the symmetric top-level cast strip applies
	// to both sides (pg_get_expr always wraps DEFAULTs in a cast, and the
	// column type — not the literal's cast — drives the eventual storage),
	// so the two collapse to the same `'0'` and compare equal.
	assert.True(t, equalDefault(
		new("'0'::bigint"),
		new("'0'::integer"),
	))
}

func TestEqualDefault_desiredCastCurrentBareStillEqual(t *testing.T) {
	// Symmetric top-level cast strip also covers the reverse direction:
	// user wrote `0::integer` but DB stored the value natively without a
	// cast (pg_get_expr returns just `0`). They compare equal so this case
	// does not produce a perpetual SET DEFAULT diff loop on every apply.
	assert.True(t, equalDefault(
		new("0"),
		new("0::integer"),
	))
}

func TestEqualDefault_currentBigintCastStripped(t *testing.T) {
	// Bigint values that don't fit in int32 take the Fval fallback path
	// inside numericAConstFromString — verify the coerce still matches
	// the user's bare numeric.
	assert.True(t, equalDefault(
		new("'9000000000'::bigint"),
		new("9000000000"),
	))
}

func TestEqualDefault_currentLargeNumericCastStripped(t *testing.T) {
	// Numeric literals beyond float64 range exercise the ErrRange-accept
	// branch in numericAConstFromString.
	assert.True(t, equalDefault(
		new("'1e400'::numeric"),
		new("1e400"),
	))
}

func TestEqualDefault_customNumericNamedTypeNotCoerced(t *testing.T) {
	// Same guard as the CHECK-constraint version: a user-defined type
	// matching a built-in numeric name does not gate the Sval→numeric
	// coercion, so the surviving Sval still compares unequal to a
	// desired bare integer.
	assert.False(t, equalDefault(
		new("'0'::myapp.int4"),
		new("0"),
	))
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

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, result.Stmts, 1)
	assert.Contains(t, result.Stmts[0], "CREATE TABLE")
	assert.Len(t, result.FKAddStmts, 1)
	assert.Contains(t, result.FKAddStmts[0], "ADD CONSTRAINT fk_user")
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
	assert.True(t, equalDefault(new(")))invalid"), new(")))invalid")))
	assert.False(t, equalDefault(new(")))invalid"), new(")))other")))
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

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, append(dropStmts, addStmts...), 2)
	assert.Equal(t, "ALTER TABLE public.orders DROP CONSTRAINT fk_user;", dropStmts[0])
	assert.Contains(t, addStmts[0], "ADD CONSTRAINT fk_user")
}

func TestDiffForeignKeys_validatedToNotValid(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: false},
		Schema:     "public",
		Table:      "orders",
	})

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, dropStmts, 1)
	assert.Equal(t, "ALTER TABLE public.orders DROP CONSTRAINT fk_user;", dropStmts[0])
	assert.Len(t, addStmts, 1)
	assert.Contains(t, addStmts[0], "NOT VALID")
}

func TestDiffForeignKeys_notValidToValidated(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: false},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, dropStmts)
	assert.Len(t, addStmts, 1)
	assert.Equal(t, "ALTER TABLE public.orders VALIDATE CONSTRAINT fk_user;", addStmts[0])
}

func TestDiffForeignKeys_bothNotValid_noChange(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: false},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: false},
		Schema:     "public",
		Table:      "orders",
	})

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, dropStmts)
	assert.Empty(t, addStmts)
}

func TestDiffForeignKeys_changeDefinitionAndValidated(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_user", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Definition: "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE", Validated: false},
		Schema:     "public",
		Table:      "orders",
	})

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, dropStmts, 1)
	assert.Equal(t, "ALTER TABLE public.orders DROP CONSTRAINT fk_user;", dropStmts[0])
	assert.Len(t, addStmts, 1)
	assert.Contains(t, addStmts[0], "ON DELETE CASCADE")
	assert.Contains(t, addStmts[0], "NOT VALID")
}

func TestDiffForeignKeys_renameAndValidate(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_old", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_old", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: false},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_new", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_new", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true, RenameFrom: new("fk_old")},
		Schema:     "public",
		Table:      "orders",
	})

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, dropStmts)
	assert.Len(t, addStmts, 2)
	assert.Equal(t, "ALTER TABLE public.orders RENAME CONSTRAINT fk_old TO fk_new;", addStmts[0])
	assert.Equal(t, "ALTER TABLE public.orders VALIDATE CONSTRAINT fk_new;", addStmts[1])
}

func TestDiffForeignKeys_renameOnly(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_old", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_old", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_new", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_new", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true, RenameFrom: new("fk_old")},
		Schema:     "public",
		Table:      "orders",
	})

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, dropStmts)
	assert.Len(t, addStmts, 1)
	assert.Equal(t, "ALTER TABLE public.orders RENAME CONSTRAINT fk_old TO fk_new;", addStmts[0])
}

func TestDiffForeignKeys_renameAndNotValid(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_old", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_old", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_new", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_new", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: false, RenameFrom: new("fk_old")},
		Schema:     "public",
		Table:      "orders",
	})

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, dropStmts, 1)
	assert.Equal(t, "ALTER TABLE public.orders DROP CONSTRAINT fk_old;", dropStmts[0])
	assert.Len(t, addStmts, 1)
	assert.Contains(t, addStmts[0], "ADD CONSTRAINT fk_new")
	assert.Contains(t, addStmts[0], "NOT VALID")
}

func TestDiffForeignKeys_renameAlreadyAppliedAndNotValid(t *testing.T) {
	// Rename fk_old→fk_new was already applied in DB (current has fk_new).
	// Desired still has RenameFrom but also changes Validated.
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_new", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_new", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_new", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_new", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: false, RenameFrom: new("fk_old")},
		Schema:     "public",
		Table:      "orders",
	})

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, dropStmts, 1)
	assert.Equal(t, "ALTER TABLE public.orders DROP CONSTRAINT fk_new;", dropStmts[0])
	assert.Len(t, addStmts, 1)
	assert.Contains(t, addStmts[0], "ADD CONSTRAINT fk_new")
	assert.Contains(t, addStmts[0], "NOT VALID")
}

func TestDiffForeignKeys_renameAndChangeDefinition(t *testing.T) {
	current := orderedmap.New[string, *model.ForeignKey]()
	current.Set("fk_old", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_old", Definition: "FOREIGN KEY (user_id) REFERENCES users(id)", Validated: true},
		Schema:     "public",
		Table:      "orders",
	})
	desired := orderedmap.New[string, *model.ForeignKey]()
	desired.Set("fk_new", &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_new", Definition: "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE", Validated: true, RenameFrom: new("fk_old")},
		Schema:     "public",
		Table:      "orders",
	})

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, dropStmts, 1)
	assert.Equal(t, "ALTER TABLE public.orders DROP CONSTRAINT fk_old;", dropStmts[0])
	assert.Len(t, addStmts, 1)
	assert.Contains(t, addStmts[0], "ADD CONSTRAINT fk_new")
	assert.Contains(t, addStmts[0], "ON DELETE CASCADE")
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

	tableResult, err := diffTable(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Len(t, tableResult.Stmts, 1)
	assert.Contains(t, tableResult.Stmts[0], "CREATE INDEX idx_new")
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

	_, err := diffTable(current, desired, allowAllDrops{})
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

	_, err := diffTable(current, desired, allowAllDrops{})
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

	_, err := diffTable(current, desired, allowAllDrops{})
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

	_, err := diffTable(current, desired, allowAllDrops{})
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

	_, err := diffTable(current, desired, allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source foreign key")
}

func TestDiffTable_columnRenameRewritesDependents(t *testing.T) {
	current := newTable("public", "users")
	current.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	current.Columns.Set("name", &model.Column{Name: "name", TypeName: "text", NotNull: true, Comment: new("legacy")})
	current.Indexes.Set("idx_users_name", &model.Index{
		Schema:     "public",
		Name:       "idx_users_name",
		Table:      "users",
		Definition: "CREATE INDEX idx_users_name ON public.users USING btree (name)",
	})

	desired := newTable("public", "users")
	desired.Columns.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})
	oldName := "name"
	desired.Columns.Set("display_name", &model.Column{
		Name:       "display_name",
		TypeName:   "text",
		NotNull:    true,
		RenameFrom: &oldName,
		Comment:    new("legacy"),
	})
	desired.Indexes.Set("idx_users_name", &model.Index{
		Schema:     "public",
		Name:       "idx_users_name",
		Table:      "users",
		Definition: "CREATE INDEX idx_users_name ON public.users USING btree (display_name)",
	})

	tableResult, err := diffTable(current, desired, allowAllDrops{})
	require.NoError(t, err)

	// Only the RENAME COLUMN should be emitted; no redundant DROP/CREATE
	// INDEX, and no comment statement (the comment is preserved through
	// RENAME).
	require.Len(t, tableResult.Stmts, 1)
	assert.Equal(t, "ALTER TABLE public.users RENAME COLUMN name TO display_name;", tableResult.Stmts[0])
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

func TestEqualConstraintDef_currentIntegerCastStripped(t *testing.T) {
	// pg_get_constraintdef adds ::integer to bare numeric literals on int
	// columns. When desired has no cast, treat them as equal.
	assert.True(t, equalConstraintDef(
		"CHECK (val > '0'::integer)",
		"CHECK (val > '0')",
	))
}

func TestEqualConstraintDef_currentTimeCastStripped(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK (t >= '00:00:00'::time without time zone)",
		"CHECK (t >= '00:00:00')",
	))
}

func TestEqualConstraintDef_currentDateCastStripped(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK (d >= '2020-01-01'::date)",
		"CHECK (d >= '2020-01-01')",
	))
}

func TestEqualConstraintDef_currentTimestampCastStripped(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK (ts >= '2020-01-01 00:00:00'::timestamp without time zone)",
		"CHECK (ts >= '2020-01-01 00:00:00')",
	))
}

func TestEqualConstraintDef_bothExplicitCastsMatch(t *testing.T) {
	// When desired explicitly has the same cast as current, normal pairwise
	// comparison succeeds.
	assert.True(t, equalConstraintDef(
		"CHECK (val > '0'::integer)",
		"CHECK (val > '0'::integer)",
	))
}

func TestEqualConstraintDef_castsDifferTypes(t *testing.T) {
	// Both sides have a cast but the target types differ → not equal.
	assert.False(t, equalConstraintDef(
		"CHECK (val > '0'::bigint)",
		"CHECK (val > '0'::integer)",
	))
}

func TestEqualConstraintDef_desiredCastCurrentBare(t *testing.T) {
	// Asymmetric rule fires only when current has the extra cast.
	// If desired has a cast and current doesn't, they remain unequal.
	assert.False(t, equalConstraintDef(
		"CHECK (val > '0')",
		"CHECK (val > '0'::integer)",
	))
}

func TestEqualConstraintDef_currentCastInsideBoolExpr(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK ((a > 0) AND (b >= '00:00:00'::time without time zone))",
		"CHECK (a > 0 AND b >= '00:00:00')",
	))
}

func TestEqualConstraintDef_currentCastOnColumnRefStripped(t *testing.T) {
	// Cast on a column expression is also stripped when desired has none.
	assert.True(t, equalConstraintDef(
		"CHECK ((col)::integer > 0)",
		"CHECK (col > 0)",
	))
}

func TestEqualConstraintDef_currentCastInArrayElementStripped(t *testing.T) {
	// pg_get_constraintdef converts IN to = ANY(ARRAY[...]) and may add
	// per-element casts on non-text types.
	assert.True(t, equalConstraintDef(
		"CHECK ((val = ANY (ARRAY['0'::integer, '1'::integer])))",
		"CHECK (val IN ('0', '1'))",
	))
}

func TestEqualConstraintDef_currentCastInCaseResultStripped(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK ((CASE WHEN (val > 0) THEN '1'::integer ELSE '0'::integer END) = '1')",
		"CHECK ((CASE WHEN val > 0 THEN '1' ELSE '0' END) = '1')",
	))
}

func TestEqualConstraintDef_currentCastInArrayLiteralStripped(t *testing.T) {
	// ARRAY[...] outside of = ANY stays as AArrayExpr after normalization;
	// per-element casts on an array-column comparison get stripped too.
	assert.True(t, equalConstraintDef(
		"CHECK ((tags = ARRAY['1'::integer, '2'::integer]))",
		"CHECK (tags = ARRAY['1', '2'])",
	))
}

func TestEqualConstraintDef_currentNegativeIntCastStripped(t *testing.T) {
	// pg_get_constraintdef emits negative integer literals as `'-40'::integer`
	// to dodge the unary-minus precedence trap; user writes bare `-40`.
	// The cast strip plus Sval→Ival coercion makes them compare equal.
	assert.True(t, equalConstraintDef(
		"CHECK (vacationhours >= '-40'::integer AND vacationhours <= 240)",
		"CHECK (vacationhours >= -40 AND vacationhours <= 240)",
	))
}

func TestEqualConstraintDef_currentPositiveIntStringCastStripped(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK (val >= '40'::integer)",
		"CHECK (val >= 40)",
	))
}

func TestEqualConstraintDef_currentBigintCastStripped(t *testing.T) {
	// Value that fits in int64 but exceeds int32 should fall back to Fval form.
	assert.True(t, equalConstraintDef(
		"CHECK (val >= '9000000000'::bigint)",
		"CHECK (val >= 9000000000)",
	))
}

func TestEqualConstraintDef_currentNumericFloatCastStripped(t *testing.T) {
	assert.True(t, equalConstraintDef(
		"CHECK (price >= '12.34'::numeric)",
		"CHECK (price >= 12.34)",
	))
}

func TestEqualConstraintDef_currentNegativeFloatCastStripped(t *testing.T) {
	// pg_get_constraintdef emits negative float/numeric literals as
	// `'-12.34'::numeric` for the same precedence-trap reason as integers.
	assert.True(t, equalConstraintDef(
		"CHECK (price >= '-12.34'::numeric)",
		"CHECK (price >= -12.34)",
	))
}

func TestEqualConstraintDef_currentLargeNumericCastStripped(t *testing.T) {
	// Numeric literals beyond float64 range (PG `numeric` carries arbitrary
	// precision). The Sval→Fval coercion must accept these so the strip is
	// not silently undone for very large values.
	assert.True(t, equalConstraintDef(
		"CHECK (price <= '1e400'::numeric)",
		"CHECK (price <= 1e400)",
	))
}

func TestEqualConstraintDef_customNumericNamedTypeNotCoerced(t *testing.T) {
	// A user-defined type that happens to share a built-in numeric name
	// (e.g. `myapp.int4`) should NOT trigger the Sval→numeric coercion.
	// alignCurrentCasts still strips the `::myapp.int4` wrapper (the strip
	// itself is unconditional, per the asymmetric rule from #201), but the
	// surviving A_Const{Sval "0"} is left as-is — so it deparses to `'0'`
	// and compares unequal to the desired bare integer `0` (A_Const{Ival}).
	// This is what keeps custom-type casts from silently matching unrelated
	// built-in numeric desired forms.
	assert.False(t, equalConstraintDef(
		"CHECK (val > '0'::myapp.int4)",
		"CHECK (val > 0)",
	))
}

func TestEqualConstraintDef_currentSmallintCastStripped(t *testing.T) {
	// pg_query canonicalises `smallint` to `int2` in the catalog form, so
	// isNumericTypeName must recognise both. This exercises the int2 path.
	assert.True(t, equalConstraintDef(
		"CHECK (val >= '0'::smallint)",
		"CHECK (val >= 0)",
	))
}

func TestEqualConstraintDef_currentDoublePrecisionCastStripped(t *testing.T) {
	// `double precision` canonicalises to `float8`; verify the float8 path.
	assert.True(t, equalConstraintDef(
		"CHECK (val >= '0.5'::double precision)",
		"CHECK (val >= 0.5)",
	))
}

func TestEqualConstraintDef_castsDifferNumericTypesStillDifferent(t *testing.T) {
	// When both sides carry casts but the target types differ, the asymmetric
	// rule does not fire and the difference still surfaces — even for numeric
	// types where the Sval→numeric coercion would otherwise erase the type.
	assert.False(t, equalConstraintDef(
		"CHECK (val > '0'::bigint)",
		"CHECK (val > '0'::integer)",
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

	stmts, _, err := diffConstraints("public.items", current, desired, allowAllDrops{})
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

	stmts, _, err := diffConstraints("public.items", current, desired, allowAllDrops{})
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

	stmts, _, err := diffConstraints("public.items", current, desired, allowAllDrops{})
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

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
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

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
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

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users RENAME TO accounts;"}, result.Stmts)
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

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
}

func TestDiffColumns_renameColumn_selfRename_skipped(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	oldName := "name"
	desired := orderedmap.New[string, *model.Column]()
	desired.Set("name", &model.Column{Name: "name", RenameFrom: &oldName, TypeName: "text"})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffConstraints_rename_selfRename_skipped(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("con", &model.Constraint{Name: "con", Definition: "UNIQUE (code)"})

	oldName := "con"
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("con", &model.Constraint{Name: "con", RenameFrom: &oldName, Definition: "UNIQUE (code)"})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffIndexes_rename_selfRename_skipped(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("idx", &model.Index{Schema: "public", Name: "idx", Table: "users", Definition: "CREATE INDEX idx ON public.users USING btree (name)"})

	oldName := "idx"
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("idx", &model.Index{Schema: "public", Name: "idx", RenameFrom: &oldName, Table: "users", Definition: "CREATE INDEX idx ON public.users USING btree (name)"})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, idxResult.Stmts)
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

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, dropStmts)
	assert.Empty(t, addStmts)
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

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, result.Stmts)
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

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	// Should only rename the table, no DROP/CREATE index
	assert.Equal(t, []string{"ALTER TABLE public.users RENAME TO accounts;"}, result.Stmts)
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

	result, err := DiffTables(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.orders RENAME TO purchases;"}, result.Stmts)
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

	_, err := DiffTables(current, desired, allowAllDrops{})
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

	_, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
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

	_, err := DiffTables(current, desired, allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cross-schema rename")
}

func TestDiffColumns_renameColumn(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("name", &model.Column{Name: "name", TypeName: "text"})

	oldName := "name"
	desired := orderedmap.New[string, *model.Column]()
	desired.Set("display_name", &model.Column{Name: "display_name", RenameFrom: &oldName, TypeName: "text"})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users RENAME COLUMN name TO display_name;"}, stmts)
}

func TestDiffColumns_renameColumn_alreadyApplied(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("display_name", &model.Column{Name: "display_name", TypeName: "text"})

	oldName := "name"
	desired := orderedmap.New[string, *model.Column]()
	desired.Set("display_name", &model.Column{Name: "display_name", RenameFrom: &oldName, TypeName: "text"})

	stmts, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffConstraints_rename(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("old_con", &model.Constraint{Name: "old_con", Definition: "UNIQUE (code)"})

	oldName := "old_con"
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("new_con", &model.Constraint{Name: "new_con", RenameFrom: &oldName, Definition: "UNIQUE (code)"})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER TABLE public.users RENAME CONSTRAINT old_con TO new_con;"}, stmts)
}

func TestDiffIndexes_rename(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("old_idx", &model.Index{Schema: "public", Name: "old_idx", Table: "users", Definition: "CREATE INDEX old_idx ON public.users USING btree (name)"})

	oldName := "old_idx"
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", RenameFrom: &oldName, Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (name)"})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{"ALTER INDEX public.old_idx RENAME TO new_idx;"}, idxResult.Stmts)
}

func TestDiffConstraints_rename_alreadyApplied(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()
	current.Set("new_con", &model.Constraint{Name: "new_con", Definition: "UNIQUE (code)"})

	oldName := "old_con"
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("new_con", &model.Constraint{Name: "new_con", RenameFrom: &oldName, Definition: "UNIQUE (code)"})

	stmts, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestDiffConstraints_rename_sourceNotFound(t *testing.T) {
	current := orderedmap.New[string, *model.Constraint]()

	oldName := "nonexistent"
	desired := orderedmap.New[string, *model.Constraint]()
	desired.Set("new_con", &model.Constraint{Name: "new_con", RenameFrom: &oldName, Definition: "UNIQUE (code)"})

	_, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source constraint")
}

func TestDiffIndexes_rename_alreadyApplied(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()
	current.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (name)"})

	oldName := "old_idx"
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", RenameFrom: &oldName, Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (name)"})

	idxResult, err := diffIndexes(current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, idxResult.Stmts)
}

func TestDiffIndexes_rename_sourceNotFound(t *testing.T) {
	current := orderedmap.New[string, *model.Index]()

	oldName := "nonexistent"
	desired := orderedmap.New[string, *model.Index]()
	desired.Set("new_idx", &model.Index{Schema: "public", Name: "new_idx", RenameFrom: &oldName, Table: "users", Definition: "CREATE INDEX new_idx ON public.users USING btree (name)"})

	_, err := diffIndexes(current, desired, allowAllDrops{})
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

	dropStmts, addStmts, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, dropStmts)
	assert.Equal(t, []string{"ALTER TABLE public.orders RENAME CONSTRAINT old_fk TO new_fk;"}, addStmts)
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

	_, _, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
	require.NoError(t, err)
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

	_, _, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
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

	_, _, err := diffConstraints("public.users", current, desired, allowAllDrops{})
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

	_, err := diffIndexes(current, desired, allowAllDrops{})
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

	_, _, _, err := diffForeignKeys("public.orders", "public", current, desired, allowAllDrops{})
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

	_, err := DiffTables(current, desired, allowAllDrops{})
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

	_, err := DiffTables(current, desired, allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source column")
}

func TestDiffColumns_renameColumn_sourceNotFound(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()

	oldName := "nonexistent"
	desired := orderedmap.New[string, *model.Column]()
	desired.Set("display_name", &model.Column{Name: "display_name", RenameFrom: &oldName, TypeName: "text"})

	_, _, err := diffColumns("public.users", current, desired, allowAllDrops{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename source column")
}

func TestUpdateIndexName_parseError(t *testing.T) {
	_, err := updateIndexName("NOT VALID SQL", "new_idx")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse index definition")
}

func TestUpdateIndexName_notIndexStmt(t *testing.T) {
	_, err := updateIndexName("SELECT 1", "new_idx")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected IndexStmt")
}

func TestUpdateIndexName_success(t *testing.T) {
	got, err := updateIndexName("CREATE INDEX old_idx ON t (id)", "new_idx")
	require.NoError(t, err)
	assert.Contains(t, got, "new_idx")
	assert.NotContains(t, got, "old_idx")
}

func TestUpdateIndexTableName_parseError(t *testing.T) {
	_, err := updateIndexTableName("NOT VALID SQL", "new_table")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse index definition")
}

func TestUpdateIndexTableName_notIndexStmt(t *testing.T) {
	_, err := updateIndexTableName("SELECT 1", "new_table")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected IndexStmt with relation")
}

func TestUpdateIndexTableName_success(t *testing.T) {
	got, err := updateIndexTableName("CREATE INDEX idx ON old_table (id)", "new_table")
	require.NoError(t, err)
	assert.Contains(t, got, "new_table")
	assert.NotContains(t, got, "old_table")
}

func TestParseFKDef_parseError(t *testing.T) {
	_, err := parseFKDef("NOT VALID SQL")
	require.Error(t, err)
}

func TestParseFKDef_success(t *testing.T) {
	con, err := parseFKDef("FOREIGN KEY (user_id) REFERENCES users(id)")
	require.NoError(t, err)
	require.NotNil(t, con)
}

func TestParseDefaultExpr_parseError(t *testing.T) {
	_, _, err := parseDefaultExpr(")))INVALID(((")
	require.Error(t, err)
}

func TestParseDefaultExpr_success(t *testing.T) {
	_, target, err := parseDefaultExpr("42")
	require.NoError(t, err)
	require.NotNil(t, target)
	require.NotNil(t, target.Val)
}

func TestStripDefaultTopLevelCast_stripsWhenPeerHasNoCast(t *testing.T) {
	// Top-level TypeCast is removed regardless of peer's shape, as long as
	// the cast is at the root. equalDefault relies on this to collapse
	// `'hello'::text` ≡ `'hello'`.
	_, target, err := parseDefaultExpr("'hello'::text")
	require.NoError(t, err)
	result := stripDefaultTopLevelCast(target.Val, nil)
	require.NotNil(t, result)
	assert.Nil(t, result.GetTypeCast())
}

func TestIsTextLikeTypeName_nil(t *testing.T) {
	assert.False(t, isTextLikeTypeName(nil))
}

func TestNormalizeIndexDef_parseError(t *testing.T) {
	_, err := normalizeIndexDef("NOT VALID SQL")
	require.Error(t, err)
}

func TestNormalizeIndexDef_notIndexStmt(t *testing.T) {
	_, err := normalizeIndexDef("SELECT 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected parse result")
}

func TestDiffColumns_addIdentity_fromNotNull(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	stmts, _, err := diffColumns("public.items", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TABLE public.items ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY;",
	}, stmts)
}

func TestDiffColumns_addIdentity_fromNullable(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('d')})

	stmts, _, err := diffColumns("public.items", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TABLE public.items ALTER COLUMN id SET NOT NULL;",
		"ALTER TABLE public.items ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY;",
	}, stmts)
}

func TestDiffColumns_addIdentity_fromSerial(t *testing.T) {
	// catalog renders serial columns with TypeName="serial" and Default=nil,
	// even though the column has a hidden nextval() default. The diff must
	// emit DROP DEFAULT in that case so ADD GENERATED ... AS IDENTITY succeeds.
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "serial", NotNull: true})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	stmts, _, err := diffColumns("public.items", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TABLE public.items ALTER COLUMN id DROP DEFAULT;",
		"ALTER TABLE public.items ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY;",
	}, stmts)
}

func TestDiffColumns_addIdentity_fromColumnWithDefault(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Default: new("1")})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	stmts, _, err := diffColumns("public.items", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TABLE public.items ALTER COLUMN id DROP DEFAULT;",
		"ALTER TABLE public.items ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY;",
	}, stmts)
}

func TestDiffColumns_dropIdentity_keepNotNull(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true})

	stmts, _, err := diffColumns("public.items", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TABLE public.items ALTER COLUMN id DROP IDENTITY IF EXISTS;",
	}, stmts)
}

func TestDiffColumns_dropIdentity_toNullable(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer"})

	stmts, _, err := diffColumns("public.items", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TABLE public.items ALTER COLUMN id DROP IDENTITY IF EXISTS;",
		"ALTER TABLE public.items ALTER COLUMN id DROP NOT NULL;",
	}, stmts)
}

func TestDiffColumns_changeIdentityKind_alwaysToByDefault(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('d')})

	stmts, _, err := diffColumns("public.items", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TABLE public.items ALTER COLUMN id SET GENERATED BY DEFAULT;",
	}, stmts)
}

func TestDiffColumns_changeIdentityKind_byDefaultToAlways(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('d')})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	stmts, _, err := diffColumns("public.items", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER TABLE public.items ALTER COLUMN id SET GENERATED ALWAYS;",
	}, stmts)
}

func TestDiffColumns_identityUnchanged(t *testing.T) {
	current := orderedmap.New[string, *model.Column]()
	current.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	desired := orderedmap.New[string, *model.Column]()
	desired.Set("id", &model.Column{Name: "id", TypeName: "integer", NotNull: true, Identity: model.ColumnIdentity('a')})

	stmts, _, err := diffColumns("public.items", current, desired, allowAllDrops{})
	require.NoError(t, err)
	assert.Empty(t, stmts)
}
