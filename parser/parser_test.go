package parser_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
	"gopkg.in/yaml.v3"
)

type parseTestCase struct {
	Input    string `yaml:"input"`
	Expected string `yaml:"expected"`
}

func TestParseSQL(t *testing.T) {
	files, err := filepath.Glob("../testdata/parser/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(file)
			require.NoError(t, err)
			var tc parseTestCase
			require.NoError(t, yaml.Unmarshal(data, &tc))

			result, err := parser.ParseSQL(tc.Input)
			require.NoError(t, err)
			got := model.TablesToSQL(result.Tables)
			assert.Equal(t, strings.TrimSpace(tc.Expected), strings.TrimSpace(got))
		})
	}
}

func TestParseSQLFile(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.sql")
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`
	require.NoError(t, os.WriteFile(tmpFile, []byte(sql), 0o644))

	result, err := parser.ParseSQLFile(tmpFile)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Tables.Len())

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	assert.Equal(t, "users", tbl.Name)
	assert.Equal(t, "public", tbl.Schema)
}

func TestParseSQLFiles(t *testing.T) {
	tmpDir := t.TempDir()
	file1 := filepath.Join(tmpDir, "tables.sql")
	file2 := filepath.Join(tmpDir, "views.sql")

	require.NoError(t, os.WriteFile(file1, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))
	require.NoError(t, os.WriteFile(file2, []byte(`CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`), 0o644))

	result, err := parser.ParseSQLFiles([]string{file1, file2})
	require.NoError(t, err)
	assert.Equal(t, 2, result.Tables.Len())

	_, ok := result.Tables.GetOk("public.users")
	assert.True(t, ok)
	_, ok = result.Tables.GetOk("public.posts")
	assert.True(t, ok)
}

func TestParseSQLFiles_FileNotFound(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "exists.sql")
	require.NoError(t, os.WriteFile(tmpFile, []byte("CREATE TABLE t (id int);"), 0o644))

	_, err := parser.ParseSQLFiles([]string{tmpFile, "/nonexistent/file.sql"})
	require.Error(t, err)
}

func TestReadSQLFile(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.sql")
	sql := "SELECT 1;"
	require.NoError(t, os.WriteFile(tmpFile, []byte(sql), 0o644))

	got, err := parser.ReadSQLFile(tmpFile)
	require.NoError(t, err)
	assert.Equal(t, sql, got)
}

func TestParseSQLFile_NotFound(t *testing.T) {
	_, err := parser.ParseSQLFile("/nonexistent/file.sql")
	require.Error(t, err)
}

func TestParseSQL_InvalidSQL(t *testing.T) {
	_, err := parser.ParseSQL("NOT VALID SQL AT ALL ;;; {{{}}")
	require.Error(t, err)
}

func TestParseSQL_View(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id, name FROM users WHERE (name IS NOT NULL);
COMMENT ON VIEW public.active_users IS 'Active users';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Tables.Len())
	assert.Equal(t, 1, result.Views.Len())

	v, ok := result.Views.GetOk("public.active_users")
	require.True(t, ok)
	assert.Equal(t, "active_users", v.Name)
	assert.Equal(t, "public", v.Schema)
	assert.NotNil(t, v.Comment)
	assert.Equal(t, "Active users", *v.Comment)

	expected := "-- public.active_users\n" +
		"CREATE OR REPLACE VIEW public.active_users AS\n" +
		"SELECT id, name FROM users WHERE name IS NOT NULL;\n" +
		"COMMENT ON VIEW public.active_users IS 'Active users';"
	got := model.ViewsToSQL(result.Views)
	assert.Equal(t, expected, got)
}

func TestParseSQL_ViewCommentOnColumn(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
COMMENT ON COLUMN public.users.name IS 'User name';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	col, ok := tbl.Columns.GetOk("name")
	require.True(t, ok)
	require.NotNil(t, col.Comment)
	assert.Equal(t, "User name", *col.Comment)
}

func TestParseSQL_SchemaQualifiedView(t *testing.T) {
	sql := `CREATE TABLE myschema.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW myschema.active_users AS SELECT id, name FROM myschema.users;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Views.Len())

	v, ok := result.Views.GetOk("myschema.active_users")
	require.True(t, ok)
	assert.Equal(t, "myschema", v.Schema)
	assert.Equal(t, "active_users", v.Name)
}

func TestParseSQL_CommentOnTable(t *testing.T) {
	sql := `CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE myschema.users IS 'Users table';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("myschema.users")
	require.True(t, ok)
	require.NotNil(t, tbl.Comment)
	assert.Equal(t, "Users table", *tbl.Comment)
}

func TestParseSQL_CommentOnTable_Schemaless(t *testing.T) {
	sql := `CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE users IS 'Users table';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	require.NotNil(t, tbl.Comment)
	assert.Equal(t, "Users table", *tbl.Comment)
}

func TestParseSQL_CommentOnColumn_Schemaless(t *testing.T) {
	sql := `CREATE TABLE users (
    id integer NOT NULL,
    name text
);
COMMENT ON COLUMN users.name IS 'User name';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	col, ok := tbl.Columns.GetOk("name")
	require.True(t, ok)
	require.NotNil(t, col.Comment)
	assert.Equal(t, "User name", *col.Comment)
}

func TestParseSQL_CommentOnView_Schemaless(t *testing.T) {
	sql := `CREATE VIEW active_users AS SELECT 1;
COMMENT ON VIEW active_users IS 'Active users';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	v, ok := result.Views.GetOk("public.active_users")
	require.True(t, ok)
	require.NotNil(t, v.Comment)
	assert.Equal(t, "Active users", *v.Comment)
}

func TestParseSQL_ForeignKeyNotValid(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
ALTER TABLE public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id) NOT VALID;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.orders")
	require.True(t, ok)
	fk, ok := tbl.ForeignKeys.GetOk("fk_user")
	require.True(t, ok)
	assert.False(t, fk.Validated)
	assert.Equal(t, "public", *fk.RefSchema)
	assert.Equal(t, "users", *fk.RefTable)
	assert.Equal(t, []string{"user_id"}, fk.Columns)
}

func TestParseSQL_InlineForeignKey(t *testing.T) {
	sql := `CREATE TABLE public.groups (
    id integer NOT NULL,
    CONSTRAINT groups_pkey PRIMARY KEY (id)
);
CREATE TABLE public.members (
    id integer NOT NULL,
    group_id integer NOT NULL,
    CONSTRAINT members_pkey PRIMARY KEY (id),
    CONSTRAINT members_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.groups(id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.members")
	require.True(t, ok)
	fk, ok := tbl.ForeignKeys.GetOk("members_group_id_fkey")
	require.True(t, ok)
	assert.True(t, fk.Validated)
	assert.Equal(t, "public", fk.Schema)
	assert.Equal(t, "members", fk.Table)
	assert.Equal(t, "public", *fk.RefSchema)
	assert.Equal(t, "groups", *fk.RefTable)
	assert.Equal(t, []string{"group_id"}, fk.Columns)
	assert.Contains(t, fk.Definition, "FOREIGN KEY (group_id)")
}

func TestParseSQL_InlineForeignKeyWithSchema(t *testing.T) {
	sql := `CREATE TABLE myapp.categories (
    id integer NOT NULL,
    CONSTRAINT categories_pkey PRIMARY KEY (id)
);
CREATE TABLE myapp.items (
    id integer NOT NULL,
    category_id integer NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id),
    CONSTRAINT items_category_id_fkey FOREIGN KEY (category_id) REFERENCES myapp.categories(id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("myapp.items")
	require.True(t, ok)
	fk, ok := tbl.ForeignKeys.GetOk("items_category_id_fkey")
	require.True(t, ok)
	assert.Equal(t, "myapp", fk.Schema)
	assert.Equal(t, "items", fk.Table)
	assert.Equal(t, "myapp", *fk.RefSchema)
	assert.Equal(t, "categories", *fk.RefTable)
	assert.Equal(t, []string{"category_id"}, fk.Columns)
}

func TestParseSQL_InlineForeignKeyUnnamed(t *testing.T) {
	// Unnamed table-level FK constraints should be skipped
	sql := `CREATE TABLE public.groups (
    id integer NOT NULL,
    CONSTRAINT groups_pkey PRIMARY KEY (id)
);
CREATE TABLE public.members (
    id integer NOT NULL,
    group_id integer NOT NULL,
    CONSTRAINT members_pkey PRIMARY KEY (id),
    FOREIGN KEY (group_id) REFERENCES public.groups(id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.members")
	require.True(t, ok)
	assert.Equal(t, 0, tbl.ForeignKeys.Len())
}

func TestParseSQL_ColumnLevelNamedCheck(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL,
    status integer NOT NULL CONSTRAINT items_status_check CHECK (status = 0 OR status = 1),
    CONSTRAINT items_pkey PRIMARY KEY (id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.items")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("items_status_check")
	require.True(t, ok)
	assert.Contains(t, con.Definition, "CHECK")
}

func TestParseSQL_ColumnLevelNamedFK(t *testing.T) {
	sql := `CREATE TABLE public.groups (
    id integer NOT NULL,
    CONSTRAINT groups_pkey PRIMARY KEY (id)
);
CREATE TABLE public.members (
    id integer NOT NULL,
    group_id integer NOT NULL CONSTRAINT members_group_fkey REFERENCES public.groups(id),
    CONSTRAINT members_pkey PRIMARY KEY (id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.members")
	require.True(t, ok)
	fk, ok := tbl.ForeignKeys.GetOk("members_group_fkey")
	require.True(t, ok)
	assert.Equal(t, "public", fk.Schema)
	assert.Equal(t, "members", fk.Table)
	assert.Equal(t, "public", *fk.RefSchema)
	assert.Equal(t, "groups", *fk.RefTable)
	assert.Equal(t, []string{"group_id"}, fk.Columns)
	assert.Contains(t, fk.Definition, "FOREIGN KEY (group_id)")
}

func TestParseSQL_ColumnLevelNamedUnique(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL,
    code text NOT NULL CONSTRAINT items_code_key UNIQUE,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.items")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("items_code_key")
	require.True(t, ok)
	assert.Contains(t, con.Definition, "UNIQUE")
}

func TestParseSQL_ColumnLevelNamedPrimaryKey(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL CONSTRAINT items_pkey PRIMARY KEY,
    name text NOT NULL
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.items")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("items_pkey")
	require.True(t, ok)
	assert.Contains(t, con.Definition, "PRIMARY KEY")
}

func TestParseSQL_ColumnLevelMixedConstraints(t *testing.T) {
	// Multiple named column-level constraints on different columns
	sql := `CREATE TABLE public.groups (
    id integer NOT NULL,
    CONSTRAINT groups_pkey PRIMARY KEY (id)
);
CREATE TABLE public.items (
    id integer NOT NULL CONSTRAINT items_pkey PRIMARY KEY,
    code text NOT NULL CONSTRAINT items_code_key UNIQUE,
    group_id integer NOT NULL CONSTRAINT items_group_fkey REFERENCES public.groups(id),
    val integer NOT NULL CONSTRAINT items_val_check CHECK (val > 0)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.items")
	require.True(t, ok)

	// PK and UNIQUE
	_, ok = tbl.Constraints.GetOk("items_pkey")
	assert.True(t, ok)
	_, ok = tbl.Constraints.GetOk("items_code_key")
	assert.True(t, ok)
	_, ok = tbl.Constraints.GetOk("items_val_check")
	assert.True(t, ok)

	// FK
	fk, ok := tbl.ForeignKeys.GetOk("items_group_fkey")
	require.True(t, ok)
	assert.Equal(t, []string{"group_id"}, fk.Columns)
}

func TestParseSQL_ColumnLevelUnnamedConstraintsSkipped(t *testing.T) {
	// Unnamed column-level PRIMARY KEY, UNIQUE, CHECK, FK should all be skipped
	sql := `CREATE TABLE public.groups (
    id integer NOT NULL,
    CONSTRAINT groups_pkey PRIMARY KEY (id)
);
CREATE TABLE public.items (
    id integer PRIMARY KEY,
    name text UNIQUE,
    val integer CHECK (val > 0),
    group_id integer REFERENCES public.groups(id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.items")
	require.True(t, ok)
	assert.Equal(t, 0, tbl.Constraints.Len())
	assert.Equal(t, 0, tbl.ForeignKeys.Len())
}

func TestParseSQL_TablespaceOnCreate(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
) TABLESPACE my_ts;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	require.NotNil(t, tbl.TableSpace)
	assert.Equal(t, "my_ts", *tbl.TableSpace)
}

func TestParseSQL_AlterTableNonFK(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL,
    code text NOT NULL
);
ALTER TABLE public.items ADD CONSTRAINT items_code_unique UNIQUE (code);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl := result.Tables.Get("public.items")
	require.NotNil(t, tbl)
	_, ok := tbl.Constraints.GetOk("items_code_unique")
	assert.True(t, ok)
}

func TestParseSQL_AlterTableUnknownTable(t *testing.T) {
	// ALTER TABLE referencing a table not in parsed result is silently skipped
	sql := `ALTER TABLE public.nonexistent ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES public.other(id);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Tables.Len())
}

func TestParseSQL_CommentOnUnknownTable(t *testing.T) {
	// COMMENT on unknown table is silently skipped
	sql := `COMMENT ON TABLE public.nonexistent IS 'test';`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Tables.Len())
}

func TestParseSQL_PartitionListType(t *testing.T) {
	sql := `CREATE TABLE public.sales (
    id integer NOT NULL,
    region text NOT NULL,
    CONSTRAINT sales_pkey PRIMARY KEY (id, region)
)
PARTITION BY LIST (region);
CREATE TABLE public.sales_east PARTITION OF public.sales FOR VALUES IN ('east');`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Tables.Len())

	parent, ok := result.Tables.GetOk("public.sales")
	require.True(t, ok)
	assert.True(t, parent.Partitioned)
	require.NotNil(t, parent.PartitionDef)
	assert.Contains(t, *parent.PartitionDef, "LIST")

	child, ok := result.Tables.GetOk("public.sales_east")
	require.True(t, ok)
	require.NotNil(t, child.PartitionOf)
	require.NotNil(t, child.PartitionBound)
}

func TestParseSQL_InlineUniqueConstraint(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL,
    code text NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id),
    CONSTRAINT items_code_key UNIQUE (code),
    CONSTRAINT items_code_check CHECK (code <> '')
);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl := result.Tables.Get("public.items")
	require.NotNil(t, tbl)
	assert.Equal(t, 3, tbl.Constraints.Len())

	_, ok := tbl.Constraints.GetOk("items_pkey")
	assert.True(t, ok)
	_, ok = tbl.Constraints.GetOk("items_code_key")
	assert.True(t, ok)
	_, ok = tbl.Constraints.GetOk("items_code_check")
	assert.True(t, ok)
}

func TestParseSQL_MultipleViews(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.v1 AS SELECT id FROM users;
CREATE VIEW public.v2 AS SELECT name FROM users;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Views.Len())
}

func TestParseSQL_UnnamedConstraintSkipped(t *testing.T) {
	// An unnamed table constraint (no CONSTRAINT name) is skipped by parseTableConstraint
	sql := `CREATE TABLE public.items (
    id integer NOT NULL,
    name text,
    PRIMARY KEY (id)
);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl := result.Tables.Get("public.items")
	require.NotNil(t, tbl)
	assert.Equal(t, 0, tbl.Constraints.Len())
}

func TestParseSQL_CommentRemove(t *testing.T) {
	// When COMMENT ON ... IS '' (empty string), the comment is set to nil
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.users IS 'Users';
COMMENT ON TABLE public.users IS '';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl := result.Tables.Get("public.users")
	require.NotNil(t, tbl)
	assert.Nil(t, tbl.Comment)
}

func TestParseSQL_ViewCommentRemove(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.v1 AS SELECT id FROM users;
COMMENT ON VIEW public.v1 IS 'my view';
COMMENT ON VIEW public.v1 IS '';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	v := result.Views.Get("public.v1")
	require.NotNil(t, v)
	assert.Nil(t, v.Comment)
}

func TestParseSQL_ColumnCommentRemove(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
COMMENT ON COLUMN public.users.name IS 'Name';
COMMENT ON COLUMN public.users.name IS '';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl := result.Tables.Get("public.users")
	require.NotNil(t, tbl)
	col, ok := tbl.Columns.GetOk("name")
	require.True(t, ok)
	assert.Nil(t, col.Comment)
}

func TestParseSQL_IndexOnUnknownTableSkipped(t *testing.T) {
	sql := `CREATE INDEX idx_name ON public.nonexistent USING btree (name);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Tables.Len())
}

func TestParseSQL_ForeignKeyWithSchema(t *testing.T) {
	sql := `CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE myschema.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
ALTER TABLE myschema.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES myschema.users(id);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("myschema.orders")
	require.True(t, ok)
	fk, ok := tbl.ForeignKeys.GetOk("fk_user")
	require.True(t, ok)
	assert.Equal(t, "myschema", fk.Schema)
	assert.Equal(t, "myschema", *fk.RefSchema)
}

func TestParseSQLWithSchema_DefaultSchema(t *testing.T) {
	sql := `CREATE TABLE users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_name ON users (name);
CREATE VIEW active_users AS SELECT id, name FROM users;
COMMENT ON TABLE users IS 'User accounts';
COMMENT ON COLUMN users.name IS 'User name';`

	result, err := parser.ParseSQLWithSchema(sql, "myschema")
	require.NoError(t, err)

	// Table defaults to myschema
	tbl, ok := result.Tables.GetOk("myschema.users")
	require.True(t, ok)
	assert.Equal(t, "myschema", tbl.Schema)

	// Index defaults to myschema
	idx, ok := tbl.Indexes.GetOk("idx_users_name")
	require.True(t, ok)
	assert.Equal(t, "myschema", idx.Schema)

	// View defaults to myschema
	v, ok := result.Views.GetOk("myschema.active_users")
	require.True(t, ok)
	assert.Equal(t, "myschema", v.Schema)

	// Table comment
	require.NotNil(t, tbl.Comment)
	assert.Equal(t, "User accounts", *tbl.Comment)

	// Column comment
	col, ok := tbl.Columns.GetOk("name")
	require.True(t, ok)
	require.NotNil(t, col.Comment)
	assert.Equal(t, "User name", *col.Comment)
}

func TestParseSQLWithSchema_AlterTable(t *testing.T) {
	sql := `CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);`

	result, err := parser.ParseSQLWithSchema(sql, "myschema")
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("myschema.orders")
	require.True(t, ok)
	fk, ok := tbl.ForeignKeys.GetOk("fk_user")
	require.True(t, ok)
	assert.Equal(t, "myschema", fk.Schema)
	assert.Equal(t, "myschema", *fk.RefSchema)
}

func TestParseSQLWithSchema_InheritedTable(t *testing.T) {
	sql := `CREATE TABLE events (
    id integer NOT NULL,
    created_at date NOT NULL
) PARTITION BY RANGE (created_at);
CREATE TABLE events_2024 PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');`

	result, err := parser.ParseSQLWithSchema(sql, "myschema")
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("myschema.events_2024")
	require.True(t, ok)
	assert.Equal(t, "myschema", tbl.Schema)
	require.NotNil(t, tbl.PartitionOf)
	assert.Contains(t, *tbl.PartitionOf, "myschema")
}

func TestParseSQLWithSchema_ViewComment(t *testing.T) {
	sql := `CREATE VIEW active_users AS SELECT 1;
COMMENT ON VIEW active_users IS 'Active users';`

	result, err := parser.ParseSQLWithSchema(sql, "myschema")
	require.NoError(t, err)

	v, ok := result.Views.GetOk("myschema.active_users")
	require.True(t, ok)
	require.NotNil(t, v.Comment)
	assert.Equal(t, "Active users", *v.Comment)
}
