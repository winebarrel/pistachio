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
