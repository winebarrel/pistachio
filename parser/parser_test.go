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

func TestReadSQLFile_Stdin(t *testing.T) {
	// Create a pipe to simulate stdin
	r, w, err := os.Pipe()
	require.NoError(t, err)

	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
		r.Close()
	}()

	sql := "SELECT 1;"
	go func() {
		w.WriteString(sql)
		w.Close()
	}()

	got, err := parser.ReadSQLFile("-")
	require.NoError(t, err)
	assert.Equal(t, sql, got)
}

func TestReadSQLFile_Stdin_ReadAll(t *testing.T) {
	// Verify that stdin reads empty content from a closed pipe without error
	r, w, err := os.Pipe()
	require.NoError(t, err)
	w.Close()

	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
		r.Close()
	}()

	got, err := parser.ReadSQLFile("-")
	require.NoError(t, err)
	assert.Equal(t, "", got)
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
	assert.NotContains(t, fk.Definition, "NOT VALID")
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
	// Unnamed table-level FK constraints should be auto-named
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
	tbl := result.Tables.Get("public.members")
	require.NotNil(t, tbl)
	_, ok := tbl.ForeignKeys.GetOk("members_group_id_fkey")
	assert.True(t, ok)
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
	assert.True(t, con.Validated)
}

func TestParseSQL_CheckConstraintNotValid(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
ALTER TABLE public.items ADD CONSTRAINT items_id_check CHECK (id > 0) NOT VALID;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.items")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("items_id_check")
	require.True(t, ok)
	assert.Contains(t, con.Definition, "CHECK")
	assert.NotContains(t, con.Definition, "NOT VALID")
	assert.False(t, con.Validated)
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

func TestParseSQL_ColumnLevelUnnamedConstraintsAutoNamed(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		table        string
		conName      string
		isForeignKey bool
	}{
		{
			name:    "PRIMARY KEY",
			sql:     `CREATE TABLE public.items (id integer PRIMARY KEY);`,
			table:   "public.items",
			conName: "items_pkey",
		},
		{
			name:    "UNIQUE",
			sql:     `CREATE TABLE public.items (id integer NOT NULL, code text UNIQUE, CONSTRAINT items_pkey PRIMARY KEY (id));`,
			table:   "public.items",
			conName: "items_code_key",
		},
		{
			name:    "CHECK",
			sql:     `CREATE TABLE public.items (id integer NOT NULL, val integer CHECK (val > 0), CONSTRAINT items_pkey PRIMARY KEY (id));`,
			table:   "public.items",
			conName: "items_val_check",
		},
		{
			name:         "FOREIGN KEY",
			sql:          "CREATE TABLE public.groups (id integer NOT NULL, CONSTRAINT groups_pkey PRIMARY KEY (id));\nCREATE TABLE public.items (id integer NOT NULL, group_id integer REFERENCES public.groups(id), CONSTRAINT items_pkey PRIMARY KEY (id));",
			table:        "public.items",
			conName:      "items_group_id_fkey",
			isForeignKey: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseSQL(tt.sql)
			require.NoError(t, err)
			tbl := result.Tables.Get(tt.table)
			require.NotNil(t, tbl)
			if tt.isForeignKey {
				_, ok := tbl.ForeignKeys.GetOk(tt.conName)
				assert.True(t, ok, "expected FK %s", tt.conName)
			} else {
				_, ok := tbl.Constraints.GetOk(tt.conName)
				assert.True(t, ok, "expected constraint %s", tt.conName)
			}
		})
	}
}

func TestParseSQL_TableLevelUnnamedExclusionAutoNamed(t *testing.T) {
	sql := `CREATE TABLE public.reservations (
    id integer NOT NULL,
    room integer NOT NULL,
    during tsrange NOT NULL,
    CONSTRAINT reservations_pkey PRIMARY KEY (id),
    EXCLUDE USING gist (room WITH =, during WITH &&)
);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	tbl := result.Tables.Get("public.reservations")
	require.NotNil(t, tbl)
	_, ok := tbl.Constraints.GetOk("reservations_room_excl")
	assert.True(t, ok)
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

func TestParseSQL_UnnamedTableConstraintAutoNamed(t *testing.T) {
	// An unnamed table constraint should be auto-named
	sql := `CREATE TABLE public.items (
    id integer NOT NULL,
    name text,
    PRIMARY KEY (id)
);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	tbl := result.Tables.Get("public.items")
	require.NotNil(t, tbl)
	_, ok := tbl.Constraints.GetOk("items_pkey")
	assert.True(t, ok)
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

func TestParseSQL_MaterializedView(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.user_stats AS SELECT count(*) AS cnt FROM public.users;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Tables.Len())
	assert.Equal(t, 1, result.Views.Len())

	v, ok := result.Views.GetOk("public.user_stats")
	require.True(t, ok)
	assert.Equal(t, "user_stats", v.Name)
	assert.Equal(t, "public", v.Schema)
	assert.True(t, v.Materialized)
	assert.Contains(t, v.Definition, "SELECT count(") // deparsed
}

func TestParseSQL_MaterializedView_Schemaless(t *testing.T) {
	sql := `CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW user_stats AS SELECT count(*) AS cnt FROM users;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Views.Len())

	v, ok := result.Views.GetOk("public.user_stats")
	require.True(t, ok)
	assert.Equal(t, "public", v.Schema) // defaults to public
	assert.True(t, v.Materialized)
}

func TestParseSQL_MaterializedView_Comment(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.user_stats AS SELECT count(*) AS cnt FROM public.users;
COMMENT ON MATERIALIZED VIEW public.user_stats IS 'User statistics';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	v, ok := result.Views.GetOk("public.user_stats")
	require.True(t, ok)
	assert.True(t, v.Materialized)
	require.NotNil(t, v.Comment)
	assert.Equal(t, "User statistics", *v.Comment)
}

func TestParseSQL_MaterializedViewWithIndex(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.user_stats AS SELECT count(*) AS cnt FROM public.users;
CREATE INDEX idx_user_stats_cnt ON public.user_stats (cnt);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	v, ok := result.Views.GetOk("public.user_stats")
	require.True(t, ok)
	assert.True(t, v.Materialized)
	assert.Equal(t, 1, v.Indexes.Len())

	idx, ok := v.Indexes.GetOk("idx_user_stats_cnt")
	require.True(t, ok)
	assert.Equal(t, "user_stats", idx.Table)
}

func TestParseSQL_MaterializedView_RenameDirective(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:renamed-from public.old_stats
CREATE MATERIALIZED VIEW public.user_stats AS SELECT count(*) AS cnt FROM public.users;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	v, ok := result.Views.GetOk("public.user_stats")
	require.True(t, ok)
	assert.True(t, v.Materialized)
	require.NotNil(t, v.RenameFrom)
	assert.Equal(t, "public.old_stats", *v.RenameFrom)
}

func TestParseSQL_MaterializedView_Duplicate(t *testing.T) {
	sql := `CREATE MATERIALIZED VIEW public.mv AS SELECT 1;
CREATE MATERIALIZED VIEW public.mv AS SELECT 2;`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate materialized view")
}

func TestParseSQL_MaterializedView_IndexDuplicate(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE MATERIALIZED VIEW public.mv AS SELECT count(*) AS cnt FROM public.users;
CREATE INDEX idx ON public.mv (cnt);
CREATE INDEX idx ON public.mv (cnt);`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate index")
}

func TestParseSQL_IndexOnSchemalessTable(t *testing.T) {
	// When using --omit-schema, indexes reference tables without schema prefix.
	// The parser should still attach the index to the correct table.
	sql := `CREATE TABLE users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_name ON users USING btree (name);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Tables.Len())

	t1, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	assert.Equal(t, 1, t1.Indexes.Len())

	idx, ok := t1.Indexes.GetOk("idx_users_name")
	require.True(t, ok)
	assert.Equal(t, "users", idx.Table)
}

func TestParseSQL_IndexOnSchemalessMatview(t *testing.T) {
	sql := `CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW user_stats AS SELECT count(*) AS cnt FROM users;
CREATE INDEX idx_user_stats_cnt ON user_stats (cnt);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	v, ok := result.Views.GetOk("public.user_stats")
	require.True(t, ok)
	assert.True(t, v.Materialized)
	assert.Equal(t, 1, v.Indexes.Len())

	idx, ok := v.Indexes.GetOk("idx_user_stats_cnt")
	require.True(t, ok)
	assert.Equal(t, "user_stats", idx.Table)
}

func TestParseSQL_ExecuteDirective(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'my_func')
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	// Table should be parsed normally
	assert.Equal(t, 1, result.Tables.Len())
	// Function should be in ExecuteStmts, not Views/Tables
	assert.Equal(t, 0, result.Views.Len())
	require.Len(t, result.ExecuteStmts, 1)
	assert.Contains(t, result.ExecuteStmts[0].SQL, "CREATE OR REPLACE FUNCTION")
	assert.Contains(t, result.ExecuteStmts[0].CheckSQL, "pg_proc")
}

func TestParseSQL_ExecuteDirective_NoCheck(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:execute
GRANT SELECT ON public.users TO readonly_role;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Tables.Len())
	require.Len(t, result.ExecuteStmts, 1)
	assert.Contains(t, result.ExecuteStmts[0].SQL, "GRANT select")
	assert.Equal(t, "", result.ExecuteStmts[0].CheckSQL)
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

func TestParseSQL_AlterTableExclusionConstraint(t *testing.T) {
	sql := `CREATE TABLE public.reservations (
    id integer NOT NULL,
    room integer NOT NULL,
    during tsrange NOT NULL,
    CONSTRAINT reservations_pkey PRIMARY KEY (id)
);
ALTER TABLE public.reservations ADD CONSTRAINT no_overlap EXCLUDE USING gist (room WITH =, during WITH &&);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.reservations")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("no_overlap")
	require.True(t, ok)
	assert.Equal(t, model.ConstraintType('x'), con.Type)
	assert.Contains(t, con.Definition, "EXCLUDE USING gist")
}

func TestParseSQL_DeferrableConstraint(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL,
    code text NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id),
    CONSTRAINT items_code_key UNIQUE (code) DEFERRABLE INITIALLY DEFERRED
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.items")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("items_code_key")
	require.True(t, ok)
	assert.True(t, con.Deferrable)
	assert.True(t, con.Deferred)
}

func TestParseSQL_StoredGeneratedColumn(t *testing.T) {
	// Test GENERATED ALWAYS AS (expr) STORED with identity column
	sql := `CREATE TABLE public.people (
    id integer GENERATED BY DEFAULT AS IDENTITY,
    first_name text NOT NULL,
    last_name text NOT NULL,
    full_name text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    CONSTRAINT people_pkey PRIMARY KEY (id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.people")
	require.True(t, ok)

	col, ok := tbl.Columns.GetOk("full_name")
	require.True(t, ok)
	assert.NotNil(t, col.Default)
	assert.Contains(t, *col.Default, "first_name")
}

func TestParseSQL_IndexWithTablespace(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_name ON public.users USING btree (name) TABLESPACE fast_ssd;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_users_name")
	require.True(t, ok)
	require.NotNil(t, idx.TableSpace)
	assert.Equal(t, "fast_ssd", *idx.TableSpace)
}

func TestParseSQL_AlterTableNonAddConstraint(t *testing.T) {
	// ALTER TABLE with non-ADD CONSTRAINT commands should be silently skipped
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
ALTER TABLE public.users DROP COLUMN name;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	// The parser does not apply DDL changes; the column should still be present
	_, ok = tbl.Columns.GetOk("name")
	assert.True(t, ok)
}

func TestParseSQL_TableLevelExclusionConstraint(t *testing.T) {
	// EXCLUSION constraint at table level with explicit name in CREATE TABLE
	sql := `CREATE TABLE public.reservations (
    id integer NOT NULL,
    room integer NOT NULL,
    during tsrange NOT NULL,
    CONSTRAINT reservations_pkey PRIMARY KEY (id),
    CONSTRAINT no_overlap EXCLUDE USING gist (room WITH =, during WITH &&)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.reservations")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("no_overlap")
	require.True(t, ok)
	assert.Equal(t, model.ConstraintType('x'), con.Type)
	assert.Contains(t, con.Definition, "EXCLUDE USING gist")
}

func TestParseSQL_DuplicateTable(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate table")
}

func TestParseSQL_DuplicateView(t *testing.T) {
	sql := `CREATE VIEW public.v1 AS SELECT 1;
CREATE VIEW public.v1 AS SELECT 2;`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate view")
}

func TestParseSQL_DuplicateIndex(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL, name text, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE INDEX idx_name ON public.users (name);
CREATE INDEX idx_name ON public.users (name);`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate index")
}

func TestParseSQL_DuplicateConstraint(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL,
    val integer NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id),
    CONSTRAINT items_pkey PRIMARY KEY (id)
);`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate constraint")
}

func TestParseSQL_DuplicateColumn(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column")
}

func TestParseSQL_DuplicateForeignKeyAlterTable(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE TABLE public.orders (id integer NOT NULL, user_id integer NOT NULL, CONSTRAINT orders_pkey PRIMARY KEY (id));
ALTER TABLE public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);
ALTER TABLE public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate foreign key")
}

func TestParseSQL_DuplicateConstraintAlterTable(t *testing.T) {
	sql := `CREATE TABLE public.items (id integer NOT NULL, code text NOT NULL);
ALTER TABLE public.items ADD CONSTRAINT items_code_unique UNIQUE (code);
ALTER TABLE public.items ADD CONSTRAINT items_code_unique UNIQUE (code);`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate constraint")
}

func TestParseSQL_DuplicateInlineForeignKey(t *testing.T) {
	sql := `CREATE TABLE public.groups (id integer NOT NULL, CONSTRAINT groups_pkey PRIMARY KEY (id));
CREATE TABLE public.items (
    id integer NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id),
    CONSTRAINT items_fk FOREIGN KEY (id) REFERENCES public.groups(id),
    CONSTRAINT items_fk FOREIGN KEY (id) REFERENCES public.groups(id)
);`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate foreign key")
}

func TestParseSQL_DuplicateColumnLevelConstraint(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL CONSTRAINT items_pkey PRIMARY KEY,
    code text NOT NULL CONSTRAINT items_pkey UNIQUE
);`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate constraint")
}

func TestParseSQL_DuplicateColumnLevelForeignKey(t *testing.T) {
	sql := `CREATE TABLE public.groups (id integer NOT NULL, CONSTRAINT groups_pkey PRIMARY KEY (id));
CREATE TABLE public.items (
    id integer NOT NULL CONSTRAINT items_fk REFERENCES public.groups(id),
    code integer NOT NULL CONSTRAINT items_fk REFERENCES public.groups(id)
);`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate foreign key")
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

func TestParseSQL_Enum(t *testing.T) {
	sql := `CREATE TYPE public.status AS ENUM ('active', 'inactive', 'pending');`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Enums.Len())

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Equal(t, "status", e.Name)
	assert.Equal(t, "public", e.Schema)
	assert.Equal(t, []string{"active", "inactive", "pending"}, e.Values)

	expected := "-- public.status\n" +
		"CREATE TYPE public.status AS ENUM (\n" +
		"    'active',\n" +
		"    'inactive',\n" +
		"    'pending'\n" +
		");"
	got := model.EnumsToSQL(result.Enums)
	assert.Equal(t, expected, got)
}

func TestParseSQL_EnumWithComment(t *testing.T) {
	sql := `CREATE TYPE public.status AS ENUM ('active', 'inactive');
COMMENT ON TYPE public.status IS 'User status';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	require.NotNil(t, e.Comment)
	assert.Equal(t, "User status", *e.Comment)
}

func TestParseSQL_EnumCommentRemove(t *testing.T) {
	sql := `CREATE TYPE public.status AS ENUM ('active', 'inactive');
COMMENT ON TYPE public.status IS 'User status';
COMMENT ON TYPE public.status IS '';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Nil(t, e.Comment)
}

func TestParseSQLWithSchema_Enum(t *testing.T) {
	sql := `CREATE TYPE status AS ENUM ('active', 'inactive');
COMMENT ON TYPE status IS 'User status';`

	result, err := parser.ParseSQLWithSchema(sql, "myschema")
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("myschema.status")
	require.True(t, ok)
	assert.Equal(t, "myschema", e.Schema)
	assert.Equal(t, "status", e.Name)
	require.NotNil(t, e.Comment)
	assert.Equal(t, "User status", *e.Comment)
}

func TestParseSQL_EnumSchemaQualified(t *testing.T) {
	sql := `CREATE TYPE myschema.status AS ENUM ('active', 'inactive');`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("myschema.status")
	require.True(t, ok)
	assert.Equal(t, "myschema", e.Schema)
	assert.Equal(t, "status", e.Name)
}

func TestParseSQL_DuplicateEnum(t *testing.T) {
	sql := `CREATE TYPE public.status AS ENUM ('active');
CREATE TYPE public.status AS ENUM ('inactive');`

	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate enum")
}

func TestParseSQL_CommentOnUnknownEnum(t *testing.T) {
	sql := `COMMENT ON TYPE public.nonexistent IS 'test';`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Enums.Len())
}

func TestParseSQL_RenameDirective_Enum(t *testing.T) {
	sql := `-- pist:renamed-from public.old_status
CREATE TYPE public.new_status AS ENUM ('active', 'inactive');`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.new_status")
	require.True(t, ok)
	require.NotNil(t, e.RenameFrom)
	assert.Equal(t, "public.old_status", *e.RenameFrom)
}

func TestParseSQL_RenameDirective_Table(t *testing.T) {
	sql := `-- pist:renamed-from public.old_users
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	require.NotNil(t, tbl.RenameFrom)
	assert.Equal(t, "public.old_users", *tbl.RenameFrom)
}

func TestParseSQL_RenameDirective_View(t *testing.T) {
	sql := `-- pist:renamed-from public.old_view
CREATE VIEW public.new_view AS SELECT 1;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	v, ok := result.Views.GetOk("public.new_view")
	require.True(t, ok)
	require.NotNil(t, v.RenameFrom)
	assert.Equal(t, "public.old_view", *v.RenameFrom)
}

func TestParseSQL_RenameDirective_Column(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    -- pist:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	col, ok := tbl.Columns.GetOk("display_name")
	require.True(t, ok)
	require.NotNil(t, col.RenameFrom)
	assert.Equal(t, "name", *col.RenameFrom)
}

func TestParseSQL_RenameDirective_Index(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:renamed-from idx_old
CREATE INDEX idx_new ON public.users (name);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_new")
	require.True(t, ok)
	require.NotNil(t, idx.RenameFrom)
	assert.Equal(t, "idx_old", *idx.RenameFrom)
}

func TestParseSQL_RenameDirective_Constraint(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    code text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    -- pist:renamed-from old_unique
    CONSTRAINT new_unique UNIQUE (code)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("new_unique")
	require.True(t, ok)
	require.NotNil(t, con.RenameFrom)
	assert.Equal(t, "old_unique", *con.RenameFrom)
}

func TestParseSQL_RenameDirective_ForeignKey(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
-- pist:renamed-from old_fk
ALTER TABLE public.orders ADD CONSTRAINT new_fk FOREIGN KEY (user_id) REFERENCES public.users(id);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.orders")
	require.True(t, ok)
	fk, ok := tbl.ForeignKeys.GetOk("new_fk")
	require.True(t, ok)
	require.NotNil(t, fk.RenameFrom)
	assert.Equal(t, "old_fk", *fk.RenameFrom)
}

func TestParseSQL_RenameDirective_AlterTableConstraint(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    code text NOT NULL
);
-- pist:renamed-from old_unique
ALTER TABLE public.users ADD CONSTRAINT new_unique UNIQUE (code);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("new_unique")
	require.True(t, ok)
	require.NotNil(t, con.RenameFrom)
	assert.Equal(t, "old_unique", *con.RenameFrom)
}

func TestParseSQLWithSchema_RenameDirective_Qualifies(t *testing.T) {
	sql := `-- pist:renamed-from old_status
CREATE TYPE new_status AS ENUM ('active', 'inactive');
-- pist:renamed-from old_users
CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:renamed-from old_view
CREATE VIEW new_view AS SELECT 1;`

	result, err := parser.ParseSQLWithSchema(sql, "myschema")
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("myschema.new_status")
	require.True(t, ok)
	require.NotNil(t, e.RenameFrom)
	assert.Equal(t, "myschema.old_status", *e.RenameFrom)

	tbl, ok := result.Tables.GetOk("myschema.users")
	require.True(t, ok)
	require.NotNil(t, tbl.RenameFrom)
	assert.Equal(t, "myschema.old_users", *tbl.RenameFrom)

	v, ok := result.Views.GetOk("myschema.new_view")
	require.True(t, ok)
	require.NotNil(t, v.RenameFrom)
	assert.Equal(t, "myschema.old_view", *v.RenameFrom)
}

func TestParseSQL_Domain(t *testing.T) {
	sql := `CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Domains.Len())

	d, ok := result.Domains.GetOk("public.pos_int")
	require.True(t, ok)
	assert.Equal(t, "pos_int", d.Name)
	assert.Equal(t, "public", d.Schema)
	assert.Equal(t, "integer", d.BaseType)
	assert.Len(t, d.Constraints, 1)
	assert.Equal(t, "pos_check", d.Constraints[0].Name)
}

func TestParseSQL_DomainWithDefault(t *testing.T) {
	sql := `CREATE DOMAIN public.pos_int AS integer NOT NULL DEFAULT 1;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.pos_int")
	require.NotNil(t, d)
	assert.True(t, d.NotNull)
	require.NotNil(t, d.Default)
	assert.Equal(t, "1", *d.Default)
}

func TestParseSQL_DomainWithComment(t *testing.T) {
	sql := `CREATE DOMAIN public.pos_int AS integer;
COMMENT ON DOMAIN public.pos_int IS 'Positive integer';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.pos_int")
	require.NotNil(t, d)
	require.NotNil(t, d.Comment)
	assert.Equal(t, "Positive integer", *d.Comment)
}

func TestParseSQLWithSchema_Domain(t *testing.T) {
	sql := `CREATE DOMAIN pos_int AS integer;`

	result, err := parser.ParseSQLWithSchema(sql, "myschema")
	require.NoError(t, err)

	d, ok := result.Domains.GetOk("myschema.pos_int")
	require.True(t, ok)
	assert.Equal(t, "myschema", d.Schema)
}

func TestParseSQL_DomainWithCollation(t *testing.T) {
	sql := `CREATE DOMAIN public.name AS text COLLATE "en_US";`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.name")
	require.NotNil(t, d)
	require.NotNil(t, d.Collation)
	assert.Equal(t, "en_US", *d.Collation)
}

func TestParseSQL_DomainDefaultCollationExcluded(t *testing.T) {
	sql := `CREATE DOMAIN public.name AS text COLLATE "default";`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.name")
	require.NotNil(t, d)
	assert.Nil(t, d.Collation)
}

func TestParseSQL_DomainCommentRemove(t *testing.T) {
	sql := `CREATE DOMAIN public.pos_int AS integer;
COMMENT ON DOMAIN public.pos_int IS 'Positive integer';
COMMENT ON DOMAIN public.pos_int IS '';`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.pos_int")
	require.NotNil(t, d)
	assert.Nil(t, d.Comment)
}

func TestParseSQL_DomainMultipleConstraints(t *testing.T) {
	sql := `CREATE DOMAIN public.bounded_int AS integer
    CONSTRAINT min_check CHECK (VALUE >= 0)
    CONSTRAINT max_check CHECK (VALUE <= 100);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.bounded_int")
	require.NotNil(t, d)
	assert.Len(t, d.Constraints, 2)
}

func TestParseSQL_DomainRenameDirective(t *testing.T) {
	sql := `-- pist:renamed-from public.old_domain
CREATE DOMAIN public.new_domain AS integer;`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.new_domain")
	require.NotNil(t, d)
	require.NotNil(t, d.RenameFrom)
	assert.Equal(t, "public.old_domain", *d.RenameFrom)
}

func TestParseSQL_DomainUnnamedConstraintAutoNamed(t *testing.T) {
	sql := `CREATE DOMAIN public.pos_int AS integer CHECK (VALUE > 0);`
	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	d := result.Domains.Get("public.pos_int")
	require.NotNil(t, d)
	require.Len(t, d.Constraints, 1)
	assert.Equal(t, "pos_int_check", d.Constraints[0].Name)
}

func TestParseSQL_DuplicateDomain(t *testing.T) {
	sql := `CREATE DOMAIN public.pos_int AS integer;
CREATE DOMAIN public.pos_int AS bigint;`
	_, err := parser.ParseSQL(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate domain")
}

func TestParseSQL_NoRenameDirective(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl := result.Tables.Get("public.users")
	assert.Nil(t, tbl.RenameFrom)
	col, _ := tbl.Columns.GetOk("name")
	assert.Nil(t, col.RenameFrom)
}

func TestParseSQL_ConcurrentlyDirective_Index(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:concurrently
CREATE INDEX idx_name ON public.users (name);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_name")
	require.True(t, ok)
	assert.True(t, idx.Concurrently)
}

func TestParseSQL_ConcurrentlyDirective_NotSet(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_name ON public.users (name);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_name")
	require.True(t, ok)
	assert.False(t, idx.Concurrently)
}

func TestParseSQL_ConcurrentlyDirective_WithRenamed(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:renamed-from idx_old
-- pist:concurrently
CREATE INDEX idx_name ON public.users (name);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_name")
	require.True(t, ok)
	assert.True(t, idx.Concurrently)
	require.NotNil(t, idx.RenameFrom)
	assert.Equal(t, "idx_old", *idx.RenameFrom)
}

func TestParseSQL_ConcurrentlyDirective_IgnoredOnNonIndex(t *testing.T) {
	// -- pist:concurrently before a CREATE TABLE should be silently ignored
	sql := `-- pist:concurrently
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	result, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	assert.NotNil(t, tbl)
}
