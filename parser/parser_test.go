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

	tbl := result.Tables.Get("public.users")
	col := tbl.Columns.Get("name")
	require.NotNil(t, col.Comment)
	assert.Equal(t, "User name", *col.Comment)
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
