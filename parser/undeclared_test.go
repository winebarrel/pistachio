package parser

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ALTER TABLE and ALTER SEQUENCE ... OWNED BY need their table or sequence
// declared before them, in the same file or an earlier one, as PostgreSQL
// would running the file. One without it used to be dropped in silence, and
// the plan then offered to drop the real constraint.
func TestParseSQL_AlterUndeclaredTargetErrors(t *testing.T) {
	for _, tc := range []struct{ name, sql, want string }{
		{"alter table", "ALTER TABLE public.t ADD CONSTRAINT c CHECK (true);", "ALTER TABLE public.t: table public.t is not declared before it"},
		{"alter table if exists", "ALTER TABLE IF EXISTS public.t ADD CONSTRAINT c CHECK (true);", "ALTER TABLE public.t: table public.t is not declared before it"},
		{"alter table only", "ALTER TABLE ONLY public.t ADD CONSTRAINT c CHECK (true);", "ALTER TABLE public.t: table public.t is not declared before it"},
		{"alter table declared later", "ALTER TABLE public.t ADD CONSTRAINT c CHECK (true);\nCREATE TABLE public.t (id integer);", "ALTER TABLE public.t: table public.t is not declared before it"},
		{"alter table on a view", "CREATE VIEW public.v AS SELECT 1 AS x;\nALTER TABLE public.v ALTER COLUMN x SET DEFAULT 2;", "ALTER TABLE public.v: table public.v is not declared before it"},
		{"alter table with an unsupported action", "ALTER TABLE public.t ADD COLUMN x text;", "ALTER TABLE public.t: table public.t is not declared before it"},
		{"quoted name", `ALTER TABLE "My Schema"."My Table" ADD CONSTRAINT c CHECK (true);`, `ALTER TABLE "My Schema"."My Table": table "My Schema"."My Table" is not declared before it`},
		{"alter sequence", "ALTER SEQUENCE public.s OWNED BY public.t.id;", "ALTER SEQUENCE public.s: sequence public.s is not declared before it"},
		{"alter sequence declared later", "ALTER SEQUENCE public.s OWNED BY public.t.id;\nCREATE SEQUENCE public.s;", "ALTER SEQUENCE public.s: sequence public.s is not declared before it"},
		{"alter sequence other option", "ALTER SEQUENCE public.s RESTART WITH 10;", "ALTER SEQUENCE public.s: sequence public.s is not declared before it"},
	} {
		_, err := parseSQLWithPublicSchema(tc.sql)
		require.Error(t, err, tc.name)
		assert.Equal(t, tc.want, err.Error(), tc.name)
	}
}

// The first such statement in the input is the one reported.
func TestParseSQL_AlterUndeclaredTargetErrorNamesTheFirst(t *testing.T) {
	_, err := parseSQLWithPublicSchema("ALTER SEQUENCE public.s OWNED BY NONE;\nALTER TABLE public.a ADD CONSTRAINT c CHECK (true);")
	require.Error(t, err)
	assert.Equal(t, "ALTER SEQUENCE public.s: sequence public.s is not declared before it", err.Error())
}

// The declaration counts from an earlier file as well as the same one, and
// a later file is too late.
func TestParseSQLFiles_AlterAfterDeclarationInEarlierFile(t *testing.T) {
	dir := t.TempDir()
	tables := filepath.Join(dir, "tables.sql")
	alters := filepath.Join(dir, "alters.sql")
	require.NoError(t, os.WriteFile(tables, []byte("CREATE TABLE public.items (id integer);\nCREATE SEQUENCE public.items_seq;\n"), 0o644))
	require.NoError(t, os.WriteFile(alters, []byte("ALTER TABLE public.items ADD CONSTRAINT items_chk CHECK (id > 0);\nALTER SEQUENCE public.items_seq OWNED BY public.items.id;\n"), 0o644))

	result, err := ParseSQLFilesWithSchema([]string{tables, alters}, "public")
	require.NoError(t, err)
	assert.NotNil(t, result.Tables.Get("public.items").Constraints.Get("items_chk"))
	assert.True(t, result.Sequences.Get("public.items_seq").Owned())

	_, err = ParseSQLFilesWithSchema([]string{alters, tables}, "public")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ALTER TABLE public.items: table public.items is not declared before it\n --> "+alters+":1:1")
}

// The error points at the statement, with its file, line and column.
func TestParseSQLFiles_AlterUndeclaredTargetLocation(t *testing.T) {
	paths := writeSQLFiles(t, map[string]string{
		"items.sql": "CREATE TABLE public.items (id integer);\n\nALTER TABLE public.item ADD CONSTRAINT items_chk CHECK (id > 0);\n",
	})

	_, err := ParseSQLFilesWithSchema(paths, "public")
	require.Error(t, err)
	assert.Equal(t, `ALTER TABLE public.item: table public.item is not declared before it
 --> `+paths[0]+`:3:1
  |
3 | ALTER TABLE public.item ADD CONSTRAINT items_chk CHECK (id > 0);
  | ^`, err.Error())
}

// A statement under -- pista:execute is run as written and not read, so it
// is not checked.
func TestParseSQL_AlterUndeclaredTargetUnderExecute(t *testing.T) {
	_, err := parseSQLWithPublicSchema("-- pista:execute\nALTER TABLE public.t ADD CONSTRAINT c CHECK (true);")
	require.NoError(t, err)
}
