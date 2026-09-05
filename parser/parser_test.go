package parser_test

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	pg_query "github.com/pganalyze/pg_query_go/v6"
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

			result, err := parseSQLWithPublicSchema(tc.Input)
			require.NoError(t, err)
			got := model.TablesToSQL(result.Tables)
			assert.Equal(t, strings.TrimSpace(tc.Expected), strings.TrimSpace(got))
		})
	}
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
	assert.Empty(t, got)
}

func TestParseSQL_InvalidSQL(t *testing.T) {
	_, err := parseSQLWithPublicSchema("NOT VALID SQL AT ALL ;;; {{{}}")
	require.Error(t, err)
}

func TestParseSQL_WarnsUnsupportedStmt(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	_, err := parser.ParseSQLWithSchema("CREATE EXTENSION pgcrypto;", "public")
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "ignored unsupported statement")
	assert.Contains(t, out, "CREATE EXTENSION pgcrypto")
}

// The warning carries only the statement, not the comments and blank lines
// around it, and a supported statement before it is still parsed.
func TestParseSQL_WarnsUnsupportedStmt_StripsComments(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	sql := "CREATE TABLE t (id integer);\n-- a leading comment\nSET foo = 1;"
	result, err := parser.ParseSQLWithSchema(sql, "public")
	require.NoError(t, err)

	_, ok := result.Tables.GetOk("public.t")
	assert.True(t, ok, "the supported table must still be parsed")

	out := buf.String()
	assert.Contains(t, out, "ignored unsupported statement: SET foo TO 1")
	assert.NotContains(t, out, "leading comment")
}

// A trailing comment after the last statement, which lacks a terminating
// semicolon, is not folded into the warning.
func TestParseSQL_WarnsUnsupportedStmt_LastNoSemicolon(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	sql := "CREATE TABLE t (id integer);\nGRANT SELECT ON t TO PUBLIC\n-- trailing comment"
	_, err := parser.ParseSQLWithSchema(sql, "public")
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "ignored unsupported statement: GRANT")
	assert.NotContains(t, out, "trailing comment")
}

// A statement longer than the limit is truncated on a rune boundary, so the
// warning stays short and valid UTF-8 even for multibyte input.
func TestParseSQL_WarnsUnsupportedStmt_Truncated(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	// U+3042 is a 3-byte rune; the escape keeps this source file ASCII.
	body := strings.Repeat("\u3042", 250)
	_, err := parser.ParseSQLWithSchema("SELECT '"+body+"';", "public")
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "...")
	assert.NotContains(t, out, body, "the full body must be truncated")
	assert.True(t, utf8.ValidString(out), "truncation must not split a rune")
}

// Deparse keeps the newlines inside a DO block, so the warning must collapse
// them; the message stays on one line. A DO block is used because deparse
// renders every other unsupported statement on one line already, and a
// CREATE FUNCTION is no longer unsupported.
func TestParseSQL_WarnsUnsupportedStmt_CollapsesMultiline(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	sql := "DO $$\nBEGIN\n  PERFORM 1;\nEND\n$$;"
	_, err := parser.ParseSQLWithSchema(sql, "public")
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "ignored unsupported statement: DO $$ BEGIN PERFORM 1; END $$")
	assert.Equal(t, 1, strings.Count(out, "\n"), "the warning must be a single line")
}

// An ALTER TABLE action the parser does not read is dropped from the desired
// schema. The statement type itself is supported, so it never reaches the
// unsupported-statement warning; without a warning of its own the column would
// silently read as absent and the plan would propose dropping it.
func TestParseSQL_WarnsIgnoredAlterTableAction(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	result, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.t (id integer);
		ALTER TABLE public.t ADD COLUMN x text;
	`)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.t")
	require.True(t, ok)
	_, hasCol := tbl.Columns.GetOk("x")
	assert.False(t, hasCol, "the action is still dropped; only the warning is new")

	out := buf.String()
	assert.Contains(t, out, "ignored unsupported statement")
	assert.Contains(t, out, "ADD COLUMN x text")
}

// A statement may mix an action the parser reads with one it does not. The
// supported action still lands in the model, and the warning names only the
// dropped one.
func TestParseSQL_WarnsIgnoredAlterTableAction_KeepsSupportedAction(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	result, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.t (id integer);
		ALTER TABLE public.t
		    ADD CONSTRAINT t_id_check CHECK (id > 0),
		    ADD COLUMN x text;
	`)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.t")
	require.True(t, ok)
	_, hasCon := tbl.Constraints.GetOk("t_id_check")
	assert.True(t, hasCon, "the supported action must still be parsed")

	out := buf.String()
	assert.Contains(t, out, "ADD COLUMN x text")
	assert.NotContains(t, out, "t_id_check", "the parsed action must not be reported as ignored")
}

// The storage and compression actions are read into the model, so neither
// reaches the warning that covers the actions the parser drops.
func TestParseSQL_ColumnStorageActionNotWarned(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	result, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.t (body text);
		ALTER TABLE public.t ALTER COLUMN body SET STORAGE MAIN;
		ALTER TABLE public.t ALTER COLUMN body SET COMPRESSION pglz;
	`)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.t")
	require.True(t, ok)
	col, ok := tbl.Columns.GetOk("body")
	require.True(t, ok)
	assert.Equal(t, "main", col.StorageType)
	assert.Equal(t, "pglz", col.Compression)

	assert.Empty(t, buf.String())
}

// Several dropped actions in one statement are reported together, on one line.
func TestParseSQL_WarnsIgnoredAlterTableAction_MultipleActions(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	_, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.t (id integer);
		ALTER TABLE public.t ADD COLUMN x text, ALTER COLUMN id TYPE bigint;
	`)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "ADD COLUMN x text")
	assert.Contains(t, out, "ALTER COLUMN id TYPE bigint")
	assert.Equal(t, 1, strings.Count(out, "\n"), "the warning must be a single line")
}

// The actions the parser does read must stay silent.
func TestParseSQL_NoWarnForSupportedAlterTableActions(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	_, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.t (id integer);
		ALTER TABLE public.t ADD CONSTRAINT t_id_check CHECK (id > 0);
		ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
		ALTER TABLE public.t FORCE ROW LEVEL SECURITY;
		CREATE TRIGGER trg BEFORE INSERT ON public.t
		    FOR EACH ROW EXECUTE FUNCTION public.f();
		ALTER TABLE public.t DISABLE TRIGGER trg;
	`)
	require.NoError(t, err)
	assert.Empty(t, buf.String())
}

// IF EXISTS is part of the statement, so it survives into the warning.
func TestParseSQL_WarnsIgnoredAlterTableAction_IfExists(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	_, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.t (id integer);
		ALTER TABLE IF EXISTS public.t ADD COLUMN x text;
	`)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "ALTER TABLE IF EXISTS public.t ADD COLUMN x text")
}

// A statement marked -- pista:execute is run as written instead of being
// diffed, so it must not warn.
func TestParseSQL_ExecuteDirectiveSilencesAlterTableWarning(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	result, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.t (id integer);
		-- pista:execute
		ALTER TABLE public.t ADD COLUMN x text;
	`)
	require.NoError(t, err)
	assert.Len(t, result.ExecuteStmts, 1)
	assert.Empty(t, buf.String())
}

// An ALTER TABLE naming a relation the file does not declare is skipped whole,
// the same as a CREATE INDEX on such a relation. That is deliberate, so it
// stays silent.
func TestParseSQL_NoWarnForAlterTableOnUndeclaredRelation(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	_, err := parseSQLWithPublicSchema(`ALTER TABLE public.nosuch ADD COLUMN x text;`)
	require.NoError(t, err)
	assert.Empty(t, buf.String())
}

// A table marked -- pista:ignore is out of the diff, so an action dropped
// from it cannot mislead the plan and the warning would be noise.
func TestParseSQL_NoWarnForAlterTableOnIgnoredTable(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	_, err := parseSQLWithPublicSchema(`
		-- pista:ignore
		CREATE TABLE public.t (id integer);
		ALTER TABLE public.t ADD COLUMN x text;
	`)
	require.NoError(t, err)
	assert.Empty(t, buf.String())
}

// COMMENT ON is dispatched by target. A target the parser does not model is
// dropped, so it warns like any other unsupported statement.
func TestParseSQL_WarnsUnsupportedCommentTarget(t *testing.T) {
	for _, stmt := range []string{
		`COMMENT ON INDEX public.t_idx IS 'i';`,
		`COMMENT ON CONSTRAINT t_id_check ON public.t IS 'c';`,
		`COMMENT ON SCHEMA public IS 's';`,
		`COMMENT ON TRIGGER trg ON public.t IS 'g';`,
	} {
		t.Run(stmt, func(t *testing.T) {
			var buf bytes.Buffer
			restore := parser.SetWarnWriter(&buf)
			defer restore()

			_, err := parseSQLWithPublicSchema("CREATE TABLE public.t (id integer);\n" + stmt)
			require.NoError(t, err)
			assert.Contains(t, buf.String(), "ignored unsupported statement")
		})
	}
}

// The comment targets the parser does model must stay silent.
func TestParseSQL_NoWarnForSupportedCommentTargets(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	_, err := parseSQLWithPublicSchema(`
		CREATE TYPE public.e AS ENUM ('a');
		CREATE DOMAIN public.d AS text;
		CREATE SEQUENCE public.s;
		CREATE TABLE public.t (id integer);
		CREATE VIEW public.v AS SELECT id FROM public.t;
		CREATE MATERIALIZED VIEW public.mv AS SELECT id FROM public.t;
		CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
		CREATE PROCEDURE public.p() LANGUAGE sql AS $$ SELECT 1 $$;
		COMMENT ON TABLE public.t IS 't';
		COMMENT ON COLUMN public.t.id IS 'c';
		COMMENT ON VIEW public.v IS 'v';
		COMMENT ON MATERIALIZED VIEW public.mv IS 'mv';
		COMMENT ON SEQUENCE public.s IS 's';
		COMMENT ON TYPE public.e IS 'e';
		COMMENT ON DOMAIN public.d IS 'd';
		COMMENT ON FUNCTION public.f() IS 'f';
		COMMENT ON PROCEDURE public.p() IS 'p';
	`)
	require.NoError(t, err)
	assert.Empty(t, buf.String())
}

// COMMENT ON COLUMN names a composite type attribute as well as a table
// column, and the type is found by the same schema-qualified name.
func TestParseSQL_CommentOnCompositeAttribute(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE TYPE public.addr AS (street text, city text);
		COMMENT ON COLUMN public.addr.city IS 'city name';
	`)
	require.NoError(t, err)

	ct, ok := result.CompositeTypes.GetOk("public.addr")
	require.True(t, ok)
	require.Len(t, ct.Attributes, 2)
	assert.Nil(t, ct.Attributes[0].Comment)
	require.NotNil(t, ct.Attributes[1].Comment)
	assert.Equal(t, "city name", *ct.Attributes[1].Comment)
}

// An explicit NULL comment clears one the same file set earlier.
func TestParseSQL_CommentIsNullClearsComment(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE SEQUENCE public.s;
		COMMENT ON SEQUENCE public.s IS 's';
		COMMENT ON SEQUENCE public.s IS NULL;
	`)
	require.NoError(t, err)

	seq, ok := result.Sequences.GetOk("public.s")
	require.True(t, ok)
	assert.Nil(t, seq.Comment)
}

func TestParseSQL_NoWarnForSupportedStmt(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	_, err := parser.ParseSQLWithSchema("CREATE TABLE t (id integer);", "public")
	require.NoError(t, err)
	assert.Empty(t, buf.String())
}

// The statements that open or close a whole-file transaction warn like any
// other unsupported statement, with a hint at the flags that wrap the apply.
// The table between them is still parsed.
func TestParseSQL_WarnsTransactionStmt(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	sql := "BEGIN;\nCREATE TABLE t (id integer);\nCOMMIT;\nSTART TRANSACTION;"
	result, err := parser.ParseSQLWithSchema(sql, "public")
	require.NoError(t, err)

	_, ok := result.Tables.GetOk("public.t")
	assert.True(t, ok)

	const hint = " (use --with-tx or --try-tx to run the apply in a transaction)"
	out := buf.String()
	assert.Contains(t, out, "ignored unsupported statement: BEGIN"+hint)
	assert.Contains(t, out, "ignored unsupported statement: COMMIT"+hint)
	assert.Contains(t, out, "ignored unsupported statement: START TRANSACTION"+hint)
}

// A non-transaction statement gets no transaction hint.
func TestParseSQL_WarnNoTxHintForOtherStmt(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	_, err := parser.ParseSQLWithSchema("SET foo = 1;", "public")
	require.NoError(t, err)
	assert.NotContains(t, buf.String(), "--with-tx")
}

// ROLLBACK and SAVEPOINT are transaction statements too, but wrapping the
// apply does not answer them, so they warn without the hint.
func TestParseSQL_WarnNoTxHintForRollback(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	_, err := parser.ParseSQLWithSchema("ROLLBACK;\nSAVEPOINT sp;", "public")
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "ignored unsupported statement: ROLLBACK")
	assert.Contains(t, out, "ignored unsupported statement: SAVEPOINT")
	assert.NotContains(t, out, "--with-tx")
}

// A statement marked -- pista:execute is skipped before the switch, so an
// unsupported statement carrying it does not warn.
func TestParseSQL_NoWarnForExecuteStmt(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	sql := "CREATE TABLE t (id integer);\n-- pista:execute\nGRANT SELECT ON t TO PUBLIC;"
	_, err := parser.ParseSQLWithSchema(sql, "public")
	require.NoError(t, err)
	assert.Empty(t, buf.String())
}

// When Deparse rejects a statement, the snippet falls back to the raw slice.
// A zero StmtLen runs the slice to the end of the input; the caller collapses
// the whitespace, so the helper returns the slice verbatim.
func TestIgnoredStmtSnippet_DeparseFallback(t *testing.T) {
	sql := "  GRANT  SELECT\n  ON t  "
	rs := &pg_query.RawStmt{StmtLocation: 0, StmtLen: 0}
	assert.Equal(t, sql, parser.IgnoredStmtSnippet(sql, rs))
}

func TestParseSQLFilesWithSchema(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.sql")
	b := filepath.Join(dir, "b.sql")
	// One unqualified, one explicitly qualified: verifies both the file-join
	// behavior and that defaultSchema is applied to unqualified names.
	require.NoError(t, os.WriteFile(a, []byte("CREATE TABLE t1 (id integer NOT NULL);"), 0o644))
	require.NoError(t, os.WriteFile(b, []byte("CREATE TABLE public.t2 (id integer NOT NULL);"), 0o644))

	result, err := parser.ParseSQLFilesWithSchema([]string{a, b}, "public")
	require.NoError(t, err)
	_, ok := result.Tables.GetOk("public.t1")
	assert.True(t, ok, "unqualified t1 should be schema-qualified to public.t1")
	_, ok = result.Tables.GetOk("public.t2")
	assert.True(t, ok, "qualified public.t2 should be preserved")
}

func TestParseSQLFilesWithSchema_MissingFile(t *testing.T) {
	_, err := parser.ParseSQLFilesWithSchema([]string{filepath.Join(t.TempDir(), "missing.sql")}, "public")
	require.ErrorIs(t, err, fs.ErrNotExist)
	assert.Contains(t, err.Error(), "failed to read SQL file")
}

func TestParseSQL_View(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id, name FROM users WHERE (name IS NOT NULL);
COMMENT ON VIEW public.active_users IS 'Active users';`

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.items")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("items_pkey")
	require.True(t, ok)
	assert.Contains(t, con.Definition, "PRIMARY KEY")
}

func TestParseSQL_UniqueConstraintOnValueColumn(t *testing.T) {
	// A single-column UNIQUE/PRIMARY KEY on a column named "value" trips a
	// libpg_query deparse bug that drops the column list. The parse tree keeps
	// the key, so the constraint Definition must still carry "(value)".
	sql := `CREATE TABLE public.api_keys (
    id bigint NOT NULL,
    value text NOT NULL,
    CONSTRAINT api_keys_pkey PRIMARY KEY (id),
    CONSTRAINT api_keys_value_key UNIQUE (value)
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.api_keys")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("api_keys_value_key")
	require.True(t, ok)
	assert.Equal(t, "UNIQUE (value)", con.Definition)
}

func TestParseSQL_PrimaryKeyOnValueColumn(t *testing.T) {
	sql := `CREATE TABLE public.settings (
    value text NOT NULL,
    CONSTRAINT settings_pkey PRIMARY KEY (value)
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.settings")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("settings_pkey")
	require.True(t, ok)
	assert.Equal(t, "PRIMARY KEY (value)", con.Definition)
}

func TestParseSQL_ColumnLevelUniqueOnValueColumn(t *testing.T) {
	// Column-level UNIQUE fills con.Keys from the column name (parser.go:539),
	// so it hits the same deparse workaround as a table-level constraint.
	sql := `CREATE TABLE public.api_keys (
    id bigint PRIMARY KEY,
    value text UNIQUE
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.api_keys")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("api_keys_value_key")
	require.True(t, ok)
	assert.Equal(t, "UNIQUE (value)", con.Definition)
}

func TestParseSQL_ColumnLevelPrimaryKeyOnValueColumn(t *testing.T) {
	sql := `CREATE TABLE public.settings (
    value text PRIMARY KEY
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.settings")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("settings_pkey")
	require.True(t, ok)
	assert.Equal(t, "PRIMARY KEY (value)", con.Definition)
}

func TestParseSQL_UniqueConstraintManyKeysWithValue(t *testing.T) {
	// A composite key with ten or more columns exercises the placeholder
	// substitution: "..._1_e" must not corrupt "..._10_e". "value" is included
	// so the deparse workaround is engaged.
	sql := `CREATE TABLE public.wide (
    c0 int, c1 int, c2 int, c3 int, c4 int, c5 int,
    c6 int, c7 int, c8 int, c9 int, value int,
    CONSTRAINT wide_key UNIQUE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, value)
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.wide")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("wide_key")
	require.True(t, ok)
	assert.Equal(t, "UNIQUE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, value)", con.Definition)
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

	result, err := parseSQLWithPublicSchema(sql)
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
			result, err := parseSQLWithPublicSchema(tt.sql)
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
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	tbl := result.Tables.Get("public.reservations")
	require.NotNil(t, tbl)
	_, ok := tbl.Constraints.GetOk("reservations_room_during_excl")
	assert.True(t, ok)
}

// PostgreSQL's ChooseConstraintName joins every key column, and takes a CHECK
// name from the one column its expression references, so these are the names
// the server picks for the same SQL. Getting them wrong means the plan adds a
// constraint under one name and offers to drop the server's, on every run
// against a database that already holds the table.
func TestParseSQL_UnnamedConstraintNamesFollowEveryColumn(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		conName      string
		isForeignKey bool
	}{
		{
			name:    "multi-column UNIQUE",
			sql:     `CREATE TABLE public.t (a integer, b integer, UNIQUE (a, b));`,
			conName: "t_a_b_key",
		},
		{
			name:    "multi-column PRIMARY KEY carries no column",
			sql:     `CREATE TABLE public.t (a integer, b integer, PRIMARY KEY (a, b));`,
			conName: "t_pkey",
		},
		{
			name:    "CHECK on one column",
			sql:     `CREATE TABLE public.t (a integer, b integer, CHECK (a > 0));`,
			conName: "t_a_check",
		},
		{
			name:    "CHECK on one column twice",
			sql:     `CREATE TABLE public.t (a integer, b integer, CHECK (a > 0 AND a < 10));`,
			conName: "t_a_check",
		},
		{
			name:    "CHECK on two columns",
			sql:     `CREATE TABLE public.t (a integer, b integer, CHECK (a > b));`,
			conName: "t_check",
		},
		{
			// The expression decides the name even for a column constraint.
			name:    "column CHECK referencing another column",
			sql:     `CREATE TABLE public.t (a integer CHECK (a > b), b integer);`,
			conName: "t_check",
		},
		{
			name: "multi-column FOREIGN KEY",
			sql: `CREATE TABLE public.p (a integer, b integer, CONSTRAINT p_pkey PRIMARY KEY (a, b));
CREATE TABLE public.t (a integer, b integer, FOREIGN KEY (a, b) REFERENCES public.p (a, b));`,
			conName:      "t_a_b_fkey",
			isForeignKey: true,
		},
		{
			name:    "multi-column EXCLUDE",
			sql:     `CREATE TABLE public.t (a integer, b integer, EXCLUDE (a WITH =, b WITH =));`,
			conName: "t_a_b_excl",
		},
		{
			name:    "EXCLUDE on an expression",
			sql:     `CREATE TABLE public.t (a integer, b integer, EXCLUDE ((a + b) WITH =));`,
			conName: "t_expr_excl",
		},
		{
			name:    "CHECK reaching its column through a subscript",
			sql:     `CREATE TABLE public.t (a integer[], CHECK (a[1] > 0));`,
			conName: "t_a_check",
		},
		{
			name:    "CHECK reaching its column through GREATEST",
			sql:     `CREATE TABLE public.t (a integer, CHECK (greatest(a, 1) > 0));`,
			conName: "t_a_check",
		},
		{
			name:    "CHECK reaching its column through a row constructor",
			sql:     `CREATE TABLE public.t (a integer, CHECK (ROW(a) IS NOT NULL));`,
			conName: "t_a_check",
		},
		{
			name:    "CHECK reaching its column through IS TRUE",
			sql:     `CREATE TABLE public.t (a boolean, CHECK (a IS TRUE));`,
			conName: "t_a_check",
		},
		{
			name:    "CHECK reaching its column through COLLATE",
			sql:     `CREATE TABLE public.t (a text, CHECK ((a COLLATE "C") > 'x'));`,
			conName: "t_a_check",
		},
		{
			name:    "CHECK reaching its column through a named argument",
			sql:     `CREATE TABLE public.t (a integer, CHECK (f(x => a, y => 1) > 0));`,
			conName: "t_a_check",
		},
		{
			name:    "CHECK reaching its column through xmlelement",
			sql:     `CREATE TABLE public.t (a text, CHECK (xmlelement(name e, a) IS NOT NULL));`,
			conName: "t_a_check",
		},
		{
			name:    "CHECK reaching its column through xmlserialize",
			sql:     `CREATE TABLE public.t (a xml, b integer, CHECK (xmlserialize(content a AS text) > ''));`,
			conName: "t_a_check",
		},
		{
			name:    "CHECK on a two-column row constructor",
			sql:     `CREATE TABLE public.t (a integer, b integer, CHECK ((a, b) IS NOT NULL));`,
			conName: "t_check",
		},
		{
			name: "ALTER TABLE ADD FOREIGN KEY",
			sql: `CREATE TABLE public.p (a integer, CONSTRAINT p_pkey PRIMARY KEY (a));
CREATE TABLE public.t (a integer, b integer);
ALTER TABLE public.t ADD FOREIGN KEY (a) REFERENCES public.p (a);`,
			conName:      "t_a_fkey",
			isForeignKey: true,
		},
		{
			name: "ALTER TABLE ADD UNIQUE",
			sql: `CREATE TABLE public.t (a integer, b integer);
ALTER TABLE public.t ADD UNIQUE (a, b);`,
			conName: "t_a_b_key",
		},
		{
			name: "ALTER TABLE ADD CHECK",
			sql: `CREATE TABLE public.t (a integer, b integer);
ALTER TABLE public.t ADD CHECK (a > 0);`,
			conName: "t_a_check",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseSQLWithPublicSchema(tt.sql)
			require.NoError(t, err)
			tbl := result.Tables.Get("public.t")
			require.NotNil(t, tbl)
			if tt.isForeignKey {
				_, ok := tbl.ForeignKeys.GetOk(tt.conName)
				assert.True(t, ok, "expected FK %s, got %v", tt.conName, tbl.ForeignKeys.CollectKeys())
			} else {
				_, ok := tbl.Constraints.GetOk(tt.conName)
				assert.True(t, ok, "expected constraint %s, got %v", tt.conName, tbl.Constraints.CollectKeys())
			}
		})
	}
}

func TestParseSQL_TablespaceOnCreate(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
) TABLESPACE my_ts;`

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl := result.Tables.Get("public.items")
	require.NotNil(t, tbl)
	_, ok := tbl.Constraints.GetOk("items_code_unique")
	assert.True(t, ok)
}

func TestParseSQL_AlterTableUnknownTable(t *testing.T) {
	// ALTER TABLE referencing a table not in parsed result is silently skipped
	sql := `ALTER TABLE public.nonexistent ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES public.other(id);`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Tables.Len())
}

func TestParseSQL_CommentOnUnknownTable(t *testing.T) {
	// COMMENT on unknown table is silently skipped
	sql := `COMMENT ON TABLE public.nonexistent IS 'test';`
	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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
	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	tbl := result.Tables.Get("public.items")
	require.NotNil(t, tbl)
	_, ok := tbl.Constraints.GetOk("items_pkey")
	assert.True(t, ok)
}

func TestParseSQL_AutoNameConstraintTruncation(t *testing.T) {
	// PostgreSQL builds an auto-generated constraint name with makeObjectName:
	// it shortens the table and column parts until the whole name fits in 63
	// bytes and always keeps the trailing label. The expected names below come
	// from PostgreSQL 16.
	tests := []struct {
		name     string
		table    string
		body     string
		expected string
	}{
		{
			name:     "column check",
			table:    strings.Repeat("a", 55),
			body:     "quantity integer CHECK (quantity > 0)",
			expected: strings.Repeat("a", 48) + "_quantity_check",
		},
		{
			name:     "column check with multibyte table name",
			table:    strings.Repeat("\u3042", 20),
			body:     "quantity integer CHECK (quantity > 0)",
			expected: strings.Repeat("\u3042", 16) + "_quantity_check",
		},
		{
			name:     "primary key keeps the whole table name",
			table:    strings.Repeat("p", 55),
			body:     "id integer PRIMARY KEY",
			expected: strings.Repeat("p", 55) + "_pkey",
		},
		{
			name:     "unique over two long columns",
			table:    strings.Repeat("b", 40),
			body:     strings.Repeat("c", 30) + " integer, " + strings.Repeat("d", 30) + " integer, UNIQUE (" + strings.Repeat("c", 30) + ", " + strings.Repeat("d", 30) + ")",
			expected: strings.Repeat("b", 29) + "_" + strings.Repeat("c", 29) + "_key",
		},
		{
			name:     "long column name is shortened instead of the table name",
			table:    "t1",
			body:     strings.Repeat("c", 60) + " integer, UNIQUE (" + strings.Repeat("c", 60) + ")",
			expected: "t1_" + strings.Repeat("c", 56) + "_key",
		},
		{
			name:     "multibyte column name is cut on a character boundary",
			table:    "t2",
			body:     `"` + strings.Repeat("\u3042", 20) + `" integer, UNIQUE ("` + strings.Repeat("\u3042", 20) + `")`,
			expected: "t2_" + strings.Repeat("\u3042", 18) + "_key",
		},
		{
			name:     "cut landing inside a character backs up to its start",
			table:    "ab" + strings.Repeat("\u3042", 20),
			body:     "quantity integer CHECK (quantity > 0)",
			expected: "ab" + strings.Repeat("\u3042", 15) + "_quantity_check",
		},
		{
			name:     "every key column joins into the name before it is shortened",
			table:    "tbl",
			body:     strings.Repeat("c", 20) + " integer, " + strings.Repeat("d", 20) + " integer, " + strings.Repeat("e", 20) + " integer, " + strings.Repeat("f", 20) + " integer, UNIQUE (" + strings.Repeat("c", 20) + ", " + strings.Repeat("d", 20) + ", " + strings.Repeat("e", 20) + ", " + strings.Repeat("f", 20) + ")",
			expected: "tbl_" + strings.Repeat("c", 20) + "_" + strings.Repeat("d", 20) + "_" + strings.Repeat("e", 13) + "_key",
		},
		{
			name:     "exclusion",
			table:    strings.Repeat("e", 40),
			body:     strings.Repeat("c", 30) + " integer, EXCLUDE (" + strings.Repeat("c", 30) + " WITH =)",
			expected: strings.Repeat("e", 29) + "_" + strings.Repeat("c", 28) + "_excl",
		},
		{
			name:     "table check over two columns has no column part",
			table:    strings.Repeat("g", 58),
			body:     "x integer, y integer, CHECK (x > y)",
			expected: strings.Repeat("g", 57) + "_check",
		},
		{
			name:     "table check that already fits is left alone",
			table:    strings.Repeat("g", 57),
			body:     "x integer, y integer, CHECK (x > y)",
			expected: strings.Repeat("g", 57) + "_check",
		},
		{
			name:     "table check with multibyte table name",
			table:    strings.Repeat("\u3044", 20),
			body:     "x integer, y integer, CHECK (x > y)",
			expected: strings.Repeat("\u3044", 19) + "_check",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := `CREATE TABLE public."` + tt.table + `" (` + tt.body + `);`
			result, err := parseSQLWithPublicSchema(sql)
			require.NoError(t, err)
			tbl := result.Tables.Get(model.Ident("public", tt.table))
			require.NotNil(t, tbl)
			_, ok := tbl.Constraints.GetOk(tt.expected)
			assert.True(t, ok, "constraint names: %v", tbl.Constraints.CollectKeys())
			assert.LessOrEqual(t, len(tt.expected), 63)
		})
	}
}

func TestParseSQL_AutoNameIndex(t *testing.T) {
	// An index written without a name takes the name PostgreSQL would choose.
	// The expected names come from PostgreSQL 16.
	tests := []struct {
		name     string
		index    string
		expected string
	}{
		{name: "single column", index: "(a)", expected: "t_a_idx"},
		{name: "two columns", index: "(a, b)", expected: "t_a_b_idx"},
		{name: "included column joins the name", index: "(b) INCLUDE (c)", expected: "t_b_c_idx"},
		{name: "access method does not", index: "USING hash (b)", expected: "t_b_idx"},
		{name: "function call takes the function name", index: "((lower(c)))", expected: "t_lower_idx"},
		{name: "operator takes expr", index: "((a + b), c)", expected: "t_expr_c_idx"},
		{name: "repeated name is numbered", index: "((lower(c)), (lower(substr(c, 1, 1))))", expected: "t_lower_lower1_idx"},
		{name: "cast keeps the name under it", index: "((a::text))", expected: "t_a_idx"},
		{name: "cast over an expression takes the type", index: "(((a + b)::text::varchar))", expected: "t_varchar_idx"},
		{name: "cast does not override a function name", index: "((coalesce(a, b)::text))", expected: "t_coalesce_idx"},
		{name: "case takes its else branch", index: "((CASE WHEN a > 0 THEN 1 ELSE b END))", expected: "t_b_idx"},
		{name: "case with no name of its own", index: "((CASE WHEN a > 0 THEN 1 ELSE 2 END))", expected: "t_case_idx"},
		{name: "case with no else branch", index: "((CASE WHEN a > 0 THEN 1 END))", expected: "t_case_idx"},
		{name: "greatest", index: "((greatest(a, b)))", expected: "t_greatest_idx"},
		{name: "least", index: "((least(a, b)))", expected: "t_least_idx"},
		{name: "nullif", index: "((nullif(a, b)))", expected: "t_nullif_idx"},
		{name: "array", index: "((ARRAY[a, b]::bigint[]))", expected: "t_array_idx"},
		{name: "collate keeps the name under it", index: `((c COLLATE "C"))`, expected: "t_c_idx"},
		{name: "collate over a function call", index: `((upper(c) COLLATE "C"))`, expected: "t_upper_idx"},
		{name: "unknown expression takes expr", index: "((j ->> 'k'))", expected: "t_expr_idx"},
		{name: "sort order is not part of the name", index: "(b DESC NULLS FIRST)", expected: "t_b_idx"},
		{name: "operator class is not either", index: "(c text_pattern_ops)", expected: "t_c_idx"},
		{name: "partial index is named after its columns", index: "(a) WHERE b > 0", expected: "t_a_idx"},
		{name: "subscript takes the column under it", index: "((arr[1]))", expected: "t_arr_idx"},
		{name: "slice takes it too", index: "((arr[1:2]))", expected: "t_arr_idx"},
		{name: "field selection takes the field", index: "(((comp).x))", expected: "t_x_idx"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := "CREATE TYPE public.ct AS (x integer, y integer);\n" +
				"CREATE TABLE public.t (a integer, b integer, c text, j jsonb, arr integer[], comp public.ct);\n" +
				"CREATE INDEX ON public.t " + tt.index + ";"
			result, err := parseSQLWithPublicSchema(sql)
			require.NoError(t, err)
			tbl := result.Tables.Get("public.t")
			require.NotNil(t, tbl)
			idx, ok := tbl.Indexes.GetOk(tt.expected)
			require.True(t, ok, "index names: %v", tbl.Indexes.CollectKeys())
			assert.Contains(t, idx.Definition, tt.expected)
		})
	}
}

func TestParseSQL_AutoNameIndexConcurrently(t *testing.T) {
	// The name is chosen with CONCURRENTLY already stripped, so the stored
	// definition stays canonical and Concurrently remains the only record.
	sql := `CREATE TABLE public.t (
    a integer
);
CREATE INDEX CONCURRENTLY ON public.t (a);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	tbl := result.Tables.Get("public.t")
	require.NotNil(t, tbl)
	idx, ok := tbl.Indexes.GetOk("t_a_idx")
	require.True(t, ok, "index names: %v", tbl.Indexes.CollectKeys())
	assert.True(t, idx.Concurrently)
	assert.NotContains(t, idx.Definition, "CONCURRENTLY")
}

func TestParseSQL_AutoNameIndexTruncation(t *testing.T) {
	// The name goes through makeObjectName, so an over-long one is shortened
	// the way PostgreSQL shortens it rather than cut from the right.
	table := strings.Repeat("a", 55)
	sql := "CREATE TABLE public." + table + " (quantity integer);\n" +
		"CREATE INDEX ON public." + table + " (quantity);"
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	tbl := result.Tables.Get("public." + table)
	require.NotNil(t, tbl)
	_, ok := tbl.Indexes.GetOk(strings.Repeat("a", 50) + "_quantity_idx")
	assert.True(t, ok, "index names: %v", tbl.Indexes.CollectKeys())
}

func TestParseSQL_AutoNameIndexDuplicate(t *testing.T) {
	// Two unnamed indexes that shorten to one name are rejected, the way two
	// unnamed constraints are. PostgreSQL would number the second one.
	sql := `CREATE TABLE public.t (a integer, b integer);
CREATE INDEX ON public.t (a);
CREATE INDEX ON public.t (a) WHERE b > 0;`
	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate index: t_a_idx on public.t", err.Error())
}

func TestParseSQL_NullColumnConstraintIsNotAConstraint(t *testing.T) {
	// NULL is a column attribute, not something PostgreSQL records in
	// pg_constraint, so it takes no auto-generated name and adds no
	// constraint to the table.
	sql := `CREATE TABLE public.items (id integer NULL);`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	tbl := result.Tables.Get("public.items")
	require.NotNil(t, tbl)
	assert.Equal(t, 0, tbl.Constraints.Len())
}

func TestParseSQL_AutoNameForeignKeyTruncation(t *testing.T) {
	// The foreign key path names the constraint separately from the other
	// table constraints, so it needs its own coverage.
	table := strings.Repeat("f", 40)
	col := strings.Repeat("c", 30)
	sql := `CREATE TABLE public.ref_t (x integer NOT NULL, CONSTRAINT ref_t_pkey PRIMARY KEY (x));
CREATE TABLE public.` + table + ` (` + col + ` integer REFERENCES public.ref_t(x));`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	tbl := result.Tables.Get("public." + table)
	require.NotNil(t, tbl)
	_, ok := tbl.ForeignKeys.GetOk(strings.Repeat("f", 29) + "_" + strings.Repeat("c", 28) + "_fkey")
	assert.True(t, ok, "foreign key names: %v", tbl.ForeignKeys.CollectKeys())
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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
-- pista:renamed-from public.old_stats
CREATE MATERIALIZED VIEW public.user_stats AS SELECT count(*) AS cnt FROM public.users;`

	result, err := parseSQLWithPublicSchema(sql)
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

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate materialized view")
}

func TestParseSQL_MaterializedView_IndexDuplicate(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE MATERIALIZED VIEW public.mv AS SELECT count(*) AS cnt FROM public.users;
CREATE INDEX idx ON public.mv (cnt);
CREATE INDEX idx ON public.mv (cnt);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate index: idx on public.mv", err.Error())
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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
-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'my_func')
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`

	result, err := parseSQLWithPublicSchema(sql)
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
-- pista:execute
GRANT SELECT ON public.users TO readonly_role;`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Tables.Len())
	require.Len(t, result.ExecuteStmts, 1)
	assert.Contains(t, result.ExecuteStmts[0].SQL, "GRANT select")
	assert.Empty(t, result.ExecuteStmts[0].CheckSQL)
}

func TestParseSQL_IndexOnUnknownTableSkipped(t *testing.T) {
	sql := `CREATE INDEX idx_name ON public.nonexistent USING btree (name);`
	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_users_name")
	require.True(t, ok)
	require.NotNil(t, idx.TableSpace)
	assert.Equal(t, "fast_ssd", *idx.TableSpace)
}

func TestParseSQL_AlterTableNonAddConstraint(t *testing.T) {
	var buf bytes.Buffer
	restore := parser.SetWarnWriter(&buf)
	defer restore()

	// ALTER TABLE with non-ADD CONSTRAINT commands is skipped, with a warning
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
ALTER TABLE public.users DROP COLUMN name;`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	// The parser does not apply DDL changes; the column should still be present
	_, ok = tbl.Columns.GetOk("name")
	assert.True(t, ok)
	assert.Contains(t, buf.String(), "ignored unsupported statement")
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

	result, err := parseSQLWithPublicSchema(sql)
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

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate table")
}

func TestParseSQL_DuplicateView(t *testing.T) {
	sql := `CREATE VIEW public.v1 AS SELECT 1;
CREATE VIEW public.v1 AS SELECT 2;`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate view")
}

func TestParseSQL_DuplicateIndex(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL, name text, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE INDEX idx_name ON public.users (name);
CREATE INDEX idx_name ON public.users (name);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate index: idx_name on public.users", err.Error())
}

func TestParseSQL_DuplicateConstraint(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL,
    val integer NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id),
    CONSTRAINT items_pkey PRIMARY KEY (id)
);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate constraint: items_pkey on public.items", err.Error())
}

func TestParseSQL_DuplicateColumn(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate column: id on public.users", err.Error())
}

func TestParseSQL_DuplicateForeignKeyAlterTable(t *testing.T) {
	sql := `CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE TABLE public.orders (id integer NOT NULL, user_id integer NOT NULL, CONSTRAINT orders_pkey PRIMARY KEY (id));
ALTER TABLE public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);
ALTER TABLE public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate foreign key: fk_user on public.orders", err.Error())
}

func TestParseSQL_DuplicateConstraintAlterTable(t *testing.T) {
	sql := `CREATE TABLE public.items (id integer NOT NULL, code text NOT NULL);
ALTER TABLE public.items ADD CONSTRAINT items_code_unique UNIQUE (code);
ALTER TABLE public.items ADD CONSTRAINT items_code_unique UNIQUE (code);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate constraint: items_code_unique on public.items", err.Error())
}

func TestParseSQL_DuplicateInlineForeignKey(t *testing.T) {
	sql := `CREATE TABLE public.groups (id integer NOT NULL, CONSTRAINT groups_pkey PRIMARY KEY (id));
CREATE TABLE public.items (
    id integer NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id),
    CONSTRAINT items_fk FOREIGN KEY (id) REFERENCES public.groups(id),
    CONSTRAINT items_fk FOREIGN KEY (id) REFERENCES public.groups(id)
);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate foreign key: items_fk on public.items", err.Error())
}

func TestParseSQL_DuplicateColumnLevelConstraint(t *testing.T) {
	sql := `CREATE TABLE public.items (
    id integer NOT NULL CONSTRAINT items_pkey PRIMARY KEY,
    code text NOT NULL CONSTRAINT items_pkey UNIQUE
);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate constraint: items_pkey on public.items", err.Error())
}

func TestParseSQL_DuplicateColumnLevelForeignKey(t *testing.T) {
	sql := `CREATE TABLE public.groups (id integer NOT NULL, CONSTRAINT groups_pkey PRIMARY KEY (id));
CREATE TABLE public.items (
    id integer NOT NULL CONSTRAINT items_fk REFERENCES public.groups(id),
    code integer NOT NULL CONSTRAINT items_fk REFERENCES public.groups(id)
);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate foreign key: items_fk on public.items", err.Error())
}

func TestParseSQL_PolicyOnUnknownTable(t *testing.T) {
	sql := `CREATE POLICY p ON public.missing FOR SELECT USING (true);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parent table")
}

func TestParseSQL_DuplicatePolicy(t *testing.T) {
	sql := `CREATE TABLE public.documents (id bigint NOT NULL, owner text, CONSTRAINT documents_pkey PRIMARY KEY (id));
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON public.documents FOR SELECT USING (owner = current_user);
CREATE POLICY p ON public.documents FOR ALL USING (owner = current_user);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Equal(t, "duplicate policy: p on public.documents", err.Error())
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("myschema.status")
	require.True(t, ok)
	assert.Equal(t, "myschema", e.Schema)
	assert.Equal(t, "status", e.Name)
}

func TestParseSQL_DuplicateEnum(t *testing.T) {
	sql := `CREATE TYPE public.status AS ENUM ('active');
CREATE TYPE public.status AS ENUM ('inactive');`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate enum")
}

func TestParseSQL_CommentOnUnknownEnum(t *testing.T) {
	sql := `COMMENT ON TYPE public.nonexistent IS 'test';`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Enums.Len())
}

func TestParseSQL_RenameDirective_Enum(t *testing.T) {
	sql := `-- pista:renamed-from public.old_status
CREATE TYPE public.new_status AS ENUM ('active', 'inactive');`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.new_status")
	require.True(t, ok)
	require.NotNil(t, e.RenameFrom)
	assert.Equal(t, "public.old_status", *e.RenameFrom)
}

func TestParseSQL_RenameDirective_EnumValue(t *testing.T) {
	sql := `CREATE TYPE public.status AS ENUM (
    'active',
    -- pista:renamed-from 'inactive'
    'disabled'
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Nil(t, e.RenameFrom)
	assert.Equal(t, map[string]string{"disabled": "inactive"}, e.ValueRenameFrom)
}

func TestParseSQL_RenameDirective_EnumValue_BareArg(t *testing.T) {
	// The old value may be written without quotes.
	sql := `CREATE TYPE public.status AS ENUM (
    -- pista:renamed-from inactive
    'disabled'
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Equal(t, map[string]string{"disabled": "inactive"}, e.ValueRenameFrom)
}

func TestParseSQL_RenameDirective_EnumValue_QuotedEscape(t *testing.T) {
	// Values containing single quotes use '' escapes on both sides.
	sql := `CREATE TYPE public.status AS ENUM (
    -- pista:renamed-from 'don''t'
    'won''t'
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Equal(t, map[string]string{"won't": "don't"}, e.ValueRenameFrom)
}

func TestParseSQL_RenameDirective_EnumValue_UnterminatedQuoteArg(t *testing.T) {
	// An argument with an unterminated quote is not a valid literal and is
	// kept as-is (bare), including the leading quote.
	sql := `CREATE TYPE public.status AS ENUM (
    -- pista:renamed-from 'oops
    'disabled'
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Equal(t, map[string]string{"disabled": "'oops"}, e.ValueRenameFrom)
}

func TestParseSQL_RenameDirective_EnumValue_CaseSensitive(t *testing.T) {
	// Enum values are literals, not identifiers: case must be preserved.
	sql := `CREATE TYPE public.status AS ENUM (
    -- pista:renamed-from Inactive
    'Disabled'
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Equal(t, map[string]string{"Disabled": "Inactive"}, e.ValueRenameFrom)
}

func TestParseSQL_RenameDirective_EnumValue_KeepsPendingAcrossComments(t *testing.T) {
	sql := `CREATE TYPE public.status AS ENUM (
    'active',
    -- pista:renamed-from 'inactive'
    -- a plain comment
    'disabled'
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Equal(t, map[string]string{"disabled": "inactive"}, e.ValueRenameFrom)
}

func TestParseSQL_RenameDirective_EnumValue_MultiLineValue(t *testing.T) {
	// String literals may span lines; the lexer-based extraction still pairs
	// the directive with the following value.
	sql := `CREATE TYPE public.status AS ENUM (
    -- pista:renamed-from 'old'
    'multi
line'
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Equal(t, map[string]string{"multi\nline": "old"}, e.ValueRenameFrom)
}

func TestParseSQL_RenameDirective_EnumValue_TrailingCommentIgnored(t *testing.T) {
	// A directive must be on its own line; a trailing comment after a value
	// is not a directive for the next value.
	sql := `CREATE TYPE public.status AS ENUM (
    'active', -- pista:renamed-from 'x'
    'disabled'
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Empty(t, e.ValueRenameFrom)
}

func TestParseSQL_RenameDirective_EnumValue_DanglingIgnored(t *testing.T) {
	// A directive not followed by a value literal is ignored.
	sql := `CREATE TYPE public.status AS ENUM (
    'active'
    -- pista:renamed-from 'inactive'
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.Empty(t, e.ValueRenameFrom)
}

func TestParseSQL_IgnoreDirective_Table(t *testing.T) {
	sql := `-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	legacy, ok := result.Tables.GetOk("public.legacy")
	require.True(t, ok)
	assert.True(t, legacy.Ignore)

	users, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	assert.False(t, users.Ignore)
}

func TestParseSQL_IgnoreDirective_View(t *testing.T) {
	sql := `-- pista:ignore
CREATE VIEW public.v AS SELECT 1;`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	v, ok := result.Views.GetOk("public.v")
	require.True(t, ok)
	assert.True(t, v.Ignore)
}

func TestParseSQL_IgnoreDirective_Enum(t *testing.T) {
	sql := `-- pista:ignore
CREATE TYPE public.status AS ENUM ('active');`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	assert.True(t, e.Ignore)
}

func TestParseSQL_IgnoreDirective_Domain(t *testing.T) {
	sql := `-- pista:ignore
CREATE DOMAIN public.email AS text;`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	d, ok := result.Domains.GetOk("public.email")
	require.True(t, ok)
	assert.True(t, d.Ignore)
}

func TestParseSQL_IgnoreDirective_SkipsColumnRefValidation(t *testing.T) {
	// An ignored table is unmanaged, so a stale column reference in its own
	// index must not fail parse-time validation.
	sql := `-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL
);
CREATE INDEX legacy_idx ON public.legacy (nonexistent);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	legacy, ok := result.Tables.GetOk("public.legacy")
	require.True(t, ok)
	assert.True(t, legacy.Ignore)
}

func TestParseSQL_RenameDirective_EnumTypeAndValue(t *testing.T) {
	// A directive above CREATE TYPE renames the type; directives inside the
	// value list rename values. Both can be combined.
	sql := `-- pista:renamed-from public.old_status
CREATE TYPE public.status AS ENUM (
    'active',
    -- pista:renamed-from 'inactive'
    'disabled'
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	e, ok := result.Enums.GetOk("public.status")
	require.True(t, ok)
	require.NotNil(t, e.RenameFrom)
	assert.Equal(t, "public.old_status", *e.RenameFrom)
	assert.Equal(t, map[string]string{"disabled": "inactive"}, e.ValueRenameFrom)
}

func TestParseSQL_RenameDirective_Table(t *testing.T) {
	sql := `-- pista:renamed-from public.old_users
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	require.NotNil(t, tbl.RenameFrom)
	assert.Equal(t, "public.old_users", *tbl.RenameFrom)
}

func TestParseSQL_RenameDirective_View(t *testing.T) {
	sql := `-- pista:renamed-from public.old_view
CREATE VIEW public.new_view AS SELECT 1;`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	v, ok := result.Views.GetOk("public.new_view")
	require.True(t, ok)
	require.NotNil(t, v.RenameFrom)
	assert.Equal(t, "public.old_view", *v.RenameFrom)
}

func TestParseSQL_RenameDirective_Column(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	result, err := parseSQLWithPublicSchema(sql)
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
-- pista:renamed-from idx_old
CREATE INDEX idx_new ON public.users (name);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_new")
	require.True(t, ok)
	require.NotNil(t, idx.RenameFrom)
	assert.Equal(t, "idx_old", *idx.RenameFrom)
}

func TestParseSQL_RenameDirective_Policy(t *testing.T) {
	sql := `CREATE TABLE public.documents (
    id bigint NOT NULL,
    owner text NOT NULL,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
-- pista:renamed-from old_policy
CREATE POLICY new_policy ON public.documents FOR SELECT USING (owner = current_user);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.documents")
	require.True(t, ok)
	pol, ok := tbl.Policies.GetOk("new_policy")
	require.True(t, ok)
	require.NotNil(t, pol.RenameFrom)
	assert.Equal(t, "old_policy", *pol.RenameFrom)
}

func TestParseSQL_RenameDirective_Constraint(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    code text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    -- pista:renamed-from old_unique
    CONSTRAINT new_unique UNIQUE (code)
);`

	result, err := parseSQLWithPublicSchema(sql)
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
-- pista:renamed-from old_fk
ALTER TABLE public.orders ADD CONSTRAINT new_fk FOREIGN KEY (user_id) REFERENCES public.users(id);`

	result, err := parseSQLWithPublicSchema(sql)
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
-- pista:renamed-from old_unique
ALTER TABLE public.users ADD CONSTRAINT new_unique UNIQUE (code);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("new_unique")
	require.True(t, ok)
	require.NotNil(t, con.RenameFrom)
	assert.Equal(t, "old_unique", *con.RenameFrom)
}

func TestParseSQLWithSchema_RenameDirective_Qualifies(t *testing.T) {
	sql := `-- pista:renamed-from old_status
CREATE TYPE new_status AS ENUM ('active', 'inactive');
-- pista:renamed-from old_users
CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:renamed-from old_view
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.name")
	require.NotNil(t, d)
	require.NotNil(t, d.Collation)
	assert.Equal(t, `"en_US"`, *d.Collation)
}

func TestParseSQL_DomainDefaultCollationExcluded(t *testing.T) {
	sql := `CREATE DOMAIN public.name AS text COLLATE "default";`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.name")
	require.NotNil(t, d)
	assert.Nil(t, d.Collation)
}

func TestParseSQL_DomainCommentRemove(t *testing.T) {
	sql := `CREATE DOMAIN public.pos_int AS integer;
COMMENT ON DOMAIN public.pos_int IS 'Positive integer';
COMMENT ON DOMAIN public.pos_int IS '';`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.pos_int")
	require.NotNil(t, d)
	assert.Nil(t, d.Comment)
}

func TestParseSQL_DomainMultipleConstraints(t *testing.T) {
	sql := `CREATE DOMAIN public.bounded_int AS integer
    CONSTRAINT min_check CHECK (VALUE >= 0)
    CONSTRAINT max_check CHECK (VALUE <= 100);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.bounded_int")
	require.NotNil(t, d)
	assert.Len(t, d.Constraints, 2)
}

func TestParseSQL_DomainRenameDirective(t *testing.T) {
	sql := `-- pista:renamed-from public.old_domain
CREATE DOMAIN public.new_domain AS integer;`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	d := result.Domains.Get("public.new_domain")
	require.NotNil(t, d)
	require.NotNil(t, d.RenameFrom)
	assert.Equal(t, "public.old_domain", *d.RenameFrom)
}

func TestParseSQL_DomainUnnamedConstraintAutoNamed(t *testing.T) {
	sql := `CREATE DOMAIN public.pos_int AS integer CHECK (VALUE > 0);`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	d := result.Domains.Get("public.pos_int")
	require.NotNil(t, d)
	require.Len(t, d.Constraints, 1)
	assert.Equal(t, "pos_int_check", d.Constraints[0].Name)
}

func TestParseSQL_DomainUnnamedConstraintAutoNameTruncated(t *testing.T) {
	domain := strings.Repeat("d", 60)
	sql := `CREATE DOMAIN public.` + domain + ` AS integer CHECK (VALUE > 0);`
	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)
	d := result.Domains.Get("public." + domain)
	require.NotNil(t, d)
	require.Len(t, d.Constraints, 1)
	assert.Equal(t, strings.Repeat("d", 57)+"_check", d.Constraints[0].Name)
}

func TestParseSQL_DuplicateDomain(t *testing.T) {
	sql := `CREATE DOMAIN public.pos_int AS integer;
CREATE DOMAIN public.pos_int AS bigint;`
	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate domain")
}

func TestParseSQL_NoRenameDirective(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	result, err := parseSQLWithPublicSchema(sql)
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
-- pista:concurrently
CREATE INDEX idx_name ON public.users (name);`

	result, err := parseSQLWithPublicSchema(sql)
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

	result, err := parseSQLWithPublicSchema(sql)
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
-- pista:renamed-from idx_old
-- pista:concurrently
CREATE INDEX idx_name ON public.users (name);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_name")
	require.True(t, ok)
	assert.True(t, idx.Concurrently)
	require.NotNil(t, idx.RenameFrom)
	assert.Equal(t, "idx_old", *idx.RenameFrom)
}

func TestParseSQL_InlineConcurrently_SetsConcurrentlyFlag(t *testing.T) {
	// CREATE INDEX CONCURRENTLY in input SQL must set Index.Concurrently so
	// that --with-tx detection and --disable-index-concurrently work correctly.
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX CONCURRENTLY idx_name ON public.users (name);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_name")
	require.True(t, ok)
	assert.True(t, idx.Concurrently)
	// Definition must be canonical (without CONCURRENTLY); Concurrently
	// is the single source of truth and gets re-applied at diff time.
	assert.NotContains(t, idx.Definition, "CONCURRENTLY")
}

func TestParseSQL_InlineConcurrently_Unique(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX CONCURRENTLY idx_name ON public.users (name);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_name")
	require.True(t, ok)
	assert.True(t, idx.Concurrently)
	assert.NotContains(t, idx.Definition, "CONCURRENTLY")
	assert.Contains(t, idx.Definition, "CREATE UNIQUE INDEX")
}

func TestParseSQL_InlineConcurrently_AndDirective(t *testing.T) {
	// Directive on top of inline CONCURRENTLY must still resolve to true (idempotent).
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:concurrently
CREATE INDEX CONCURRENTLY idx_name ON public.users (name);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	idx, ok := tbl.Indexes.GetOk("idx_name")
	require.True(t, ok)
	assert.True(t, idx.Concurrently)
}

func TestParseSQL_BulkAlterDirective_Table(t *testing.T) {
	sql := `-- pista:bulk-alter
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	users, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	assert.True(t, users.BulkAlter)

	posts, ok := result.Tables.GetOk("public.posts")
	require.True(t, ok)
	assert.False(t, posts.BulkAlter)
}

func TestParseSQL_BulkAlterDirective_WithRenamed(t *testing.T) {
	sql := `-- pista:renamed-from public.old_users
-- pista:bulk-alter
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	assert.True(t, tbl.BulkAlter)
	require.NotNil(t, tbl.RenameFrom)
	assert.Equal(t, "public.old_users", *tbl.RenameFrom)
}

func TestParseSQL_BulkAlterDirective_IgnoredOnNonTable(t *testing.T) {
	// -- pista:bulk-alter before a CREATE INDEX should be silently ignored
	// and should NOT leak to the following CREATE TABLE
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:bulk-alter
CREATE INDEX idx_name ON public.users (name);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	users, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	assert.False(t, users.BulkAlter)

	posts, ok := result.Tables.GetOk("public.posts")
	require.True(t, ok)
	assert.False(t, posts.BulkAlter)
}

func TestParseSQL_ConcurrentlyDirective_IgnoredOnNonIndex(t *testing.T) {
	// -- pista:concurrently before a CREATE TABLE should be silently ignored
	// and should NOT leak to the following CREATE INDEX
	sql := `-- pista:concurrently
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_id ON public.users (id);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.users")
	require.True(t, ok)
	assert.NotNil(t, tbl)

	idx, ok := tbl.Indexes.GetOk("idx_users_id")
	require.True(t, ok)
	assert.False(t, idx.Concurrently)
}

func TestParseAlterTableConstraints_Multiple(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.orders (id integer, amount integer, code text);
		ALTER TABLE public.orders
			ADD CONSTRAINT orders_amount_check CHECK (amount > 0),
			ADD CONSTRAINT orders_code_key UNIQUE (code);
	`)
	require.NoError(t, err)
	tbl, ok := result.Tables.GetOk("public.orders")
	require.True(t, ok)
	assert.Equal(t, 2, tbl.Constraints.Len())
	_, ok = tbl.Constraints.GetOk("orders_amount_check")
	assert.True(t, ok)
	_, ok = tbl.Constraints.GetOk("orders_code_key")
	assert.True(t, ok)
}

func TestParseAlterTableConstraints_MultipleWithForeignKey(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.users (id integer PRIMARY KEY);
		CREATE TABLE public.orders (id integer, user_id integer, code text);
		ALTER TABLE public.orders
			ADD CONSTRAINT orders_code_key UNIQUE (code),
			ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id);
	`)
	require.NoError(t, err)
	tbl, ok := result.Tables.GetOk("public.orders")
	require.True(t, ok)
	assert.Equal(t, 1, tbl.Constraints.Len())
	assert.Equal(t, 1, tbl.ForeignKeys.Len())
	fk, ok := tbl.ForeignKeys.GetOk("orders_user_id_fkey")
	require.True(t, ok)
	require.NotNil(t, fk.RefTable)
	assert.Equal(t, "users", *fk.RefTable)
}

func TestParseAlterTableConstraints_AmbiguousRename(t *testing.T) {
	_, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.orders (id integer, amount integer, code text);
		-- pista:renamed-from orders_old
		ALTER TABLE public.orders
			ADD CONSTRAINT orders_amount_check CHECK (amount > 0),
			ADD CONSTRAINT orders_code_key UNIQUE (code);
	`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pista:renamed-from is ambiguous")
}

func TestParseAlterTableConstraints_SingleRenameStillApplies(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.orders (id integer, code text);
		-- pista:renamed-from orders_old_key
		ALTER TABLE public.orders ADD CONSTRAINT orders_code_key UNIQUE (code);
	`)
	require.NoError(t, err)
	tbl, ok := result.Tables.GetOk("public.orders")
	require.True(t, ok)
	con, ok := tbl.Constraints.GetOk("orders_code_key")
	require.True(t, ok)
	require.NotNil(t, con.RenameFrom)
	assert.Equal(t, "orders_old_key", *con.RenameFrom)
}

func TestParseCommentStmt_PartitionChildColumn(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.events (id integer, created_at date) PARTITION BY RANGE (created_at);
		CREATE TABLE public.events_2024 PARTITION OF public.events
			FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		COMMENT ON COLUMN public.events_2024.id IS 'event id';
	`)
	require.NoError(t, err)
	child, ok := result.Tables.GetOk("public.events_2024")
	require.True(t, ok)
	// The child declares no columns, so the comment creates the only entry.
	col, ok := child.Columns.GetOk("id")
	require.True(t, ok)
	require.NotNil(t, col.Comment)
	assert.Equal(t, "event id", *col.Comment)
}

func TestParseCommentStmt_InheritsChildColumnNotSynthesized(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE TABLE public.parent (id integer, name text);
		CREATE TABLE public.child () INHERITS (public.parent);
		COMMENT ON COLUMN public.child.id IS 'inherited';
	`)
	require.NoError(t, err)
	child, ok := result.Tables.GetOk("public.child")
	require.True(t, ok)
	// An INHERITS child goes through the regular column diff, where a bodyless
	// entry would look like a new column, so no entry is created.
	assert.Equal(t, 0, child.Columns.Len())
}

func TestReadSQLFile_Stdin_ReadError(t *testing.T) {
	// Reading from the write end of a pipe fails, so ReadSQLFile reports it.
	_, w, err := os.Pipe()
	require.NoError(t, err)
	defer w.Close()

	origStdin := os.Stdin
	os.Stdin = w
	defer func() {
		os.Stdin = origStdin
	}()

	_, err = parser.ReadSQLFile("-")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read SQL from stdin")
}

func TestParseSQL_RenameDirective_InlineForeignKey(t *testing.T) {
	sql := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id),
    -- pista:renamed-from old_fk
    CONSTRAINT new_fk FOREIGN KEY (user_id) REFERENCES public.users(id)
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	tbl, ok := result.Tables.GetOk("public.orders")
	require.True(t, ok)
	fk, ok := tbl.ForeignKeys.GetOk("new_fk")
	require.True(t, ok)
	require.NotNil(t, fk.RenameFrom)
	assert.Equal(t, "old_fk", *fk.RenameFrom)
}

func TestParseSQL_RenameDirective_CompositeType(t *testing.T) {
	sql := `-- pista:renamed-from public.old_addr
CREATE TYPE public.addr AS (
    street text,
    -- pista:renamed-from town
    city text
);`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	ct, ok := result.CompositeTypes.GetOk("public.addr")
	require.True(t, ok)
	require.NotNil(t, ct.RenameFrom)
	assert.Equal(t, "public.old_addr", *ct.RenameFrom)
	require.Len(t, ct.Attributes, 2)
	assert.Nil(t, ct.Attributes[0].RenameFrom)
	require.NotNil(t, ct.Attributes[1].RenameFrom)
	assert.Equal(t, "town", *ct.Attributes[1].RenameFrom)
}

func TestParseSQL_DuplicateCompositeType(t *testing.T) {
	sql := `CREATE TYPE public.addr AS (street text);
CREATE TYPE public.addr AS (street text);`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate composite type")
}

func TestParseSQL_RenameDirective_Sequence(t *testing.T) {
	sql := `-- pista:renamed-from old_seq
CREATE SEQUENCE public.new_seq;`

	result, err := parseSQLWithPublicSchema(sql)
	require.NoError(t, err)

	seq, ok := result.Sequences.GetOk("public.new_seq")
	require.True(t, ok)
	require.NotNil(t, seq.RenameFrom)
	assert.Equal(t, "public.old_seq", *seq.RenameFrom)
}

func TestParseSQL_DuplicateSequence(t *testing.T) {
	sql := `CREATE SEQUENCE public.s1;
CREATE SEQUENCE public.s1;`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate sequence")
}

func TestParseSQL_SequenceValueOverflow(t *testing.T) {
	// A literal past int64 arrives as a Float node and fails ParseInt.
	sql := `CREATE SEQUENCE public.s1 MAXVALUE 99999999999999999999;`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid value "99999999999999999999" for sequence option maxvalue`)
}

func TestParseSQL_IdentityValueOverflow(t *testing.T) {
	sql := `CREATE TABLE public.t1 (id bigint GENERATED ALWAYS AS IDENTITY (START WITH 99999999999999999999));`

	_, err := parseSQLWithPublicSchema(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse identity options for column id")
	assert.Contains(t, err.Error(), `invalid value "99999999999999999999" for sequence option start`)
}

func TestParseSQL_UnloggedSequence(t *testing.T) {
	result, err := parseSQLWithPublicSchema(`
		CREATE UNLOGGED SEQUENCE public.jobs_seq;
		CREATE SEQUENCE public.plain_seq;
	`)
	require.NoError(t, err)

	seq, ok := result.Sequences.GetOk("public.jobs_seq")
	require.True(t, ok)
	assert.True(t, seq.Unlogged)

	seq, ok = result.Sequences.GetOk("public.plain_seq")
	require.True(t, ok)
	assert.False(t, seq.Unlogged)
}
