package parser_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/parser"
)

func writeSQLFiles(t *testing.T, files map[string]string) []string {
	t.Helper()
	dir := t.TempDir()
	var paths []string
	for name, sql := range files {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(sql), 0o644))
		paths = append(paths, path)
	}
	return paths
}

func TestParseSQLFiles_SyntaxErrorLocation(t *testing.T) {
	paths := writeSQLFiles(t, map[string]string{
		"items.sql": `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL
);

CREATE TABEL public.items (
    id integer
);
`,
	})

	_, err := parser.ParseSQLFilesWithSchema(paths, "public")
	require.Error(t, err)
	assert.Equal(t, `failed to parse SQL: syntax error at or near "TABEL"
 --> `+paths[0]+`:6:8
  |
6 | CREATE TABEL public.items (
  |        ^`, err.Error())
}

func TestParseSQLFiles_SyntaxErrorInSecondFile(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "a.sql")
	second := filepath.Join(dir, "b.sql")
	require.NoError(t, os.WriteFile(first, []byte("CREATE TABLE public.users (\n    id integer NOT NULL\n);\n"), 0o644))
	require.NoError(t, os.WriteFile(second, []byte("-- items\nCREATE TABLE public.items (\n    id integer x\n);\n"), 0o644))

	_, err := parser.ParseSQLFilesWithSchema([]string{first, second}, "public")
	require.Error(t, err)
	assert.Equal(t, `failed to parse SQL: syntax error at or near "x"
 --> `+second+`:3:16
  |
3 |     id integer x
  |                ^`, err.Error())
}

func TestParseSQLFiles_SyntaxErrorAtFileStart(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "a.sql")
	second := filepath.Join(dir, "b.sql")
	require.NoError(t, os.WriteFile(first, []byte("CREATE TABLE public.a (id integer);\n"), 0o644))
	require.NoError(t, os.WriteFile(second, []byte(");\n"), 0o644))

	_, err := parser.ParseSQLFilesWithSchema([]string{first, second}, "public")
	require.Error(t, err)
	assert.Equal(t, `failed to parse SQL: syntax error at or near ")"
 --> `+second+`:1:1
  |
1 | );
  | ^`, err.Error())
}

func TestParseSQLFiles_SyntaxErrorInMiddleFile(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "a.sql")
	second := filepath.Join(dir, "b.sql")
	third := filepath.Join(dir, "c.sql")
	require.NoError(t, os.WriteFile(first, []byte("CREATE TABLE public.a (id integer);\n"), 0o644))
	require.NoError(t, os.WriteFile(second, []byte("CREATE TABLE public.b (id integer x);\n"), 0o644))
	require.NoError(t, os.WriteFile(third, []byte("CREATE TABLE public.c (id integer);\n"), 0o644))

	_, err := parser.ParseSQLFilesWithSchema([]string{first, second, third}, "public")
	require.Error(t, err)
	assert.Contains(t, err.Error(), second+":1:35")
}

func TestParseSQLFiles_SyntaxErrorTabIndent(t *testing.T) {
	paths := writeSQLFiles(t, map[string]string{
		"tab.sql": "CREATE TABLE public.items (\n\tid integer x\n);\n",
	})

	_, err := parser.ParseSQLFilesWithSchema(paths, "public")
	require.Error(t, err)
	assert.Equal(t, `failed to parse SQL: syntax error at or near "x"
 --> `+paths[0]+`:2:13
  |
2 | `+"\tid integer x"+`
  | `+"\t           ^", err.Error())
}

func TestParseSQLFiles_SyntaxErrorMultibyteColumn(t *testing.T) {
	// Two 2-byte runes precede the bad token; the column counts runes.
	paths := writeSQLFiles(t, map[string]string{
		"mb.sql": "CREATE TABLE public.items (\n    note text DEFAULT '\u00e9\u00e9' x\n);\n",
	})

	_, err := parser.ParseSQLFilesWithSchema(paths, "public")
	require.Error(t, err)
	assert.Equal(t, "failed to parse SQL: syntax error at or near \"x\"\n"+
		" --> "+paths[0]+":2:28\n"+
		"  |\n"+
		"2 |     note text DEFAULT '\u00e9\u00e9' x\n"+
		"  |                            ^", err.Error())
}

func TestParseSQLFiles_SyntaxErrorAtEndOfInput(t *testing.T) {
	paths := writeSQLFiles(t, map[string]string{
		"eof.sql": "CREATE TABLE public.items (",
	})

	_, err := parser.ParseSQLFilesWithSchema(paths, "public")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error at end of input")
	assert.Contains(t, err.Error(), paths[0]+":1:28")
}

func TestParseSQLFiles_UnknownDirectiveLocation(t *testing.T) {
	paths := writeSQLFiles(t, map[string]string{
		"dir.sql": `CREATE TABLE public.items (
    id integer
);

-- pista:renmaed-from public.old_items
CREATE TABLE public.new_items (
    id integer
);
`,
	})

	_, err := parser.ParseSQLFilesWithSchema(paths, "public")
	require.Error(t, err)
	assert.Equal(t, `unknown directive: -- pista:renmaed-from
 --> `+paths[0]+`:5:10
  |
5 | -- pista:renmaed-from public.old_items
  |          ^`, err.Error())
}

func TestParseSQLFiles_DirectiveWithArgsLocation(t *testing.T) {
	paths := writeSQLFiles(t, map[string]string{
		"dir.sql": "-- pista:concurrently 123\nCREATE INDEX idx ON public.items (id);\n",
	})

	_, err := parser.ParseSQLFilesWithSchema(paths, "public")
	require.Error(t, err)
	assert.Equal(t, `-- pista:concurrently does not accept arguments
 --> `+paths[0]+`:1:1
  |
1 | -- pista:concurrently 123
  | ^`, err.Error())
}

func TestParseSQLFiles_SyntaxErrorOnStdin(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)

	origStdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = origStdin
		r.Close()
	}()

	go func() {
		w.WriteString("CREATE TABEL public.items (id integer);\n")
		w.Close()
	}()

	_, err = parser.ParseSQLFilesWithSchema([]string{"-"}, "public")
	require.Error(t, err)
	assert.Equal(t, `failed to parse SQL: syntax error at or near "TABEL"
 --> <stdin>:1:8
  |
1 | CREATE TABEL public.items (id integer);
  |        ^`, err.Error())
}

func TestAnnotateError_NoPosition(t *testing.T) {
	err := assert.AnError
	got := parser.AnnotateError(err, "CREATE TABLE t (id int);", []parser.FileSpan{parser.NewFileSpan("a.sql", 0)})
	assert.Same(t, err, got)
}

func TestAnnotateError_CursorPastInput(t *testing.T) {
	sql := "CREATE"
	err := parser.NewLocatedError("boom", 100)
	got := parser.AnnotateError(err, sql, []parser.FileSpan{parser.NewFileSpan("a.sql", 0)})
	assert.Equal(t, `boom
 --> a.sql:1:7
  |
1 | CREATE
  |       ^`, got.Error())
}

func TestAnnotateError_WideGutter(t *testing.T) {
	sql := "-- 1\n-- 2\n-- 3\n-- 4\n-- 5\n-- 6\n-- 7\n-- 8\n-- 9\nCREATE x"
	err := parser.NewLocatedError("boom", len(sql)-1)
	got := parser.AnnotateError(err, sql, []parser.FileSpan{parser.NewFileSpan("a.sql", 0)})
	assert.Equal(t, `boom
  --> a.sql:10:8
   |
10 | CREATE x
   |        ^`, got.Error())
}
