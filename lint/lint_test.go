package lint_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/lint"
	"github.com/winebarrel/pistachio/parser"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func parse(t *testing.T, sql string) *parser.ParseResult {
	t.Helper()

	result, err := parser.ParseSQLSourcesWithSchema([]parser.Source{{Name: "schema.sql", SQL: sql}}, "public")
	require.NoError(t, err)
	return result
}

func run(t *testing.T, rules, sql string) []string {
	t.Helper()

	path := writeFile(t, t.TempDir(), "rules.yml", rules)
	linter, err := lint.Load([]string{path}, &bytes.Buffer{})
	require.NoError(t, err)

	violations, err := linter.Run(parse(t, sql), []string{"public"})
	require.NoError(t, err)

	lines := []string{}
	for _, v := range violations {
		lines = append(lines, v.String())
	}
	return lines
}

// The standard rules shipped in the repository, run over a schema that breaks
// each of them once.
func TestStandardRules(t *testing.T) {
	linter, err := lint.Load([]string{filepath.Join("..", "rules")}, &bytes.Buffer{})
	require.NoError(t, err)

	violations, err := linter.Run(parse(t, `
CREATE TABLE public.users (id bigint GENERATED ALWAYS AS IDENTITY, name text NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE TABLE public.posts (
  id bigserial NOT NULL,
  user_id bigint,
  org_id bigint,
  body json,
  "Title" varchar(100),
  created_at timestamp,
  CONSTRAINT posts_pkey PRIMARY KEY (id),
  CONSTRAINT posts_user_fkey FOREIGN KEY (user_id) REFERENCES public.users (id),
  CONSTRAINT posts_org_fkey FOREIGN KEY (org_id) REFERENCES public.users (id)
);
CREATE INDEX posts_created_idx ON public.posts (created_at);
CREATE INDEX posts_created_dup_idx ON public.posts (created_at);
CREATE INDEX posts_created_part_idx ON public.posts (created_at) WHERE id > 0;
CREATE INDEX posts_org_idx ON public.posts (org_id, created_at);
CREATE TABLE public."Audit" (msg text);`), []string{"public"})
	require.NoError(t, err)

	var got []string
	for _, v := range violations {
		got = append(got, v.String())
	}
	assert.Equal(t, []string{
		"column public.posts.id: prefer-identity: use an identity column",
		"column public.posts.body: prefer-jsonb: use jsonb",
		`column public.posts."Title": snake-case-column: the column name is not snake_case`,
		`column public.posts."Title": prefer-text: use text, with a check constraint if the length matters`,
		"column public.posts.created_at: prefer-timestamptz: use timestamp with time zone",
		"index public.posts_created_idx: duplicate-index: another index has the same definition",
		"index public.posts_created_dup_idx: duplicate-index: another index has the same definition",
		"foreign key posts_user_fkey on public.posts: fk-needs-index: no index starts with the columns of the foreign key",
		`table public."Audit": require-primary-key: the table has no primary key`,
		`table public."Audit": snake-case-table: the table name is not snake_case`,
	}, got)
}

func TestRun_Kinds(t *testing.T) {
	got := run(t, `
rules:
  - name: t
    on: table
    assert: table.name != "t"
    message: table
  - name: c
    on: column
    assert: column.name != "a"
    message: column
  - name: i
    on: index
    assert: index.method != "btree"
    message: index
  - name: f
    on: foreign_key
    assert: fk.on_delete != "cascade"
    message: fk
`, `
CREATE TABLE public.u (id int NOT NULL, CONSTRAINT u_pkey PRIMARY KEY (id));
CREATE TABLE public.t (a int, b int, CONSTRAINT t_a_fkey FOREIGN KEY (a) REFERENCES public.u (id) ON DELETE CASCADE);
CREATE INDEX t_b_idx ON public.t (b);
CREATE MATERIALIZED VIEW public.mv AS SELECT a FROM public.t;
CREATE INDEX mv_a_idx ON public.mv (a);
CREATE VIEW public.v AS SELECT a FROM public.t;`)

	assert.Equal(t, []string{
		"table public.t: t: table",
		"column public.t.a: c: column",
		"index public.t_b_idx: i: index",
		"foreign key t_a_fkey on public.t: f: fk",
		"index public.mv_a_idx: i: index",
	}, got)
}

// An index on a materialized view is checked with the view as table.
func TestRun_MaterializedViewIndexReadsTheView(t *testing.T) {
	got := run(t, `
rules:
  - name: i
    on: index
    assert: table.name != "mv"
    message: on mv
`, `
CREATE TABLE public.t (a int);
CREATE MATERIALIZED VIEW public.mv AS SELECT a FROM public.t;
CREATE INDEX mv_a_idx ON public.mv (a);`)

	assert.Equal(t, []string{"index public.mv_a_idx: i: on mv"}, got)
}

func TestRun_LintIgnore(t *testing.T) {
	rules := `
rules:
  - name: no-t
    on: table
    assert: "false"
    message: table
  - name: no-c
    on: column
    assert: "false"
    message: column
  - name: no-i
    on: index
    assert: "false"
    message: index
  - name: no-f
    on: foreign_key
    assert: "false"
    message: fk
`
	got := run(t, rules, `
CREATE TABLE public.u (id int NOT NULL, CONSTRAINT u_pkey PRIMARY KEY (id));
-- pista:lint-ignore no-t
CREATE TABLE public.t (
  -- pista:lint-ignore no-c
  a int,
  -- pista:lint-ignore no-f
  CONSTRAINT t_a_fkey FOREIGN KEY (a) REFERENCES public.u (id)
);
-- pista:lint-ignore no-i
CREATE INDEX t_a_idx ON public.t (a);`)

	// Only the objects on u are left, and the directive on t does not reach
	// the objects on it.
	assert.Equal(t, []string{
		"table public.u: no-t: table",
		"column public.u.id: no-c: column",
	}, got)
}

// A table marked -- pista:ignore is not managed, so nothing on it is checked.
func TestRun_PistaIgnoreSkipsTheTable(t *testing.T) {
	got := run(t, `
rules:
  - name: no-t
    on: table
    assert: "false"
    message: table
  - name: no-i
    on: index
    assert: "false"
    message: index
`, `
-- pista:ignore
CREATE TABLE public.t (a int);
CREATE INDEX t_a_idx ON public.t (a);
CREATE TABLE public.u (a int);
-- pista:ignore
CREATE MATERIALIZED VIEW public.mv AS SELECT a FROM public.u;
CREATE INDEX mv_a_idx ON public.mv (a);`)

	assert.Equal(t, []string{"table public.u: no-t: table"}, got)
}

// doc is the whole document, so a rule can read another table.
func TestRun_Doc(t *testing.T) {
	got := run(t, `
rules:
  - name: fk-type
    on: foreign_key
    assert: >-
      table.columns.filter(c, c.name == fk.columns[0])[0].base_type ==
      doc.tables[fk.ref_schema + "." + fk.ref_table].columns.filter(c, c.name == fk.ref_columns[0])[0].base_type
    message: the column types differ
`, `
CREATE TABLE public.u (id bigint NOT NULL, CONSTRAINT u_pkey PRIMARY KEY (id));
CREATE TABLE public.t (a integer, b bigint,
  CONSTRAINT t_a_fkey FOREIGN KEY (a) REFERENCES public.u (id),
  CONSTRAINT t_b_fkey FOREIGN KEY (b) REFERENCES public.u (id));`)

	assert.Equal(t, []string{"foreign key t_a_fkey on public.t: fk-type: the column types differ"}, got)
}

func TestRun_Functions(t *testing.T) {
	got := run(t, `
rules:
  - name: values
    on: table
    assert: table.constraints.values().size() == 1
    message: values
  - name: prefix
    on: table
    assert: >-
      hasPrefix(["a", "b"], ["a"]) && hasPrefix(["a"], []) && !hasPrefix(["a"], ["a", "b"])
      && !hasPrefix(["a", "b"], ["b"])
    message: prefix
`, `CREATE TABLE public.t (id int NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id));`)

	assert.Empty(t, got)
}

func TestRun_Debug(t *testing.T) {
	path := writeFile(t, t.TempDir(), "rules.yml", `
rules:
  - name: dbg
    on: column
    assert: debug("col", column.name) != "" && debug("list", [1, "a"]) != null && debug("type", int) != null
    message: m
`)
	var out bytes.Buffer
	linter, err := lint.Load([]string{path}, &out)
	require.NoError(t, err)

	_, err = linter.Run(parse(t, `CREATE TABLE public.t (a int);`), []string{"public"})
	require.NoError(t, err)
	assert.Equal(t, `debug: column public.t.a: dbg: col = "a"
debug: column public.t.a: dbg: list = [1,"a"]
debug: column public.t.a: dbg: type = int
`, out.String())
}

func TestRun_Errors(t *testing.T) {
	for name, tc := range map[string]struct {
		assert string
		want   string
	}{
		"a missing key":     {`column.nope == 1`, "column public.t.a: bad: no such key: nope"},
		"a result not bool": {`column.name`, "column public.t.a: bad: assert returned string, not bool"},
	} {
		t.Run(name, func(t *testing.T) {
			path := writeFile(t, t.TempDir(), "rules.yml", "rules:\n  - name: bad\n    on: column\n    assert: '"+tc.assert+"'\n    message: m\n")
			linter, err := lint.Load([]string{path}, &bytes.Buffer{})
			require.NoError(t, err)

			_, err = linter.Run(parse(t, `CREATE TABLE public.t (a int);`), []string{"public"})
			require.EqualError(t, err, tc.want)
		})
	}
}

func TestLoad_Directory(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "b.yaml", "rules:\n  - {name: b, on: table, assert: 'true', message: m}\n")
	writeFile(t, dir, "a.yml", "rules:\n  - {name: a, on: table, assert: 'true', message: m}\n")
	writeFile(t, dir, "notes.txt", "not a rule file")
	writeFile(t, dir, "sub/c.yml", "rules:\n  - {name: c, on: table, assert: 'true', message: m}\n")
	single := writeFile(t, t.TempDir(), "d.yml", "rules:\n  - {name: d, on: table, assert: 'true', message: m}\n")

	linter, err := lint.Load([]string{dir, single}, &bytes.Buffer{})
	require.NoError(t, err)

	var names []string
	for _, r := range linter.Rules {
		names = append(names, r.Name)
	}
	assert.Equal(t, []string{"a", "b", "d"}, names, "name order, no subdirectory, then the file")
}

func TestLoad_Errors(t *testing.T) {
	for name, tc := range map[string]struct {
		content string
		want    string
	}{
		"no name":           {"rules:\n  - {on: table, assert: 'true', message: m}\n", "a rule has no name"},
		"a bad kind":        {"rules:\n  - {name: r, on: view, assert: 'true', message: m}\n", `rule r: on must be one of table, column, index or foreign_key, not "view"`},
		"no assert":         {"rules:\n  - {name: r, on: table, message: m}\n", "rule r: assert is empty"},
		"no message":        {"rules:\n  - {name: r, on: table, assert: 'true'}\n", "rule r: message is empty"},
		"an unknown key":    {"rules:\n  - {name: r, on: table, assert: 'true', message: m, level: error}\n", "field level not found"},
		"a syntax error":    {"rules:\n  - {name: r, on: table, assert: 'table.', message: m}\n", "rule r: ERROR"},
		"a wrong variable":  {"rules:\n  - {name: r, on: table, assert: 'column.name == \"a\"', message: m}\n", "undeclared reference to 'column'"},
		"a non-bool assert": {"rules:\n  - {name: r, on: table, assert: '1 + 1', message: m}\n", "rule r: assert is int, not bool"},
		"no rules":          {"", "no rules found in"},
		"bad yaml":          {"rules: [", "did not find expected"},
	} {
		t.Run(name, func(t *testing.T) {
			path := writeFile(t, t.TempDir(), "rules.yml", tc.content)
			_, err := lint.Load([]string{path}, &bytes.Buffer{})
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestLoad_DuplicateName(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.yml", "rules:\n  - {name: r, on: table, assert: 'true', message: m}\n")
	writeFile(t, dir, "b.yml", "rules:\n  - {name: r, on: column, assert: 'true', message: m}\n")

	_, err := lint.Load([]string{dir}, &bytes.Buffer{})
	require.ErrorContains(t, err, "b.yml: rule r is also defined in")
}

func TestLoad_MissingPath(t *testing.T) {
	_, err := lint.Load([]string{filepath.Join(t.TempDir(), "nope.yml")}, &bytes.Buffer{})
	require.ErrorIs(t, err, os.ErrNotExist)
}
