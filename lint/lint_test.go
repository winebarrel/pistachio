package lint_test

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
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
// each of them, and that holds the cases each must leave alone.
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
  price money,
  opens time with time zone,
  editor_id bigint,
  CONSTRAINT posts_pkey PRIMARY KEY (id),
  CONSTRAINT posts_user_fkey FOREIGN KEY (user_id) REFERENCES public.users (id),
  CONSTRAINT posts_org_fkey FOREIGN KEY (org_id) REFERENCES public.users (id),
  CONSTRAINT posts_editor_fkey FOREIGN KEY (editor_id) REFERENCES public.users (id)
);
CREATE INDEX posts_created_idx ON public.posts (created_at);
CREATE INDEX posts_created_dup_idx ON public.posts (created_at);
CREATE INDEX posts_created_part_idx ON public.posts (created_at) WHERE id > 0;
CREATE INDEX posts_org_idx ON public.posts (org_id);
CREATE INDEX posts_org_created_idx ON public.posts (org_id, created_at);
CREATE UNIQUE INDEX posts_user_key ON public.posts (user_id);
CREATE UNIQUE INDEX posts_user_org_key ON public.posts (user_id, org_id);
CREATE INDEX posts_lower_title_idx ON public.posts (lower("Title"));
CREATE INDEX posts_lower_body_idx ON public.posts (lower(body::text));
CREATE TABLE public.tags (id integer NOT NULL, CONSTRAINT tags_pkey PRIMARY KEY (id));
CREATE TABLE public."Audit" (msg text);
CREATE TABLE public.events (id bigint NOT NULL, at timestamptz NOT NULL, CONSTRAINT events_pkey PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
CREATE TABLE public.events_2026 PARTITION OF public.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE public.archived_users () INHERITS (public.users);`), []string{"public"})
	require.NoError(t, err)

	var got []string
	for _, v := range violations {
		got = append(got, v.String())
	}
	assert.Equal(t, []string{
		"schema.sql:4:3: column public.posts.id: prefer-identity: use an identity column",
		"schema.sql:7:3: column public.posts.body: prefer-jsonb: use jsonb",
		`schema.sql:8:3: column public.posts."Title": snake-case-column: the column name is not snake_case`,
		`schema.sql:8:3: column public.posts."Title": prefer-text: use text, with a check constraint if the length matters`,
		"schema.sql:9:3: column public.posts.created_at: prefer-timestamptz: use timestamp with time zone",
		"schema.sql:10:3: column public.posts.price: no-money: use numeric",
		"schema.sql:11:3: column public.posts.opens: no-timetz: use timestamp with time zone",
		"schema.sql:18:1: index public.posts_created_idx: duplicate-index: another index has the same definition",
		"schema.sql:19:1: index public.posts_created_dup_idx: duplicate-index: another index has the same definition",
		"schema.sql:21:1: index public.posts_org_idx: redundant-index: another index starts with the same columns",
		"schema.sql:16:3: foreign key posts_editor_fkey on public.posts: fk-needs-index: no index starts with the columns of the foreign key",
		"schema.sql:27:1: table public.tags: prefer-bigint-key: use bigint for the primary key",
		`schema.sql:28:1: table public."Audit": require-primary-key: the table has no primary key`,
		`schema.sql:28:1: table public."Audit": snake-case-table: the table name is not snake_case`,
		// A partition takes its parent's primary key, and an INHERITS child
		// does not.
		"schema.sql:31:1: table public.archived_users: require-primary-key: the table has no primary key",
	}, got)
}

// The index rules compare the definition from USING on, so an operator class,
// a collation, a sort order or a WHERE clause makes two indexes different.
func TestStandardRules_Indexes(t *testing.T) {
	linter, err := lint.Load([]string{filepath.Join("..", "rules", "indexes.yml")}, &bytes.Buffer{})
	require.NoError(t, err)

	violations, err := linter.Run(parse(t, `
CREATE TABLE public.t (id bigint NOT NULL, a text, b text, CONSTRAINT t_pkey PRIMARY KEY (id));
CREATE INDEX t_a_idx ON public.t (a);
CREATE INDEX t_a_ops_idx ON public.t (a text_pattern_ops);
CREATE INDEX t_a_c_idx ON public.t (a COLLATE "C");
CREATE INDEX t_a_desc_idx ON public.t (a DESC);
CREATE INDEX t_a_dup_idx ON public.t (a);
CREATE INDEX t_ab_idx ON public.t (a, b);
CREATE INDEX t_ops_b_idx ON public.t (a text_pattern_ops, b);
CREATE INDEX t_lower_idx ON public.t (lower(a));
CREATE INDEX t_lower_b_idx ON public.t (lower(a), b);
CREATE INDEX t_part_idx ON public.t (a) WHERE id > 0;
CREATE INDEX t_part2_idx ON public.t (a) WHERE id > 0;
CREATE INDEX t_part3_idx ON public.t (a) WHERE id > 1;
CREATE INDEX t_hash_idx ON public.t USING hash (a);
CREATE INDEX t_incl_idx ON public.t (b) INCLUDE (a);
CREATE INDEX t_ba_idx ON public.t (b, a);`), []string{"public"})
	require.NoError(t, err)

	var got []string
	for _, v := range violations {
		got = append(got, v.String())
	}
	assert.Equal(t, []string{
		"schema.sql:3:1: index public.t_a_idx: duplicate-index: another index has the same definition",
		"schema.sql:3:1: index public.t_a_idx: redundant-index: another index starts with the same columns",
		"schema.sql:4:1: index public.t_a_ops_idx: redundant-index: another index starts with the same columns",
		"schema.sql:7:1: index public.t_a_dup_idx: duplicate-index: another index has the same definition",
		"schema.sql:7:1: index public.t_a_dup_idx: redundant-index: another index starts with the same columns",
		"schema.sql:10:1: index public.t_lower_idx: redundant-index: another index starts with the same columns",
		"schema.sql:12:1: index public.t_part_idx: duplicate-index: another index has the same definition",
		"schema.sql:13:1: index public.t_part2_idx: duplicate-index: another index has the same definition",
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
		"schema.sql:3:1: table public.t: t: table",
		"schema.sql:3:24: column public.t.a: c: column",
		"schema.sql:4:1: index public.t_b_idx: i: index",
		"schema.sql:3:38: foreign key t_a_fkey on public.t: f: fk",
		"schema.sql:6:1: index public.mv_a_idx: i: index",
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

	assert.Equal(t, []string{"schema.sql:4:1: index public.mv_a_idx: i: on mv"}, got)
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
		"schema.sql:2:1: table public.u: no-t: table",
		"schema.sql:2:24: column public.u.id: no-c: column",
	}, got)
}

// A directive with no rule name turns off every rule for the object, and is
// not reported as an unknown name.
func TestRun_LintIgnoreAll(t *testing.T) {
	path := writeFile(t, t.TempDir(), "rules.yml", `
rules:
  - {name: no-t, on: table, assert: "false", message: table}
  - {name: no-t2, on: table, assert: "false", message: table 2}
`)
	var stderr bytes.Buffer
	linter, err := lint.Load([]string{path}, &stderr)
	require.NoError(t, err)

	violations, err := linter.Run(parse(t, `
-- pista:lint-ignore -- a scratch table
CREATE TABLE public.t (a int);
CREATE TABLE public.u (a int);`), []string{"public"})
	require.NoError(t, err)

	var got []string
	for _, v := range violations {
		got = append(got, v.String())
	}
	assert.Equal(t, []string{
		"schema.sql:4:1: table public.u: no-t: table",
		"schema.sql:4:1: table public.u: no-t2: table 2",
	}, got)
	assert.Empty(t, stderr.String())
}

// A name in -- pista:lint-ignore that no rule of the object's kind has is
// reported on stderr, and does not change the result.
func TestRun_LintIgnoreUnknownName(t *testing.T) {
	path := writeFile(t, t.TempDir(), "rules.yml", `
rules:
  - {name: no-t, on: table, assert: "false", message: table}
  - {name: no-c, on: column, assert: "true", message: column}
`)
	var stderr bytes.Buffer
	linter, err := lint.Load([]string{path}, &stderr)
	require.NoError(t, err)

	violations, err := linter.Run(parse(t, `
-- pista:lint-ignore no-t, typo, no-c
CREATE TABLE public.t (a int);`), []string{"public"})
	require.NoError(t, err)

	assert.Empty(t, violations)
	assert.Equal(t, `warning: schema.sql:3:1: table public.t: -- pista:lint-ignore names no table rule typo
warning: schema.sql:3:1: table public.t: -- pista:lint-ignore names no table rule no-c
`, stderr.String())
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

	assert.Equal(t, []string{"schema.sql:5:1: table public.u: no-t: table"}, got)
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

	assert.Equal(t, []string{"schema.sql:4:3: foreign key t_a_fkey on public.t: fk-type: the column types differ"}, got)
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
  - name: strings
    on: table
    assert: '"a USING b".indexOf(" USING ") == 1 && "abc".substring(1) == "bc"'
    message: strings
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
		"a missing key":     {`column.nope == 1`, "schema.sql:1:24: column public.t.a: bad: no such key: nope"},
		"a result not bool": {`column.name`, "schema.sql:1:24: column public.t.a: bad: assert returned string, not bool"},
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
	writeFile(t, dir, "c.YML", "rules:\n  - {name: c, on: table, assert: 'true', message: m}\n")
	writeFile(t, dir, "notes.txt", "not a rule file")
	writeFile(t, dir, "sub/s.yml", "rules:\n  - {name: s, on: table, assert: 'true', message: m}\n")
	writeFile(t, dir, "dir.yml/x.yml", "rules:\n  - {name: x, on: table, assert: 'true', message: m}\n")
	shared := writeFile(t, t.TempDir(), "shared.yml", "rules:\n  - {name: l, on: table, assert: 'true', message: m}\n")
	require.NoError(t, os.Symlink(shared, filepath.Join(dir, "link.yml")))
	single := writeFile(t, t.TempDir(), "d.yml", "rules:\n  - {name: d, on: table, assert: 'true', message: m}\n")

	linter, err := lint.Load([]string{dir, single}, &bytes.Buffer{})
	require.NoError(t, err)

	var names []string
	for _, r := range linter.Rules {
		names = append(names, r.Name)
	}
	// Name order, with upper-case extensions and links to files, but no
	// subdirectory; then the file named on its own.
	assert.Equal(t, []string{"a", "b", "c", "l", "d"}, names)
}

func TestLoad_BrokenLink(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Symlink(filepath.Join(dir, "nope.yml"), filepath.Join(dir, "link.yml")))

	_, err := lint.Load([]string{dir}, &bytes.Buffer{})
	require.ErrorIs(t, err, os.ErrNotExist)
}

// A parse result with no positions, such as one built in memory, prints the
// object alone.
func TestRun_NoPosition(t *testing.T) {
	path := writeFile(t, t.TempDir(), "rules.yml", "rules:\n  - {name: r, on: table, assert: 'false', message: m}\n")
	linter, err := lint.Load([]string{path}, &bytes.Buffer{})
	require.NoError(t, err)

	result := parse(t, `CREATE TABLE public.t (a int);`)
	result.Positions = nil
	violations, err := linter.Run(result, []string{"public"})
	require.NoError(t, err)
	require.Len(t, violations, 1)
	assert.Equal(t, "table public.t: r: m", violations[0].String())
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
		"a comma in a name": {"rules:\n  - {name: 'a,b', on: table, assert: 'true', message: m}\n", `rule name "a,b" has a comma or a space`},
		"a space in a name": {"rules:\n  - {name: 'a b', on: table, assert: 'true', message: m}\n", `rule name "a b" has a comma or a space`},
		"a misspelled key":  {"rule:\n  - {name: r, on: table, assert: 'true', message: m}\n", "field rule not found"},
		"no rules":          {"", "no rules found in"},
		"bad yaml":          {"rules: [", "did not find expected"},
		"two documents":     {"rules:\n  - {name: a, on: table, assert: 'true', message: m}\n---\nrules:\n  - {name: b, on: table, assert: 'true', message: m}\n", "a rule file holds one YAML document"},
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

func TestLoad_DuplicateNameInOneFile(t *testing.T) {
	path := writeFile(t, t.TempDir(), "rules.yml", "rules:\n  - {name: r, on: table, assert: 'true', message: m}\n  - {name: r, on: column, assert: 'true', message: m}\n")

	_, err := lint.Load([]string{path}, &bytes.Buffer{})
	require.ErrorContains(t, err, "rules.yml: rule r is also defined in")
}

// A rule file or a directory that cannot be read is an error. Root reads
// anything, and Windows does not apply these modes, so neither can run it.
func TestLoad_Unreadable(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("permissions do not stop this user")
	}

	file := writeFile(t, t.TempDir(), "rules.yml", "rules:\n  - {name: r, on: table, assert: 'true', message: m}\n")
	require.NoError(t, os.Chmod(file, 0o000))
	_, err := lint.Load([]string{file}, &bytes.Buffer{})
	require.ErrorIs(t, err, os.ErrPermission)

	dir := t.TempDir()
	writeFile(t, dir, "rules.yml", "rules:\n  - {name: r, on: table, assert: 'true', message: m}\n")
	require.NoError(t, os.Chmod(dir, 0o000))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	_, err = lint.Load([]string{dir}, &bytes.Buffer{})
	require.ErrorIs(t, err, os.ErrPermission)
}

func TestLoad_MissingPath(t *testing.T) {
	_, err := lint.Load([]string{filepath.Join(t.TempDir(), "nope.yml")}, &bytes.Buffer{})
	require.ErrorIs(t, err, os.ErrNotExist)
}
