package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/parser"
)

func parseLintIgnores(t *testing.T, sql string) map[parser.LintTarget][]string {
	t.Helper()

	result, err := parser.ParseSQLSourcesWithSchema([]parser.Source{{Name: "schema.sql", SQL: sql}}, "public")
	require.NoError(t, err)
	return result.LintIgnores
}

func TestLintIgnore_Table(t *testing.T) {
	got := parseLintIgnores(t, `
-- pista:lint-ignore require-primary-key
CREATE TABLE public.logs (at timestamp);`)

	assert.Equal(t, map[parser.LintTarget][]string{
		{Kind: parser.LintTable, Table: "public.logs"}: {"require-primary-key"},
	}, got)
}

// Names are separated by commas or spaces, and the lines before one statement
// add up.
func TestLintIgnore_SeveralNames(t *testing.T) {
	got := parseLintIgnores(t, `
-- pista:lint-ignore a, b  c
-- pista:lint-ignore d
CREATE TABLE public.t (id int);`)

	assert.Equal(t, []string{"a", "b", "c", "d"}, got[parser.LintTarget{Kind: parser.LintTable, Table: "public.t"}])
}

func TestLintIgnore_ColumnAndForeignKeyInCreateTable(t *testing.T) {
	got := parseLintIgnores(t, `
CREATE TABLE public.users (id bigint NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE TABLE public.posts (
  id bigint NOT NULL,
  -- pista:lint-ignore prefer-timestamptz
  at timestamp,
  -- pista:lint-ignore quoted-rule
  "Odd Name" text,
  user_id bigint,
  CONSTRAINT posts_pkey PRIMARY KEY (id),
  -- pista:lint-ignore fk-needs-index
  CONSTRAINT posts_user_fkey FOREIGN KEY (user_id) REFERENCES public.users (id)
);`)

	assert.Equal(t, map[parser.LintTarget][]string{
		{Kind: parser.LintColumn, Table: "public.posts", Name: "at"}:                  {"prefer-timestamptz"},
		{Kind: parser.LintColumn, Table: "public.posts", Name: "Odd Name"}:            {"quoted-rule"},
		{Kind: parser.LintForeignKey, Table: "public.posts", Name: "posts_user_fkey"}: {"fk-needs-index"},
	}, got)
}

func TestLintIgnore_IndexAndAlterTable(t *testing.T) {
	got := parseLintIgnores(t, `
CREATE TABLE public.users (id bigint NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE TABLE public.posts (id bigint NOT NULL, user_id bigint);
-- pista:lint-ignore duplicate-index
CREATE INDEX posts_user_idx ON public.posts (user_id);
-- pista:lint-ignore fk-needs-index
ALTER TABLE public.posts ADD CONSTRAINT posts_user_fkey FOREIGN KEY (user_id) REFERENCES public.users (id);
CREATE MATERIALIZED VIEW public.mv AS SELECT id FROM public.posts;
-- pista:lint-ignore mv-rule
CREATE INDEX mv_id_idx ON public.mv (id);`)

	assert.Equal(t, map[parser.LintTarget][]string{
		{Kind: parser.LintIndex, Table: "public.posts", Name: "posts_user_idx"}:       {"duplicate-index"},
		{Kind: parser.LintForeignKey, Table: "public.posts", Name: "posts_user_fkey"}: {"fk-needs-index"},
		{Kind: parser.LintIndex, Table: "public.mv", Name: "mv_id_idx"}:               {"mv-rule"},
	}, got)
}

// The directive is not part of the model, so it reaches neither the diff nor
// the state hash that a plan file records.
func TestLintIgnore_NotInModel(t *testing.T) {
	result, err := parser.ParseSQLSourcesWithSchema([]parser.Source{{Name: "a.sql", SQL: `
-- pista:lint-ignore require-primary-key
CREATE TABLE public.t (id int);`}}, "public")
	require.NoError(t, err)

	plain, err := parser.ParseSQLSourcesWithSchema([]parser.Source{{Name: "a.sql", SQL: `CREATE TABLE public.t (id int);`}}, "public")
	require.NoError(t, err)

	assert.Equal(t, plain.Tables.Get("public.t"), result.Tables.Get("public.t"))
}

func TestLintIgnore_RequiresAName(t *testing.T) {
	_, err := parser.ParseSQLSourcesWithSchema([]parser.Source{{Name: "a.sql", SQL: `
-- pista:lint-ignore
CREATE TABLE public.t (id int);`}}, "public")
	require.ErrorContains(t, err, "-- pista:lint-ignore requires a rule name")
}

// Several lines before one column add up, a line before a check constraint
// has no target, and an unquoted name is folded as PostgreSQL folds it.
func TestLintIgnore_InlineDetails(t *testing.T) {
	got := parseLintIgnores(t, `
CREATE TABLE public.t (
  -- pista:lint-ignore a
  -- pista:lint-ignore b
  CreatedAt timestamp,
  -- pista:lint-ignore c
  CONSTRAINT t_check CHECK (true)
);`)

	assert.Equal(t, map[parser.LintTarget][]string{
		{Kind: parser.LintColumn, Table: "public.t", Name: "createdat"}: {"a", "b"},
	}, got)
}

// One ALTER TABLE that adds two foreign keys gives the rules to both.
func TestLintIgnore_AlterTableTwoForeignKeys(t *testing.T) {
	got := parseLintIgnores(t, `
CREATE TABLE public.u (id int NOT NULL, CONSTRAINT u_pkey PRIMARY KEY (id));
CREATE TABLE public.t (a int, b int);
-- pista:lint-ignore fk-needs-index
ALTER TABLE public.t
  ADD CONSTRAINT t_a_fkey FOREIGN KEY (a) REFERENCES public.u (id),
  ADD CONSTRAINT t_b_fkey FOREIGN KEY (b) REFERENCES public.u (id);`)

	assert.Equal(t, map[parser.LintTarget][]string{
		{Kind: parser.LintForeignKey, Table: "public.t", Name: "t_a_fkey"}: {"fk-needs-index"},
		{Kind: parser.LintForeignKey, Table: "public.t", Name: "t_b_fkey"}: {"fk-needs-index"},
	}, got)
}

// Before a statement it does not apply to, the directive does nothing.
func TestLintIgnore_OtherStatement(t *testing.T) {
	got := parseLintIgnores(t, `
CREATE TABLE public.t (a int);
-- pista:lint-ignore some-rule
CREATE VIEW public.v AS SELECT a FROM public.t;`)

	assert.Empty(t, got)
}

func TestPositions(t *testing.T) {
	result, err := parser.ParseSQLSourcesWithSchema([]parser.Source{
		{Name: "users.sql", SQL: `CREATE TABLE public.users (id bigint NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));`},
		{Name: "posts.sql", SQL: `
CREATE TABLE public.posts (
    id bigint NOT NULL,
    user_id bigint REFERENCES public.users (id),
    org_id bigint,
    editor_id bigint,
    CONSTRAINT posts_org_fkey FOREIGN KEY (org_id) REFERENCES public.users (id),
    FOREIGN KEY (editor_id) REFERENCES public.users (id)
);
CREATE INDEX posts_org_idx ON public.posts (org_id);
CREATE TABLE public.likes (post_id bigint);
ALTER TABLE public.likes ADD CONSTRAINT likes_post_fkey FOREIGN KEY (post_id) REFERENCES public.posts (id);
CREATE MATERIALIZED VIEW public.mv AS SELECT id FROM public.posts;
CREATE INDEX mv_id_idx ON public.mv (id);`},
	}, "public")
	require.NoError(t, err)

	pos := func(kind parser.LintKind, table, name string) string {
		p, ok := result.Positions[parser.LintTarget{Kind: kind, Table: table, Name: name}]
		require.True(t, ok, "%s %s %s", kind, table, name)
		return p.String()
	}

	assert.Equal(t, "users.sql:1:1", pos(parser.LintTable, "public.users", ""))
	assert.Equal(t, "users.sql:1:28", pos(parser.LintColumn, "public.users", "id"))
	assert.Equal(t, "posts.sql:2:1", pos(parser.LintTable, "public.posts", ""))
	assert.Equal(t, "posts.sql:4:5", pos(parser.LintColumn, "public.posts", "user_id"))
	// A key written on a column is placed at the column, and a table
	// constraint at its first keyword, with or without a name.
	assert.Equal(t, "posts.sql:4:5", pos(parser.LintForeignKey, "public.posts", "posts_user_id_fkey"))
	assert.Equal(t, "posts.sql:7:5", pos(parser.LintForeignKey, "public.posts", "posts_org_fkey"))
	assert.Equal(t, "posts.sql:8:5", pos(parser.LintForeignKey, "public.posts", "posts_editor_id_fkey"))
	assert.Equal(t, "posts.sql:10:1", pos(parser.LintIndex, "public.posts", "posts_org_idx"))
	assert.Equal(t, "posts.sql:12:1", pos(parser.LintForeignKey, "public.likes", "likes_post_fkey"))
	assert.Equal(t, "posts.sql:14:1", pos(parser.LintIndex, "public.mv", "mv_id_idx"))
}

// SQL that came from no file has no positions.
func TestPositions_NoFile(t *testing.T) {
	result, err := parser.ParseSQLFilesWithSchema(nil, "public")
	require.NoError(t, err)
	assert.Empty(t, result.Positions)
}

// A rename and a lint-ignore before the same column or key both apply.
func TestLintIgnore_WithRenamedFrom(t *testing.T) {
	result, err := parser.ParseSQLSourcesWithSchema([]parser.Source{{Name: "a.sql", SQL: `
CREATE TABLE public.u (id int NOT NULL, CONSTRAINT u_pkey PRIMARY KEY (id));
CREATE TABLE public.t (
  -- pista:renamed-from old_at
  -- pista:lint-ignore prefer-timestamptz
  at timestamp,
  u_id int,
  -- pista:lint-ignore fk-needs-index
  -- pista:renamed-from t_old_fkey
  CONSTRAINT t_u_fkey FOREIGN KEY (u_id) REFERENCES public.u (id)
);`}}, "public")
	require.NoError(t, err)

	table := result.Tables.Get("public.t")
	assert.Equal(t, "old_at", *table.Columns.Get("at").RenameFrom)
	assert.Equal(t, "t_old_fkey", *table.ForeignKeys.Get("t_u_fkey").RenameFrom)
	assert.Equal(t, map[parser.LintTarget][]string{
		{Kind: parser.LintColumn, Table: "public.t", Name: "at"}:           {"prefer-timestamptz"},
		{Kind: parser.LintForeignKey, Table: "public.t", Name: "t_u_fkey"}: {"fk-needs-index"},
	}, result.LintIgnores)
}

// The column counts characters, so text that is not ASCII before an object
// does not shift it.
func TestPositions_NonASCII(t *testing.T) {
	result, err := parser.ParseSQLSourcesWithSchema([]parser.Source{
		{Name: "a.sql", SQL: "-- あい\nCREATE TABLE public.t (/* あ */ at timestamp);"},
	}, "public")
	require.NoError(t, err)

	pos := result.Positions[parser.LintTarget{Kind: parser.LintColumn, Table: "public.t", Name: "at"}]
	assert.Equal(t, "a.sql:2:32", pos.String())
}
