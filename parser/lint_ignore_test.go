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
