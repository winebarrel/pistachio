package pistachio_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/model"
)

func TestExtractObjectName(t *testing.T) {
	tests := []struct {
		sql      string
		expected string
	}{
		{"CREATE TABLE public.users (\n  id integer\n);", "public.users"},
		{"CREATE UNLOGGED TABLE public.logs (id integer);", "public.logs"},
		{"ALTER TABLE public.users ADD COLUMN name text;", "public.users"},
		{"ALTER TABLE ONLY public.users ADD CONSTRAINT pk PRIMARY KEY (id);", "public.users"},
		{"DROP TABLE public.users;", "public.users"},
		{"CREATE TYPE public.status AS ENUM ('a', 'b');", "public.status"},
		{"ALTER TYPE public.status ADD VALUE 'c';", "public.status"},
		{"DROP TYPE public.status;", "public.status"},
		{"CREATE DOMAIN public.pos_int AS integer;", "public.pos_int"},
		{"ALTER DOMAIN public.pos_int SET NOT NULL;", "public.pos_int"},
		{"DROP DOMAIN public.pos_int;", "public.pos_int"},
		{"CREATE OR REPLACE VIEW public.v AS SELECT 1;", "public.v"},
		{"DROP VIEW public.v;", "public.v"},
		{"CREATE INDEX idx_users_name ON public.users USING btree (name);", "public.users"},
		{"COMMENT ON TABLE public.users IS 'Users table';", "public.users"},
		{"COMMENT ON COLUMN public.users.name IS 'Name';", "public.users"},
		{`CREATE TABLE "MySchema"."MyTable" (id integer);`, `"MySchema"."MyTable"`},
		// Edge cases
		{"SELECT 1;", ""},
		{"CREATE INDEX idx ON ONLY public.t (x);", "public.t"},
		{"CREATE INDEX bad_no_on;", ""},
		// DROP INDEX / ALTER INDEX
		{`DROP INDEX public.idx_users_name;`, ""},
		{`ALTER INDEX public.idx_old RENAME TO idx_new;`, ""},
		{`COMMENT ON COLUMN "S"."T".col IS 'x';`, `"S"."T"`},
		{`CREATE TABLE public."escaped""quote" (id integer);`, `public."escaped""quote"`},
	}

	for _, tt := range tests {
		name := tt.sql
		if len(name) > 40 {
			name = name[:40]
		}
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, pistachio.ExtractObjectName(tt.sql))
		})
	}
}

func TestOrderStatements_Fallback(t *testing.T) {
	// Test that fallbackOrder is used when topological sort would fail
	// (cyclic FK dependencies between desired tables)
	desiredEnums := orderedmap.New[string, *model.Enum]()
	desiredDomains := orderedmap.New[string, *model.Domain]()
	desiredViews := orderedmap.New[string, *model.View]()

	// Create two tables with mutual FK references → cycle
	refA := "a"
	refB := "b"
	schemaPublic := "public"
	desiredTables := orderedmap.New[string, *model.Table]()
	tblA := &model.Table{Schema: "public", Name: "a"}
	tblA.Columns = orderedmap.New[string, *model.Column]()
	tblA.Indexes = orderedmap.New[string, *model.Index]()
	tblA.Constraints = orderedmap.New[string, *model.Constraint]()
	tblA.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tblA.ForeignKeys.Set("a_b_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "a_b_fk"},
		RefSchema:  &schemaPublic,
		RefTable:   &refB,
	})
	desiredTables.Set("public.a", tblA)

	tblB := &model.Table{Schema: "public", Name: "b"}
	tblB.Columns = orderedmap.New[string, *model.Column]()
	tblB.Indexes = orderedmap.New[string, *model.Index]()
	tblB.Constraints = orderedmap.New[string, *model.Constraint]()
	tblB.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	tblB.ForeignKeys.Set("b_a_fk", &model.ForeignKey{
		Constraint: model.Constraint{Name: "b_a_fk"},
		RefSchema:  &schemaPublic,
		RefTable:   &refA,
	})
	desiredTables.Set("public.b", tblB)

	enumDiff := &diff.EnumDiffResult{Stmts: []string{"CREATE TYPE public.s AS ENUM ('x');"}}
	domainDiff := &diff.DomainDiffResult{}
	tableDiff := &diff.TableDiffResult{Stmts: []string{"CREATE TABLE public.a (id integer);"}}
	viewDiff := &diff.ViewDiffResult{}

	result := pistachio.OrderStatements(
		desiredEnums, desiredDomains, desiredTables, desiredViews,
		enumDiff, domainDiff, tableDiff, viewDiff,
	)

	// Should still produce output (via fallback)
	require.NotEmpty(t, result)
	assert.Contains(t, result[0], "CREATE TYPE")
	assert.Contains(t, result[1], "CREATE TABLE")
}
