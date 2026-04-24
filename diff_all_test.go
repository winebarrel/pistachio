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
		{`CREATE TABLE public.$foo (id integer);`, "public.$foo"},
		{`COMMENT ON COLUMN "S""x"."T".col IS 'x';`, `"S""x"."T"`},
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

	currentEnums := orderedmap.New[string, *model.Enum]()
	currentDomains := orderedmap.New[string, *model.Domain]()
	currentTables := orderedmap.New[string, *model.Table]()
	currentViews := orderedmap.New[string, *model.View]()

	result := pistachio.OrderStatements(
		currentEnums, currentDomains, currentTables, currentViews,
		desiredEnums, desiredDomains, desiredTables, desiredViews,
		enumDiff, domainDiff, tableDiff, viewDiff,
	)

	// Should still produce output (via fallback)
	require.NotEmpty(t, result)
	assert.Contains(t, result[0], "CREATE TYPE")
	assert.Contains(t, result[1], "CREATE TABLE")
}

func TestOrderStatements_DropUsesCurrentSchema(t *testing.T) {
	// View B depends on View A in the current schema.
	// When both are dropped, B must be dropped before A.
	currentEnums := orderedmap.New[string, *model.Enum]()
	currentDomains := orderedmap.New[string, *model.Domain]()
	currentTables := orderedmap.New[string, *model.Table]()
	tbl := &model.Table{Schema: "public", Name: "users"}
	tbl.Columns = orderedmap.New[string, *model.Column]()
	tbl.Indexes = orderedmap.New[string, *model.Index]()
	tbl.Constraints = orderedmap.New[string, *model.Constraint]()
	tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	currentTables.Set("public.users", tbl)

	currentViews := orderedmap.New[string, *model.View]()
	currentViews.Set("public.view_a", &model.View{
		Schema: "public", Name: "view_a",
		Definition: "SELECT id FROM public.users",
	})
	currentViews.Set("public.view_b", &model.View{
		Schema: "public", Name: "view_b",
		Definition: "SELECT id FROM public.view_a",
	})

	// Desired: only table, both views dropped
	desiredEnums := orderedmap.New[string, *model.Enum]()
	desiredDomains := orderedmap.New[string, *model.Domain]()
	desiredTables := orderedmap.New[string, *model.Table]()
	desiredTables.Set("public.users", tbl)
	desiredViews := orderedmap.New[string, *model.View]()

	enumDiff := &diff.EnumDiffResult{}
	domainDiff := &diff.DomainDiffResult{}
	tableDiff := &diff.TableDiffResult{}
	viewDiff := &diff.ViewDiffResult{
		DropStmts: []string{
			"DROP VIEW public.view_a;",
			"DROP VIEW public.view_b;",
		},
	}

	result := pistachio.OrderStatements(
		currentEnums, currentDomains, currentTables, currentViews,
		desiredEnums, desiredDomains, desiredTables, desiredViews,
		enumDiff, domainDiff, tableDiff, viewDiff,
	)

	require.Len(t, result, 2)
	// view_b depends on view_a → view_b must be dropped first (reverse topo order)
	assert.Contains(t, result[0], "view_b", "dependent view dropped first")
	assert.Contains(t, result[1], "view_a", "dependency dropped second")
}

func TestOrderStatements_DropFallbackOnCurrentCycle(t *testing.T) {
	// Current schema has cyclic FK → drop ordering should fall back
	refA := "a"
	refB := "b"
	schemaPublic := "public"

	currentEnums := orderedmap.New[string, *model.Enum]()
	currentDomains := orderedmap.New[string, *model.Domain]()
	currentViews := orderedmap.New[string, *model.View]()
	currentTables := orderedmap.New[string, *model.Table]()
	for _, cfg := range []struct{ name, ref string }{{"a", refB}, {"b", refA}} {
		tbl := &model.Table{Schema: "public", Name: cfg.name}
		tbl.Columns = orderedmap.New[string, *model.Column]()
		tbl.Indexes = orderedmap.New[string, *model.Index]()
		tbl.Constraints = orderedmap.New[string, *model.Constraint]()
		tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
		tbl.ForeignKeys.Set(cfg.name+"_fk", &model.ForeignKey{
			Constraint: model.Constraint{Name: cfg.name + "_fk"},
			RefSchema:  &schemaPublic,
			RefTable:   &cfg.ref,
		})
		currentTables.Set("public."+cfg.name, tbl)
	}

	desiredEnums := orderedmap.New[string, *model.Enum]()
	desiredDomains := orderedmap.New[string, *model.Domain]()
	desiredTables := orderedmap.New[string, *model.Table]()
	desiredViews := orderedmap.New[string, *model.View]()

	enumDiff := &diff.EnumDiffResult{}
	domainDiff := &diff.DomainDiffResult{}
	tableDiff := &diff.TableDiffResult{
		DropStmts: []string{"DROP TABLE public.a;", "DROP TABLE public.b;"},
	}
	viewDiff := &diff.ViewDiffResult{}

	result := pistachio.OrderStatements(
		currentEnums, currentDomains, currentTables, currentViews,
		desiredEnums, desiredDomains, desiredTables, desiredViews,
		enumDiff, domainDiff, tableDiff, viewDiff,
	)

	// Should still produce output via fallback (not panic or error)
	require.Len(t, result, 2)
}

func TestOrderStatements_UnknownPosBeforeKnown(t *testing.T) {
	// Statements with unknown position (e.g., RENAME, INDEX ops) should
	// be placed before topo-ordered statements, not after.
	currentEnums := orderedmap.New[string, *model.Enum]()
	currentDomains := orderedmap.New[string, *model.Domain]()
	currentViews := orderedmap.New[string, *model.View]()

	currentTables := orderedmap.New[string, *model.Table]()
	tbl := &model.Table{Schema: "public", Name: "users"}
	tbl.Columns = orderedmap.New[string, *model.Column]()
	tbl.Indexes = orderedmap.New[string, *model.Index]()
	tbl.Constraints = orderedmap.New[string, *model.Constraint]()
	tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	currentTables.Set("public.users", tbl)

	desiredEnums := orderedmap.New[string, *model.Enum]()
	desiredDomains := orderedmap.New[string, *model.Domain]()
	desiredViews := orderedmap.New[string, *model.View]()

	desiredTables := orderedmap.New[string, *model.Table]()
	tbl2 := &model.Table{Schema: "public", Name: "accounts"}
	tbl2.Columns = orderedmap.New[string, *model.Column]()
	tbl2.Indexes = orderedmap.New[string, *model.Index]()
	tbl2.Constraints = orderedmap.New[string, *model.Constraint]()
	tbl2.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	desiredTables.Set("public.accounts", tbl2)

	enumDiff := &diff.EnumDiffResult{}
	domainDiff := &diff.DomainDiffResult{}
	tableDiff := &diff.TableDiffResult{
		Stmts: []string{
			// RENAME uses old name → not in desired posMap → pos=-1
			"ALTER TABLE public.users RENAME TO accounts;",
			// Column change uses new name → in desired posMap
			"ALTER TABLE public.accounts ADD COLUMN name text;",
		},
	}
	viewDiff := &diff.ViewDiffResult{}

	result := pistachio.OrderStatements(
		currentEnums, currentDomains, currentTables, currentViews,
		desiredEnums, desiredDomains, desiredTables, desiredViews,
		enumDiff, domainDiff, tableDiff, viewDiff,
	)

	require.Len(t, result, 2)
	// RENAME (unknown pos) must come before ADD COLUMN (known pos)
	assert.Contains(t, result[0], "RENAME TO accounts")
	assert.Contains(t, result[1], "ADD COLUMN name")
}

func TestExtractObjectName_QuotedWithEscapedQuote(t *testing.T) {
	// COMMENT ON COLUMN with escaped quotes in identifier
	got := pistachio.ExtractObjectName(`COMMENT ON COLUMN "My""Schema"."My""Table".col IS 'x';`)
	assert.Equal(t, `"My""Schema"."My""Table"`, got)
}
