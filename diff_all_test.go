package pistachio

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/toposort"
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
		// An object kind the order has no key for, and a name too short to
		// hold the object it is on, are unplaced rather than an error.
		{"DROP SCHEMA public;", ""},
		{"COMMENT ON COLUMN col IS 'x';", ""},
		{"CREATE INDEX idx ON ONLY public.t (x);", "public.t"},
		{"CREATE INDEX bad_no_on;", ""},
		// DROP INDEX / ALTER INDEX
		{`DROP INDEX public.idx_users_name;`, ""},
		{`ALTER INDEX public.idx_old RENAME TO idx_new;`, ""},
		{`COMMENT ON COLUMN "S"."T".col IS 'x';`, `"S"."T"`},
		{`CREATE TABLE public."escaped""quote" (id integer);`, `public."escaped""quote"`},
		// PostgreSQL rejects a bare $foo, and model.Ident writes such a name
		// quoted, which is the form that comes back.
		{`CREATE TABLE public."$foo" (id integer);`, `public."$foo"`},
		{`COMMENT ON COLUMN "S""x"."T".col IS 'x';`, `"S""x"."T"`},
		{`ALTER MATERIALIZED VIEW public.mv RENAME TO mv2;`, "public.mv"},
		// An unlogged sequence is the same object as a logged one, and its
		// statement has to reach the same position.
		{"CREATE UNLOGGED SEQUENCE public.jobs_seq;", "public.jobs_seq"},
		{"CREATE SEQUENCE public.jobs_seq;", "public.jobs_seq"},
		// The spelling is the parser's business, not a list's: a statement
		// PostgreSQL reads is one the order can place.
		{"CREATE TABLE IF NOT EXISTS public.users (id integer);", "public.users"},
		{"ALTER SEQUENCE public.jobs_seq OWNED BY public.jobs.id;", "public.jobs_seq"},
		{"ALTER SEQUENCE public.jobs_seq RENAME TO tasks_seq;", "public.jobs_seq"},
		// A rename on a type or a domain carries the name alone, where the
		// rest carry a relation.
		{"ALTER TYPE public.status RENAME TO state;", "public.status"},
		{"ALTER DOMAIN public.pos_int RENAME TO positive_int;", "public.pos_int"},
		{"DROP SEQUENCE public.jobs_seq;", "public.jobs_seq"},
		{"COMMENT ON SEQUENCE public.jobs_seq IS 'x';", "public.jobs_seq"},
		{"CREATE MATERIALIZED VIEW public.mv AS SELECT 1;", "public.mv"},
		{"DROP MATERIALIZED VIEW public.mv;", "public.mv"},
		{"ALTER VIEW public.v SET (check_option='local');", "public.v"},
		{"ALTER VIEW public.v RENAME TO v2;", "public.v"},
		{"ALTER MATERIALIZED VIEW public.mv SET (fillfactor=90);", "public.mv"},
		{"COMMENT ON VIEW public.v IS 'x';", "public.v"},
		{"COMMENT ON MATERIALIZED VIEW public.mv IS 'x';", "public.mv"},
		{"COMMENT ON TYPE public.status IS 'x';", "public.status"},
		{"COMMENT ON DOMAIN public.pos_int IS 'x';", "public.pos_int"},
		{"CREATE TYPE public.addr AS (city text);", "public.addr"},
		{"ALTER TYPE public.addr ADD ATTRIBUTE zip text;", "public.addr"},
		{"COMMENT ON INDEX public.idx_users_name IS 'x';", "public.idx_users_name"},
		// A trigger and a policy belong to the relation they are on.
		{"CREATE TRIGGER trg BEFORE INSERT ON public.users FOR EACH ROW EXECUTE FUNCTION public.f();", "public.users"},
		{"CREATE CONSTRAINT TRIGGER trg AFTER INSERT ON public.users FOR EACH ROW EXECUTE FUNCTION public.f();", "public.users"},
		{"CREATE OR REPLACE TRIGGER trg BEFORE INSERT ON public.users FOR EACH ROW EXECUTE FUNCTION public.f();", "public.users"},
		{"DROP TRIGGER trg ON public.users;", "public.users"},
		{"ALTER TRIGGER trg ON public.users RENAME TO trg2;", "public.users"},
		{"CREATE POLICY p ON public.users FOR SELECT USING (true);", "public.users"},
		{"ALTER POLICY p ON public.users USING (false);", "public.users"},
		{"DROP POLICY p ON public.users;", "public.users"},
		// A routine is keyed by its name alone, without the argument list.
		{"CREATE OR REPLACE FUNCTION public.f(a integer) RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql;", "routine:public.f"},
		{"DROP FUNCTION public.f(integer);", "routine:public.f"},
		{"COMMENT ON FUNCTION public.f(integer) IS 'x';", "routine:public.f"},
		{"CREATE OR REPLACE PROCEDURE public.p(a integer) AS $$ BEGIN END $$ LANGUAGE plpgsql;", "routine:public.p"},
		{"DROP PROCEDURE public.p(integer);", "routine:public.p"},
		{"COMMENT ON PROCEDURE public.p(integer) IS 'x';", "routine:public.p"},
		// CREATE UNIQUE INDEX should be recognized like CREATE INDEX
		{"CREATE UNIQUE INDEX idx_users_email ON public.users USING btree (email);", "public.users"},
		// CONCURRENTLY variants
		{"CREATE INDEX CONCURRENTLY idx_name ON public.users USING btree (name);", "public.users"},
		{"CREATE UNIQUE INDEX CONCURRENTLY idx_email ON public.users USING btree (email);", "public.users"},
		{"DROP INDEX CONCURRENTLY public.idx_name;", ""},
		// --bulk-alter merged form: fqtn is followed by a newline, then
		// indented action lines. extractObjectName must still recover the
		// fqtn so topological ordering can place the merged statement
		// alongside other ops on the same table.
		{"ALTER TABLE public.users\n  ADD COLUMN x int,\n  ADD COLUMN y int;", "public.users"},
		{"ALTER TABLE \"MySchema\".\"MyTable\"\n  ADD COLUMN x int,\n  DROP CONSTRAINT chk;", `"MySchema"."MyTable"`},
	}

	for _, tt := range tests {
		name := tt.sql
		if len(name) > 40 {
			name = name[:40]
		}
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, extractObjectName(tt.sql))
		})
	}
}

// emptySchema returns one side of a diff with every kind present and empty,
// for a case to fill in the kinds it is about.
func emptySchema() *schemaObjects {
	return &schemaObjects{
		Tables:         orderedmap.New[string, *model.Table](),
		Views:          orderedmap.New[string, *model.View](),
		Enums:          orderedmap.New[string, *model.Enum](),
		Domains:        orderedmap.New[string, *model.Domain](),
		CompositeTypes: orderedmap.New[string, *model.CompositeType](),
		Sequences:      orderedmap.New[string, *model.Sequence](),
		Routines:       orderedmap.New[string, *model.Routine](),
	}
}

// emptyDiffs returns a diff with every kind present and no statement in any
// of them.
func emptyDiffs() *objectDiffs {
	return &objectDiffs{
		Tables:         &diff.TableDiffResult{},
		Views:          &diff.ViewDiffResult{},
		Enums:          &diff.EnumDiffResult{},
		Domains:        &diff.DomainDiffResult{},
		CompositeTypes: &diff.CompositeTypeDiffResult{},
		Sequences:      &diff.SequenceDiffResult{},
		Routines:       &diff.RoutineDiffResult{},
	}
}

func TestOrderStatements_Fallback(t *testing.T) {
	// Test that fallbackOrder is used when topological sort would fail. A
	// domain built on text and a table named text holding it read as a cycle,
	// since the bare text resolves to the table.
	desired := emptySchema()
	desired.Domains.Set("public.d1", &model.Domain{Schema: "public", Name: "d1", BaseType: "text"})
	tbl := &model.Table{Schema: "public", Name: "text"}
	tbl.Columns = orderedmap.New[string, *model.Column]()
	tbl.Columns.Set("v", &model.Column{Name: "v", TypeName: "public.d1"})
	desired.Tables.Set("public.text", tbl)

	diffs := emptyDiffs()
	diffs.Enums = &diff.EnumDiffResult{Stmts: []string{"CREATE TYPE public.s AS ENUM ('x');"}}
	diffs.Tables = &diff.TableDiffResult{
		Stmts:       []string{"CREATE TABLE public.a (id integer);"},
		PolicyStmts: []string{"CREATE POLICY p ON public.a USING (true);"},
	}

	result := orderStatements(emptySchema(), desired, diffs)

	// Should still produce output (via fallback), policies last.
	require.Len(t, result, 3)
	assert.Contains(t, result[0], "CREATE TYPE")
	assert.Contains(t, result[1], "CREATE TABLE")
	assert.Contains(t, result[2], "CREATE POLICY")
}

func TestOrderStatements_DropUsesCurrentSchema(t *testing.T) {
	// View B depends on View A in the current schema.
	// When both are dropped, B must be dropped before A.
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

	current := emptySchema()
	current.Tables = currentTables
	current.Views = currentViews

	// Desired: only table, both views dropped
	desired := emptySchema()
	desired.Tables = orderedmap.New[string, *model.Table]()
	desired.Tables.Set("public.users", tbl)

	diffs := emptyDiffs()
	diffs.Views = &diff.ViewDiffResult{
		DropStmts: []string{
			"DROP VIEW public.view_a;",
			"DROP VIEW public.view_b;",
		},
	}

	result := orderStatements(current, desired, diffs)

	require.Len(t, result, 2)
	// view_b depends on view_a -> view_b must be dropped first (reverse topo order)
	assert.Contains(t, result[0], "view_b", "dependent view dropped first")
	assert.Contains(t, result[1], "view_a", "dependency dropped second")
}

func TestOrderStatements_DropFallbackOnCurrentCycle(t *testing.T) {
	// A domain built on text and a table named text read as a cycle on the
	// current side, so drop ordering falls back.
	current := emptySchema()
	current.Domains.Set("public.d1", &model.Domain{Schema: "public", Name: "d1", BaseType: "text"})
	tbl := &model.Table{Schema: "public", Name: "text"}
	tbl.Columns = orderedmap.New[string, *model.Column]()
	tbl.Columns.Set("v", &model.Column{Name: "v", TypeName: "public.d1"})
	current.Tables.Set("public.text", tbl)

	_, err := toposort.OrderFromSchema(current.Enums, current.Domains, current.CompositeTypes, current.Tables, current.Views, current.Sequences, current.Routines)
	require.Error(t, err, "the current schema has to fail the sort for the fallback to run")

	diffs := emptyDiffs()
	diffs.Tables = &diff.TableDiffResult{DropStmts: []string{"DROP TABLE public.text;"}}
	diffs.Domains = &diff.DomainDiffResult{DropStmts: []string{"DROP DOMAIN public.d1;"}}

	result := orderStatements(current, emptySchema(), diffs)
	assert.Equal(t, []string{"DROP TABLE public.text;", "DROP DOMAIN public.d1;"}, result)
}

func TestOrderStatements_UnknownPosBeforeKnown(t *testing.T) {
	// Statements with unknown position (e.g., RENAME, INDEX ops) should
	// be placed before topo-ordered statements, not after.
	currentTables := orderedmap.New[string, *model.Table]()
	tbl := &model.Table{Schema: "public", Name: "users"}
	tbl.Columns = orderedmap.New[string, *model.Column]()
	tbl.Indexes = orderedmap.New[string, *model.Index]()
	tbl.Constraints = orderedmap.New[string, *model.Constraint]()
	tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
	currentTables.Set("public.users", tbl)

	tbl2 := &model.Table{Schema: "public", Name: "accounts"}
	tbl2.Columns = orderedmap.New[string, *model.Column]()
	tbl2.Indexes = orderedmap.New[string, *model.Index]()
	tbl2.Constraints = orderedmap.New[string, *model.Constraint]()
	tbl2.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()

	current := emptySchema()
	current.Tables = currentTables

	desired := emptySchema()
	desired.Tables.Set("public.accounts", tbl2)

	diffs := emptyDiffs()
	diffs.Tables = &diff.TableDiffResult{
		Stmts: []string{
			// RENAME uses old name -> not in desired posMap -> pos=-1
			"ALTER TABLE public.users RENAME TO accounts;",
			// Column change uses new name -> in desired posMap
			"ALTER TABLE public.accounts ADD COLUMN name text;",
		},
	}

	result := orderStatements(current, desired, diffs)

	require.Len(t, result, 2)
	// RENAME (unknown pos) must come before ADD COLUMN (known pos)
	assert.Contains(t, result[0], "RENAME TO accounts")
	assert.Contains(t, result[1], "ADD COLUMN name")
}

func TestOrderStatements_CreateUniqueIndexAfterTable(t *testing.T) {
	// CREATE UNIQUE INDEX must be ordered after the CREATE TABLE it belongs to.
	// Bug: extractObjectName did not recognize "CREATE UNIQUE INDEX" so the
	// statement got pos=-1 and was placed before the CREATE TABLE.

	// Desired: one table with a unique index
	tbl := &model.Table{Schema: "public", Name: "deposits"}
	tbl.Columns = orderedmap.New[string, *model.Column]()
	tbl.Indexes = orderedmap.New[string, *model.Index]()
	tbl.Constraints = orderedmap.New[string, *model.Constraint]()
	tbl.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()

	desired := emptySchema()
	desired.Tables.Set("public.deposits", tbl)

	diffs := emptyDiffs()
	diffs.Tables = &diff.TableDiffResult{
		Stmts: []string{
			"CREATE TABLE public.deposits (\n  id integer,\n  account_id integer,\n  item_key text\n);",
			"CREATE UNIQUE INDEX ix_deposits_account_item ON public.deposits USING btree (account_id, item_key);",
		},
	}

	result := orderStatements(emptySchema(), desired, diffs)

	require.Len(t, result, 2)
	assert.Contains(t, result[0], "CREATE TABLE", "table must be created first")
	assert.Contains(t, result[1], "CREATE UNIQUE INDEX", "unique index must come after table")
}

func TestExtractObjectName_QuotedWithEscapedQuote(t *testing.T) {
	// COMMENT ON COLUMN with escaped quotes in identifier
	got := extractObjectName(`COMMENT ON COLUMN "My""Schema"."My""Table".col IS 'x';`)
	assert.Equal(t, `"My""Schema"."My""Table"`, got)
}

func TestCompareTaggedPos(t *testing.T) {
	// Both unknown: preserve original order via stable sort
	assert.False(t, compareTaggedPos(-1, -1, false))
	assert.False(t, compareTaggedPos(-1, -1, true))

	// Only i unknown: unknown sorts before known
	assert.True(t, compareTaggedPos(-1, 0, false))
	assert.True(t, compareTaggedPos(-1, 5, true))

	// Only j unknown: known never precedes unknown
	assert.False(t, compareTaggedPos(0, -1, false))
	assert.False(t, compareTaggedPos(5, -1, true))

	// Both known: forward sort
	assert.True(t, compareTaggedPos(1, 2, false))
	assert.False(t, compareTaggedPos(2, 1, false))

	// Both known: reverse sort
	assert.True(t, compareTaggedPos(2, 1, true))
	assert.False(t, compareTaggedPos(1, 2, true))
}

func TestDroppedKeys(t *testing.T) {
	constraints, indexes := droppedKeys([]string{
		"ALTER TABLE public.t DROP CONSTRAINT t_pkey;",
		`ALTER TABLE "My Schema"."T" DROP CONSTRAINT "Key.One";`,
		"ALTER TABLE public.t ADD CONSTRAINT t_pkey PRIMARY KEY (a, b);",
		"ALTER TABLE public.t DROP COLUMN n, DROP CONSTRAINT t_n_key;",
		"DROP INDEX public.t_code_idx;",
		"DROP INDEX CONCURRENTLY public.t_name_idx;",
		"DROP VIEW public.v;",
		"ALTER TABLE public.t DROP CONSTRAINT a b;",
	})
	assert.Equal(t, []string{
		"public.t.t_pkey",
		`"My Schema"."T"."Key.One"`,
		"public.t.t_n_key",
	}, constraints)
	assert.Equal(t, []string{"public.t_code_idx", "public.t_name_idx"}, indexes)
}

func TestSplitConstraintKey(t *testing.T) {
	table, name := splitConstraintKey("public.t.t_pkey")
	assert.Equal(t, "public.t", table)
	assert.Equal(t, "t_pkey", name)

	table, name = splitConstraintKey(`"My Schema"."T"."Key.One"`)
	assert.Equal(t, `"My Schema"."T"`, table)
	assert.Equal(t, `"Key.One"`, name)
}
