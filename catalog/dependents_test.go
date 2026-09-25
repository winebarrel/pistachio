package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestViewDependents(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("views, materialized views and a schema this run does not manage", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			DROP SCHEMA IF EXISTS reporting CASCADE;
			CREATE SCHEMA reporting;
			CREATE TABLE public.employees (id integer NOT NULL, name text, dept text);
			CREATE VIEW public.staff AS SELECT id, name, dept FROM public.employees;
			CREATE VIEW public.eng_staff AS SELECT id, name FROM public.staff WHERE dept = 'eng';
			CREATE VIEW public.eng_names AS SELECT name FROM public.eng_staff;
			CREATE VIEW public.via_cte AS WITH s AS (SELECT id FROM public.staff) SELECT id FROM s;
			CREATE MATERIALIZED VIEW public.staff_count AS SELECT dept, count(*) AS total FROM public.staff GROUP BY dept;
			CREATE VIEW reporting.outside AS SELECT id FROM public.staff;
			CREATE VIEW public.lonely AS SELECT 1 AS one;
		`)
		t.Cleanup(func() {
			_, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS reporting CASCADE")
			require.NoError(t, err)
		})

		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		dependents, err := cat.ViewDependents(ctx)
		require.NoError(t, err)

		// A reference from inside a CTE counts, and so does one from a schema
		// the run does not manage, because either one blocks the drop.
		assert.Equal(t, []catalog.Dependent{
			{Kind: "materialized view", Name: "public.staff_count", Relation: "public.staff_count"},
			{Kind: "view", Name: "public.eng_staff", Relation: "public.eng_staff"},
			{Kind: "view", Name: "public.via_cte", Relation: "public.via_cte"},
			{Kind: "view", Name: "reporting.outside", Relation: "reporting.outside"},
		}, dependents["public.staff"])

		// Only the direct dependents: eng_names reads eng_staff, and shows up
		// under eng_staff rather than under staff.
		assert.Equal(t, []catalog.Dependent{
			{Kind: "view", Name: "public.eng_names", Relation: "public.eng_names"},
		}, dependents["public.eng_staff"])

		// A view nothing reads is absent rather than present and empty, and
		// its own rewrite rule is not a dependent of itself.
		assert.NotContains(t, dependents, "public.lonely")
		assert.NotContains(t, dependents, "public.eng_names")
	})

	// A BEGIN ATOMIC body is parsed at creation, so the routine records the
	// dependency a view does. A routine that returns the view's row type
	// records it on the type the view owns, which blocks the drop the same
	// way. A body written as a string literal records neither.
	t.Run("routine bodies and the row type", func(t *testing.T) {
		if testutil.ServerMajorVersion(t, ctx, conn) < 14 {
			t.Skip("requires PostgreSQL 14 or later")
		}
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.t (id integer NOT NULL, n text);
			CREATE VIEW public.v AS SELECT id, n FROM public.t;
			CREATE FUNCTION public.total() RETURNS bigint LANGUAGE sql BEGIN ATOMIC SELECT count(*) FROM public.v; END;
			CREATE FUNCTION public.row_of_v() RETURNS public.v LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
			CREATE FUNCTION public.plain() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM public.v $$;
		`)

		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		dependents, err := cat.ViewDependents(ctx)
		require.NoError(t, err)

		assert.Equal(t, []catalog.Dependent{
			{Kind: "function", Name: "public.row_of_v()"},
			{Kind: "function", Name: "public.total()"},
		}, dependents["public.v"])
	})

	// A rule on a plain table records the dependency a view does, through the
	// same rewrite rule, so it is named as a rule rather than as the table it
	// sits on. A policy comes through the generic branch.
	t.Run("rule and policy", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.t (id integer, n text);
			CREATE VIEW public.v AS SELECT id, n FROM public.t;
			CREATE TABLE public.log (id integer);
			CREATE RULE r AS ON INSERT TO public.log DO INSTEAD SELECT id FROM public.v;
			CREATE TABLE public.secured (id integer);
			CREATE POLICY p ON public.secured USING (id IN (SELECT id FROM public.v));
		`)

		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		dependents, err := cat.ViewDependents(ctx)
		require.NoError(t, err)

		// Relation stays empty for both: neither is a view the plan can drop
		// to get the target out from under them.
		assert.Equal(t, []catalog.Dependent{
			{Kind: "policy", Name: "p on public.secured"},
			{Kind: "rule", Name: "r on public.log"},
		}, dependents["public.v"])
	})

	// A table is not read here: the check is about the relations the view
	// diff drops.
	t.Run("a table is not a target", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.t (id integer NOT NULL, n text);
			CREATE VIEW public.v AS SELECT id, n FROM public.t;
		`)

		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		dependents, err := cat.ViewDependents(ctx)
		require.NoError(t, err)

		assert.NotContains(t, dependents, "public.t")
	})
}

func TestColumnDependents(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("each kind that blocks a type change", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			DROP SCHEMA IF EXISTS reporting CASCADE;
			CREATE SCHEMA reporting;
			CREATE FUNCTION public.trg() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
			CREATE TABLE public.t (
				id integer PRIMARY KEY, a integer, b integer, c integer, d integer, e integer, f integer,
				g integer GENERATED ALWAYS AS (e * 2) STORED
			);
			CREATE TABLE public.r (tid integer REFERENCES public.t (id));
			ALTER TABLE public.t ENABLE ROW LEVEL SECURITY;
			CREATE POLICY p ON public.t USING (a > 0);
			CREATE TRIGGER tr BEFORE UPDATE OF b ON public.t FOR EACH ROW EXECUTE FUNCTION public.trg();
			CREATE RULE r AS ON INSERT TO public.t WHERE (new.c < 0) DO INSTEAD NOTHING;
			CREATE FUNCTION public.total() RETURNS bigint LANGUAGE sql BEGIN ATOMIC SELECT sum(d) FROM public.t; END;
			CREATE INDEX t_f_idx ON public.t ((f + 1));
			ALTER TABLE public.t ADD CONSTRAINT t_f_check CHECK (f > 0);
			CREATE VIEW public.v AS SELECT id, f FROM public.t;
			CREATE MATERIALIZED VIEW reporting.mv AS SELECT id FROM public.t;
		`)
		t.Cleanup(func() {
			_, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS reporting CASCADE")
			require.NoError(t, err)
		})

		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		dependents, err := cat.ColumnDependents(ctx)
		require.NoError(t, err)

		// The foreign key is rebuilt, so only the views are read, including one
		// outside the managed schemas.
		assert.Equal(t, []catalog.Dependent{
			{Kind: "materialized view", Name: "reporting.mv", Relation: "reporting.mv"},
			{Kind: "view", Name: "public.v", Relation: "public.v"},
		}, dependents["public.t.id"])
		assert.Equal(t, []catalog.Dependent{{Kind: "policy", Name: "p on public.t"}}, dependents["public.t.a"])
		assert.Equal(t, []catalog.Dependent{{Kind: "trigger", Name: "tr on public.t"}}, dependents["public.t.b"])
		assert.Equal(t, []catalog.Dependent{{Kind: "rule", Name: "r on public.t"}}, dependents["public.t.c"])
		assert.Equal(t, []catalog.Dependent{{Kind: "function", Name: "public.total()"}}, dependents["public.t.d"])
		assert.Equal(t, []catalog.Dependent{{Kind: "generated column", Name: "public.t.g"}}, dependents["public.t.e"])
		// The index and the check constraint are rebuilt too.
		assert.Equal(t, []catalog.Dependent{{Kind: "view", Name: "public.v", Relation: "public.v"}}, dependents["public.t.f"])
		assert.NotContains(t, dependents, "public.t.g")
		assert.NotContains(t, dependents, "public.r.tid")
	})
}

func TestDependentString(t *testing.T) {
	assert.Equal(t, "view public.eng_staff", catalog.Dependent{Kind: "view", Name: "public.eng_staff"}.String())
	assert.Equal(t, "function public.total()", catalog.Dependent{Kind: "function", Name: "public.total()"}.String())
}
