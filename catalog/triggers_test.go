package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
	"github.com/winebarrel/pistachio/model"
)

func TestListTriggers(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("table triggers", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE FUNCTION public.noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TABLE public.events (
				id integer NOT NULL,
				name text,
				CONSTRAINT events_pkey PRIMARY KEY (id)
			);
			CREATE TRIGGER events_audit AFTER UPDATE OF name ON public.events
				FOR EACH ROW WHEN (old.name IS DISTINCT FROM new.name)
				EXECUTE FUNCTION public.noop();
			CREATE CONSTRAINT TRIGGER events_check AFTER INSERT ON public.events
				DEFERRABLE INITIALLY DEFERRED
				FOR EACH ROW EXECUTE FUNCTION public.noop();
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.events")
		assert.Equal(t, []string{"events_audit", "events_check"}, tbl.Triggers.CollectKeys())

		// pg_get_triggerdef drops the schema from a relation and a function
		// the session reaches through search_path, the way pg_get_viewdef
		// does. The test connection leaves search_path at the server default,
		// which carries public, so nothing here is qualified. Schema and
		// Table come from pg_class and stay right either way.
		audit := tbl.Triggers.Get("events_audit")
		assert.Equal(t,
			"CREATE TRIGGER events_audit AFTER UPDATE OF name ON events FOR EACH ROW WHEN (old.name IS DISTINCT FROM new.name) EXECUTE FUNCTION noop()",
			audit.Definition)
		assert.True(t, audit.State.IsDefault())

		check := tbl.Triggers.Get("events_check")
		assert.Equal(t,
			"CREATE CONSTRAINT TRIGGER events_check AFTER INSERT ON events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION noop()",
			check.Definition)
	})

	// A foreign key installs its own triggers on both tables. PostgreSQL
	// maintains them, and pg_get_triggerdef writes them as constraint
	// triggers on internal RI functions, so they must not reach the model.
	t.Run("internal triggers excluded", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.parents (
				id integer NOT NULL,
				CONSTRAINT parents_pkey PRIMARY KEY (id)
			);
			CREATE TABLE public.children (
				id integer NOT NULL,
				parent_id integer,
				CONSTRAINT children_pkey PRIMARY KEY (id),
				CONSTRAINT children_parent_fkey FOREIGN KEY (parent_id) REFERENCES public.parents(id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		assert.Empty(t, tables.Get("public.parents").Triggers.CollectKeys())
		assert.Empty(t, tables.Get("public.children").Triggers.CollectKeys())
	})

	// A trigger on a partitioned table is cloned onto every partition. The
	// clone cannot be dropped on its own, and the parent's definition already
	// covers it, so only the parent's row belongs to the model.
	t.Run("partition clones excluded", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE FUNCTION public.noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TABLE public.logs (id integer NOT NULL, at date NOT NULL) PARTITION BY RANGE (at);
			CREATE TABLE public.logs_2026 PARTITION OF public.logs FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
			CREATE TRIGGER logs_audit BEFORE INSERT ON public.logs
				FOR EACH ROW EXECUTE FUNCTION public.noop();
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		assert.Equal(t, []string{"logs_audit"}, tables.Get("public.logs").Triggers.CollectKeys())
		assert.Empty(t, tables.Get("public.logs_2026").Triggers.CollectKeys())
	})

	t.Run("enable state", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE FUNCTION public.noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TABLE public.events (id integer NOT NULL);
			CREATE TRIGGER events_off BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION public.noop();
			CREATE TRIGGER events_always BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION public.noop();
			CREATE TRIGGER events_replica BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION public.noop();
			ALTER TABLE public.events DISABLE TRIGGER events_off;
			ALTER TABLE public.events ENABLE ALWAYS TRIGGER events_always;
			ALTER TABLE public.events ENABLE REPLICA TRIGGER events_replica;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		triggers := tables.Get("public.events").Triggers
		assert.Equal(t, model.TriggerState('D'), triggers.Get("events_off").State)
		assert.Equal(t, model.TriggerState('A'), triggers.Get("events_always").State)
		assert.Equal(t, model.TriggerState('R'), triggers.Get("events_replica").State)
	})

	t.Run("view trigger", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE FUNCTION public.noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TABLE public.events (id integer NOT NULL);
			CREATE VIEW public.recent_events AS SELECT id FROM public.events;
			CREATE TRIGGER recent_events_insert INSTEAD OF INSERT ON public.recent_events
				FOR EACH ROW EXECUTE FUNCTION public.noop();
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		views, err := cat.Views(ctx)
		require.NoError(t, err)

		vw := views.Get("public.recent_events")
		assert.Equal(t, []string{"recent_events_insert"}, vw.Triggers.CollectKeys())
		assert.Equal(t,
			"CREATE TRIGGER recent_events_insert INSTEAD OF INSERT ON recent_events FOR EACH ROW EXECUTE FUNCTION noop()",
			vw.Triggers.Get("recent_events_insert").Definition)
	})
}
