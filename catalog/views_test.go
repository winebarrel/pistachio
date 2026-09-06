package catalog_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestViews(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("empty", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, "")
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		views, err := cat.Views(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, views.Len())
	})

	t.Run("single view", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				name text NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE VIEW public.active_users AS SELECT id, name FROM public.users;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		views, err := cat.Views(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, views.Len())

		v, ok := views.GetOk("public.active_users")
		require.True(t, ok)
		assert.Equal(t, "active_users", v.Name)
		assert.Equal(t, "public", v.Schema)
	})

	t.Run("materialized view", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE MATERIALIZED VIEW public.user_stats AS SELECT count(*) AS cnt FROM public.users;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		views, err := cat.Views(ctx)
		require.NoError(t, err)

		v, ok := views.GetOk("public.user_stats")
		require.True(t, ok)
		assert.Equal(t, "user_stats", v.Name)
		assert.True(t, v.Materialized)
		assert.NotEmpty(t, v.Definition)
	})

	t.Run("materialized view with index", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE MATERIALIZED VIEW public.user_stats AS SELECT count(*) AS cnt FROM public.users;
			CREATE INDEX idx_user_stats_cnt ON public.user_stats (cnt);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		views, err := cat.Views(ctx)
		require.NoError(t, err)

		v, ok := views.GetOk("public.user_stats")
		require.True(t, ok)
		assert.True(t, v.Materialized)
		assert.Equal(t, 1, v.Indexes.Len())

		idx, ok := v.Indexes.GetOk("idx_user_stats_cnt")
		require.True(t, ok)
		assert.Equal(t, "user_stats", idx.Table)
	})

	t.Run("view check option", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.items (
				id integer NOT NULL,
				status text NOT NULL,
				CONSTRAINT items_pkey PRIMARY KEY (id)
			);
			CREATE VIEW public.cascaded_items AS SELECT id, status FROM public.items WHERE status = 'active' WITH CHECK OPTION;
			CREATE VIEW public.local_items AS SELECT id, status FROM public.items WHERE status = 'active' WITH LOCAL CHECK OPTION;
			CREATE VIEW public.barrier_items WITH (security_barrier = true) AS SELECT id, status FROM public.items WHERE status = 'active';
			CREATE VIEW public.upper_items WITH (check_option = 'CASCADED') AS SELECT id, status FROM public.items WHERE status = 'active';
			CREATE VIEW public.plain_items AS SELECT id, status FROM public.items WHERE status = 'active';
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		views, err := cat.Views(ctx)
		require.NoError(t, err)

		assert.Equal(t, "cascaded", views.Get("public.cascaded_items").CheckOption)
		assert.Equal(t, "local", views.Get("public.local_items").CheckOption)
		// The value is stored as written and validated case-insensitively.
		assert.Equal(t, "cascaded", views.Get("public.upper_items").CheckOption)
		// Another option in reloptions does not stand in for it.
		assert.Empty(t, views.Get("public.barrier_items").CheckOption)
		assert.Empty(t, views.Get("public.plain_items").CheckOption)
	})

	t.Run("view storage params", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.items (
				id integer NOT NULL,
				body text,
				CONSTRAINT items_pkey PRIMARY KEY (id)
			);
			CREATE VIEW public.safe_items WITH (security_invoker = true, security_barrier = true) AS SELECT id FROM public.items WITH CASCADED CHECK OPTION;
			CREATE VIEW public.plain_items AS SELECT id FROM public.items;
			CREATE MATERIALIZED VIEW public.item_bodies WITH (fillfactor = 70, toast.autovacuum_enabled = off) AS SELECT id, body FROM public.items;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		views, err := cat.Views(ctx)
		require.NoError(t, err)

		// Keyed in name order whatever order reloptions kept, and the check
		// option is not among them: it is read on its own.
		safe := views.Get("public.safe_items")
		assert.Equal(t, []string{"security_barrier", "security_invoker"}, slices.Collect(safe.StorageParams.Keys()))
		assert.Equal(t, "true", safe.StorageParams.Get("security_barrier"))
		assert.Equal(t, "cascaded", safe.CheckOption)

		assert.Equal(t, 0, views.Get("public.plain_items").StorageParams.Len())

		// A materialized view carries the TOAST relation's parameters too.
		bodies := views.Get("public.item_bodies")
		assert.Equal(t, []string{"fillfactor", "toast.autovacuum_enabled"}, slices.Collect(bodies.StorageParams.Keys()))
		assert.Equal(t, "70", bodies.StorageParams.Get("fillfactor"))
		assert.Equal(t, "off", bodies.StorageParams.Get("toast.autovacuum_enabled"))
	})

	t.Run("view comment", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				name text NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE VIEW public.active_users AS SELECT id, name FROM public.users;
			COMMENT ON VIEW public.active_users IS 'Active users only';
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		views, err := cat.Views(ctx)
		require.NoError(t, err)

		v := views.Get("public.active_users")
		require.NotNil(t, v)
		require.NotNil(t, v.Comment)
		assert.Equal(t, "Active users only", *v.Comment)
	})
}
