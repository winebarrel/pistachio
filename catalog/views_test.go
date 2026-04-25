package catalog_test

import (
	"context"
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
