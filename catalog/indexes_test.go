package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestListIndexes(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("btree index", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				name text NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE INDEX idx_users_name ON public.users USING btree (name);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		assert.Equal(t, 1, tbl.Indexes.Len())

		idx, ok := tbl.Indexes.GetOk("idx_users_name")
		require.True(t, ok)
		assert.Equal(t, "users", idx.Table)
		assert.Equal(t, "public", idx.Schema)
		assert.Contains(t, idx.Definition, "btree")
	})

	t.Run("unique index", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				email text NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE UNIQUE INDEX users_email_idx ON public.users USING btree (email);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		idx, ok := tbl.Indexes.GetOk("users_email_idx")
		require.True(t, ok)
		assert.Contains(t, idx.Definition, "UNIQUE")
	})

	t.Run("hash index", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				name text NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE INDEX idx_users_name ON public.users USING hash (name);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		idx, ok := tbl.Indexes.GetOk("idx_users_name")
		require.True(t, ok)
		assert.Contains(t, idx.Definition, "hash")
	})

	t.Run("partial index", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				active boolean NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE INDEX idx_active_users ON public.users (id) WHERE active;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		idx, ok := tbl.Indexes.GetOk("idx_active_users")
		require.True(t, ok)
		assert.Contains(t, idx.Definition, "WHERE")
	})

	t.Run("multi column index", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				first_name text NOT NULL,
				last_name text NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE INDEX idx_users_fullname ON public.users (first_name, last_name);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		idx, ok := tbl.Indexes.GetOk("idx_users_fullname")
		require.True(t, ok)
		assert.Contains(t, idx.Definition, "first_name")
		assert.Contains(t, idx.Definition, "last_name")
	})

	t.Run("expression index", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				email text NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE INDEX idx_users_email_lower ON public.users (lower(email));
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		idx, ok := tbl.Indexes.GetOk("idx_users_email_lower")
		require.True(t, ok)
		assert.Contains(t, idx.Definition, "lower")
	})

	t.Run("constraint index excluded", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		// PRIMARY KEY creates an index, but it should not appear in Indexes
		assert.Equal(t, 0, tbl.Indexes.Len())
	})
}
