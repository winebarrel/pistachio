package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestTables(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("empty", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, "")
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, tables.Len())
	})

	t.Run("single table", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				name text NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, tables.Len())

		tbl, ok := tables.GetOk("public.users")
		require.True(t, ok)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, "public", tbl.Schema)
		assert.Equal(t, 2, tbl.Columns.Len())
		assert.Equal(t, 1, tbl.Constraints.Len())
	})

	t.Run("multiple tables", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE TABLE public.posts (
				id integer NOT NULL,
				CONSTRAINT posts_pkey PRIMARY KEY (id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, tables.Len())

		_, ok := tables.GetOk("public.users")
		assert.True(t, ok)
		_, ok = tables.GetOk("public.posts")
		assert.True(t, ok)
	})

	t.Run("unlogged", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE UNLOGGED TABLE public.logs (
				id integer NOT NULL,
				CONSTRAINT logs_pkey PRIMARY KEY (id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.logs")
		require.NotNil(t, tbl)
		assert.True(t, tbl.Unlogged)
	})

	t.Run("table comment", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			COMMENT ON TABLE public.users IS 'User accounts';
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		require.NotNil(t, tbl.Comment)
		assert.Equal(t, "User accounts", *tbl.Comment)
	})

	t.Run("partitioned table", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.logs (
				id integer NOT NULL,
				created_at date NOT NULL
			) PARTITION BY RANGE (created_at);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.logs")
		require.NotNil(t, tbl)
		assert.True(t, tbl.Partitioned)
		require.NotNil(t, tbl.PartitionDef)
		assert.Contains(t, *tbl.PartitionDef, "created_at")
	})

	t.Run("partition child records a schema-qualified parent", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.logs (
				id integer NOT NULL,
				created_at date NOT NULL
			) PARTITION BY RANGE (created_at);
			CREATE TABLE public.logs_2024 PARTITION OF public.logs
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		child := tables.Get("public.logs_2024")
		require.NotNil(t, child)
		require.NotNil(t, child.PartitionOf)
		// Must match Table.FQTN so the dependency graph and the emitted SQL
		// both resolve the parent.
		assert.Equal(t, "public.logs", *child.PartitionOf)
		assert.Equal(t, tables.Get("public.logs").FQTN(), *child.PartitionOf)

		parent := tables.Get("public.logs")
		require.NotNil(t, parent)
		assert.Nil(t, parent.PartitionOf)
	})

	t.Run("partition parent name is quoted when needed", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public."Odd Parent" (
				id integer NOT NULL,
				created_at date NOT NULL
			) PARTITION BY RANGE (created_at);
			CREATE TABLE public."Odd Child" PARTITION OF public."Odd Parent"
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		child := tables.Get(`public."Odd Child"`)
		require.NotNil(t, child)
		require.NotNil(t, child.PartitionOf)
		assert.Equal(t, `public."Odd Parent"`, *child.PartitionOf)
	})
}
