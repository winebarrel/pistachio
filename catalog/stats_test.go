package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestTableStats(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("analyzed table", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				name text,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			INSERT INTO public.users SELECT g, 'n' FROM generate_series(1, 3) g;
			ANALYZE public.users;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		stats, err := cat.TableStats(ctx)
		require.NoError(t, err)

		st, ok := stats["public.users"]
		require.True(t, ok)
		assert.Equal(t, int64(3), st.Rows)
		assert.Equal(t, int64(8192), st.Bytes)
	})

	t.Run("table never analyzed reports -1 rows", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			INSERT INTO public.users SELECT g FROM generate_series(1, 3) g;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		stats, err := cat.TableStats(ctx)
		require.NoError(t, err)

		st, ok := stats["public.users"]
		require.True(t, ok)
		assert.Equal(t, int64(-1), st.Rows)
	})

	// A partitioned table holds no rows of its own; each partition carries its
	// own entry, which is what the caller sums.
	t.Run("partitioned table and its partitions", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.events (
				id integer NOT NULL,
				at date NOT NULL
			) PARTITION BY RANGE (at);
			CREATE TABLE public.events_2024 PARTITION OF public.events
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
			INSERT INTO public.events VALUES (1, '2024-03-01'), (2, '2024-04-01');
			ANALYZE public.events;
			ANALYZE public.events_2024;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		stats, err := cat.TableStats(ctx)
		require.NoError(t, err)

		assert.Contains(t, stats, "public.events")
		child, ok := stats["public.events_2024"]
		require.True(t, ok)
		assert.Equal(t, int64(2), child.Rows)
		assert.Equal(t, int64(8192), child.Bytes)
	})

	t.Run("a view is not a table", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (id integer NOT NULL);
			CREATE VIEW public.user_ids AS SELECT id FROM public.users;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		stats, err := cat.TableStats(ctx)
		require.NoError(t, err)

		assert.Contains(t, stats, "public.users")
		assert.NotContains(t, stats, "public.user_ids")
	})
}

func TestTypeChanges(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
		CREATE DOMAIN public.plain_text AS text;
		CREATE DOMAIN public.email AS text CHECK (VALUE ~ '@');
	`)
	cat, err := catalog.NewCatalog(conn, []string{"public"})
	require.NoError(t, err)

	t.Run("no changes asks nothing", func(t *testing.T) {
		infos, err := cat.TypeChanges(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, infos)
	})

	tests := []struct {
		name        string
		change      catalog.TypeChange
		known       bool
		sameBase    bool
		baseType    string
		binary      bool
		constrained bool
	}{
		// pg_cast holds no row for a type to itself, so Binary is false
		// whenever the two sides resolve to one base type. The caller reads
		// SameBase first, so it never consults Binary there.
		{
			name:     "same type keeps the base",
			change:   catalog.TypeChange{Src: "character varying", Dst: "character varying"},
			known:    true,
			sameBase: true,
			baseType: "character varying",
		},
		{
			name:     "varchar to text is binary coercible",
			change:   catalog.TypeChange{Src: "character varying", Dst: "text"},
			known:    true,
			baseType: "text",
			binary:   true,
		},
		{
			name:     "integer to bigint is a conversion",
			change:   catalog.TypeChange{Src: "integer", Dst: "bigint"},
			known:    true,
			baseType: "bigint",
		},
		{
			name:     "a domain stands for its base type",
			change:   catalog.TypeChange{Src: "text", Dst: "public.plain_text"},
			known:    true,
			sameBase: true,
			baseType: "text",
		},
		{
			name:        "a domain that carries a check is constrained",
			change:      catalog.TypeChange{Src: "text", Dst: "public.email"},
			known:       true,
			sameBase:    true,
			baseType:    "text",
			constrained: true,
		},
		{
			name:   "a name that does not resolve is unknown",
			change: catalog.TypeChange{Src: "text", Dst: "nosuchtype"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			infos, err := cat.TypeChanges(ctx, []catalog.TypeChange{tt.change})
			require.NoError(t, err)
			info, ok := infos[tt.change]
			require.True(t, ok)
			assert.Equal(t, tt.known, info.Known, "Known")
			assert.Equal(t, tt.sameBase, info.SameBase, "SameBase")
			assert.Equal(t, tt.baseType, info.BaseType, "BaseType")
			assert.Equal(t, tt.binary, info.Binary, "Binary")
			assert.Equal(t, tt.constrained, info.Constrained, "Constrained")
		})
	}
}

func TestVolatileFunctions(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")
	cat, err := catalog.NewCatalog(conn, []string{"public"})
	require.NoError(t, err)

	t.Run("no names asks nothing", func(t *testing.T) {
		volatile, err := cat.VolatileFunctions(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, volatile)
	})

	t.Run("reports volatility by name", func(t *testing.T) {
		volatile, err := cat.VolatileFunctions(ctx, []string{"random", "now", "length", "nosuchfunction"})
		require.NoError(t, err)
		assert.True(t, volatile["random"], "random is volatile")
		assert.False(t, volatile["now"], "now is stable")
		assert.False(t, volatile["length"], "length is immutable")
		assert.NotContains(t, volatile, "nosuchfunction")
	})
}
