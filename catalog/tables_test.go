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

func TestTables_StorageParams(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("none", func(t *testing.T) {
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
		tbl, ok := tables.GetOk("public.users")
		require.True(t, ok)
		assert.Equal(t, 0, tbl.StorageParams.Len())
	})

	// The TOAST relation's parameters are read under the toast. prefix
	// PostgreSQL sets them with, and the table's own without one. The text
	// column is what gives the table a TOAST relation to read.
	t.Run("table and toast", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.docs (
				id integer NOT NULL,
				body text,
				CONSTRAINT docs_pkey PRIMARY KEY (id)
			) WITH (fillfactor = 70, autovacuum_enabled = off, toast.autovacuum_enabled = off);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)
		tbl, ok := tables.GetOk("public.docs")
		require.True(t, ok)
		assert.Equal(t,
			[]string{"autovacuum_enabled", "fillfactor", "toast.autovacuum_enabled"},
			tbl.StorageParams.CollectKeys())
		assert.Equal(t, []string{"off", "70", "off"}, tbl.StorageParams.CollectValues())
	})
}

// An INHERITS child carries only what it declares itself. Reading the columns
// and constraints it merely inherits made the desired side, which holds what
// CREATE TABLE writes, look like it had dropped them.
func TestTables_inheritsChildLocalOnly(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
		CREATE TABLE public.parent (
			id integer NOT NULL,
			n integer,
			CONSTRAINT parent_n_check CHECK (n > 0)
		);
		CREATE TABLE public.child_empty () INHERITS (public.parent);
		CREATE TABLE public.child_merge (n integer NOT NULL) INHERITS (public.parent);
		CREATE TABLE public.child_own (extra text) INHERITS (public.parent);
		ALTER TABLE public.child_own ADD CONSTRAINT child_own_extra_check CHECK (extra <> '') NOT VALID;
	`)
	cat, err := catalog.NewCatalog(conn, []string{"public"})
	require.NoError(t, err)
	tables, err := cat.Tables(ctx)
	require.NoError(t, err)

	parent, ok := tables.GetOk("public.parent")
	require.True(t, ok)
	assert.Equal(t, []string{"id", "n"}, parent.Columns.CollectKeys())
	assert.Equal(t, []string{"parent_n_check"}, parent.Constraints.CollectKeys())

	// Declares nothing, so it holds nothing.
	empty, ok := tables.GetOk("public.child_empty")
	require.True(t, ok)
	assert.True(t, empty.IsInheritsChild())
	assert.Empty(t, empty.Columns.CollectKeys())
	assert.Empty(t, empty.Constraints.CollectKeys())

	// A redeclared column is local and inherited at once, and stays.
	merge, ok := tables.GetOk("public.child_merge")
	require.True(t, ok)
	assert.Equal(t, []string{"n"}, merge.Columns.CollectKeys())
	assert.Empty(t, merge.Constraints.CollectKeys())

	// Its own column and its own constraint, not the parent's.
	own, ok := tables.GetOk("public.child_own")
	require.True(t, ok)
	assert.Equal(t, []string{"extra"}, own.Columns.CollectKeys())
	assert.Equal(t, []string{"child_own_extra_check"}, own.Constraints.CollectKeys())
}

// A partition child keeps its inherited columns: its branch of the diff skips
// the column comparison but still compares their comments.
func TestTables_partitionChildKeepsInheritedColumns(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
		CREATE TABLE public.logs (id integer NOT NULL, at date NOT NULL) PARTITION BY RANGE (at);
		CREATE TABLE public.logs_2026 PARTITION OF public.logs
			FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
	`)
	cat, err := catalog.NewCatalog(conn, []string{"public"})
	require.NoError(t, err)
	tables, err := cat.Tables(ctx)
	require.NoError(t, err)

	part, ok := tables.GetOk("public.logs_2026")
	require.True(t, ok)
	assert.True(t, part.IsPartitionChild())
	assert.Equal(t, []string{"id", "at"}, part.Columns.CollectKeys())
}
