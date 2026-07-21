package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestListSequences(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("no sequences", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqs, err := cat.ListSequences(ctx)
		require.NoError(t, err)
		assert.Empty(t, seqs)
	})

	t.Run("serial sequence has owner", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id serial NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqs, err := cat.ListSequences(ctx)
		require.NoError(t, err)
		require.Len(t, seqs, 1)
		assert.Equal(t, "users_id_seq", seqs[0].Name)
		require.NotNil(t, seqs[0].OwnerTable)
		assert.Contains(t, *seqs[0].OwnerTable, "users")
		require.NotNil(t, seqs[0].OwnerColumn)
		assert.Equal(t, "id", *seqs[0].OwnerColumn)
	})

	t.Run("identity sequence has owner", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqs, err := cat.ListSequences(ctx)
		require.NoError(t, err)
		require.Len(t, seqs, 1)
		require.NotNil(t, seqs[0].OwnerTable)
		assert.Contains(t, *seqs[0].OwnerTable, "users")
		require.NotNil(t, seqs[0].OwnerColumn)
		assert.Equal(t, "id", *seqs[0].OwnerColumn)
	})

	t.Run("standalone sequence has no owner", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE SEQUENCE public.my_seq;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqs, err := cat.ListSequences(ctx)
		require.NoError(t, err)
		require.Len(t, seqs, 1)
		assert.Equal(t, "my_seq", seqs[0].Name)
		assert.Nil(t, seqs[0].OwnerTable)
	})

	t.Run("sequence with comment", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE SEQUENCE public.my_seq;
			COMMENT ON SEQUENCE public.my_seq IS 'id generator';
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqs, err := cat.ListSequences(ctx)
		require.NoError(t, err)
		require.Len(t, seqs, 1)
		require.NotNil(t, seqs[0].Comment)
		assert.Equal(t, "id generator", *seqs[0].Comment)
	})

	t.Run("sequence with options", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE SEQUENCE public.custom_seq
				INCREMENT BY 5
				START WITH 100
				MINVALUE 1
				MAXVALUE 10000
				CACHE 10
				CYCLE;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqs, err := cat.ListSequences(ctx)
		require.NoError(t, err)
		require.Len(t, seqs, 1)

		seq := seqs[0]
		assert.Equal(t, "custom_seq", seq.Name)
		assert.Equal(t, int64(5), seq.Increment)
		assert.Equal(t, int64(100), seq.Start)
		assert.Equal(t, int64(1), seq.Min)
		assert.Equal(t, int64(10000), seq.Max)
		assert.Equal(t, int64(10), seq.Cache)
		assert.True(t, seq.Cycle)
	})
}

// TestSequences verifies the map getter returns only standalone sequences,
// excluding serial/identity-owned ones.
func TestSequences(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
		CREATE TABLE public.users (
			id serial NOT NULL,
			code integer NOT NULL GENERATED ALWAYS AS IDENTITY,
			CONSTRAINT users_pkey PRIMARY KEY (id)
		);
		CREATE SEQUENCE public.standalone_seq;
	`)
	cat, err := catalog.NewCatalog(conn, []string{"public"})
	require.NoError(t, err)
	seqs, err := cat.Sequences(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, seqs.Len())
	seq, ok := seqs.GetOk("public.standalone_seq")
	require.True(t, ok)
	assert.Equal(t, "standalone_seq", seq.Name)
	assert.Nil(t, seq.OwnerTable)
}
