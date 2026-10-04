package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestSequences_List(t *testing.T) {
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
		seqMap, err := cat.Sequences(ctx)
		require.NoError(t, err)
		seqs := seqMap.CollectValues()
		assert.Empty(t, seqs)
	})

	t.Run("serial and identity sequences are left out", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id serial NOT NULL,
				code integer NOT NULL GENERATED ALWAYS AS IDENTITY,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqMap, err := cat.Sequences(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, seqMap.Len())
	})

	t.Run("owner table is not qualified outside the search path", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			DROP SCHEMA IF EXISTS seqowner CASCADE;
			CREATE SCHEMA seqowner;
			CREATE SEQUENCE seqowner.s;
			CREATE TABLE seqowner.users (
				id integer
			);
			ALTER SEQUENCE seqowner.s OWNED BY seqowner.users.id;
		`)
		t.Cleanup(func() {
			_, _ = conn.Exec(ctx, "DROP SCHEMA IF EXISTS seqowner CASCADE")
		})
		cat, err := catalog.NewCatalog(conn, []string{"seqowner"})
		require.NoError(t, err)
		seqMap, err := cat.Sequences(ctx)
		require.NoError(t, err)
		seqs := seqMap.CollectValues()
		require.Len(t, seqs, 1)
		require.NotNil(t, seqs[0].OwnerTable)
		assert.Equal(t, "users", *seqs[0].OwnerTable)
	})

	t.Run("standalone sequence has no owner", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE SEQUENCE public.my_seq;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqMap, err := cat.Sequences(ctx)
		require.NoError(t, err)
		seqs := seqMap.CollectValues()
		require.Len(t, seqs, 1)
		assert.Equal(t, "my_seq", seqs[0].Name)
		assert.Nil(t, seqs[0].OwnerTable)
	})

	t.Run("unlogged sequence", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE UNLOGGED SEQUENCE public.jobs_seq;
			CREATE SEQUENCE public.plain_seq;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqMap, err := cat.Sequences(ctx)
		require.NoError(t, err)
		seqs := seqMap.CollectValues()
		require.Len(t, seqs, 2)
		assert.Equal(t, "jobs_seq", seqs[0].Name)
		assert.True(t, seqs[0].Unlogged)
		assert.Equal(t, "plain_seq", seqs[1].Name)
		assert.False(t, seqs[1].Unlogged)
	})

	t.Run("sequence with comment", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE SEQUENCE public.my_seq;
			COMMENT ON SEQUENCE public.my_seq IS 'id generator';
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqMap, err := cat.Sequences(ctx)
		require.NoError(t, err)
		seqs := seqMap.CollectValues()
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
		seqMap, err := cat.Sequences(ctx)
		require.NoError(t, err)
		seqs := seqMap.CollectValues()
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

// TestSequences verifies the map getter leaves out the sequences of serial and
// identity columns and keeps the rest, owned or not.
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

// TestSequences_Owned covers the owned sequences that are not a serial
// column's: under a name serial would not give them, behind a column of a type
// a sequence cannot hold, and owned without drawing the default.
func TestSequences_Owned(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
		CREATE SEQUENCE public.custom_seq;
		CREATE TABLE public.users (
			id bigint DEFAULT nextval('custom_seq') NOT NULL,
			code serial NOT NULL,
			note integer
		);
		ALTER SEQUENCE public.custom_seq OWNED BY public.users.id;
		CREATE TABLE public.retyped (
			id serial NOT NULL
		);
		ALTER TABLE public.retyped ALTER COLUMN id SET DATA TYPE text;
		CREATE SEQUENCE public.plain_seq OWNED BY public.users.note;
	`)
	cat, err := catalog.NewCatalog(conn, []string{"public"})
	require.NoError(t, err)
	seqs, err := cat.Sequences(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"public.custom_seq", "public.plain_seq", "public.retyped_id_seq"}, seqs.CollectKeys())

	seq := seqs.Get("public.custom_seq")
	require.True(t, seq.Owned())
	assert.Equal(t, "users", *seq.OwnerTable)
	assert.Equal(t, "id", *seq.OwnerColumn)
}
