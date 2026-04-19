package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestSequences(t *testing.T) {
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
		seqs, err := cat.Sequences(ctx)
		require.NoError(t, err)
		assert.Empty(t, seqs)
	})

	t.Run("serial sequence", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id serial NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqs, err := cat.Sequences(ctx)
		require.NoError(t, err)
		require.Len(t, seqs, 1)
		assert.Equal(t, "users_id_seq", seqs[0].Name)
		require.NotNil(t, seqs[0].OwnerTable)
		assert.Contains(t, *seqs[0].OwnerTable, "users")
		require.NotNil(t, seqs[0].OwnerColumn)
		assert.Equal(t, "id", *seqs[0].OwnerColumn)
	})

	t.Run("standalone sequence", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE SEQUENCE public.my_seq;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		seqs, err := cat.Sequences(ctx)
		require.NoError(t, err)
		require.Len(t, seqs, 1)
		assert.Equal(t, "my_seq", seqs[0].Name)
		assert.Equal(t, "public", seqs[0].Schema)
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
		seqs, err := cat.Sequences(ctx)
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
