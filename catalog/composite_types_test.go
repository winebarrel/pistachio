package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestCompositeTypes(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("empty", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, "")
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		cts, err := cat.CompositeTypes(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, cts.Len())
	})

	t.Run("single composite type", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TYPE public.address AS (street text, city text, zip text);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		cts, err := cat.CompositeTypes(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, cts.Len())

		ct, ok := cts.GetOk("public.address")
		require.True(t, ok)
		assert.Equal(t, "address", ct.Name)
		assert.Equal(t, "public", ct.Schema)
		require.Len(t, ct.Attributes, 3)
		assert.Equal(t, "street", ct.Attributes[0].Name)
		assert.Equal(t, "text", ct.Attributes[0].TypeName)
		assert.Equal(t, "zip", ct.Attributes[2].Name)
		assert.Nil(t, ct.Comment)
	})

	t.Run("composite type with comments", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TYPE public.address AS (street text, city text);
			COMMENT ON TYPE public.address IS 'postal address';
			COMMENT ON COLUMN public.address.city IS 'city name';
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		cts, err := cat.CompositeTypes(ctx)
		require.NoError(t, err)

		ct := cts.Get("public.address")
		require.NotNil(t, ct)
		require.NotNil(t, ct.Comment)
		assert.Equal(t, "postal address", *ct.Comment)
		require.Len(t, ct.Attributes, 2)
		require.NotNil(t, ct.Attributes[1].Comment)
		assert.Equal(t, "city name", *ct.Attributes[1].Comment)
	})

	t.Run("table rowtype is not a composite type", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (id int, name text);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		cts, err := cat.CompositeTypes(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, cts.Len())
	})
}
