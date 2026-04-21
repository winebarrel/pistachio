package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestEnums(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("empty", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, "")
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		enums, err := cat.Enums(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, enums.Len())
	})

	t.Run("single enum", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TYPE public.status AS ENUM ('active', 'inactive', 'pending');
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		enums, err := cat.Enums(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, enums.Len())

		e, ok := enums.GetOk("public.status")
		require.True(t, ok)
		assert.Equal(t, "status", e.Name)
		assert.Equal(t, "public", e.Schema)
		assert.Equal(t, []string{"active", "inactive", "pending"}, e.Values)
		assert.Nil(t, e.Comment)
	})

	t.Run("enum with comment", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TYPE public.status AS ENUM ('active', 'inactive');
			COMMENT ON TYPE public.status IS 'User status';
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		enums, err := cat.Enums(ctx)
		require.NoError(t, err)

		e := enums.Get("public.status")
		require.NotNil(t, e)
		require.NotNil(t, e.Comment)
		assert.Equal(t, "User status", *e.Comment)
	})

	t.Run("multiple enums", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TYPE public.status AS ENUM ('active', 'inactive');
			CREATE TYPE public.role AS ENUM ('admin', 'user', 'guest');
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		enums, err := cat.Enums(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, enums.Len())

		_, ok := enums.GetOk("public.role")
		assert.True(t, ok)
		_, ok = enums.GetOk("public.status")
		assert.True(t, ok)
	})
}
