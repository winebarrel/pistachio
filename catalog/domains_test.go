package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestDomains(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("empty", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, "")
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		domains, err := cat.Domains(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, domains.Len())
	})

	t.Run("simple domain", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		domains, err := cat.Domains(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, domains.Len())

		d, ok := domains.GetOk("public.pos_int")
		require.True(t, ok)
		assert.Equal(t, "pos_int", d.Name)
		assert.Equal(t, "public", d.Schema)
		assert.Equal(t, "integer", d.BaseType)
		assert.Len(t, d.Constraints, 1)
		assert.Equal(t, "pos_check", d.Constraints[0].Name)
	})

	t.Run("domain with default and not null", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE DOMAIN public.email AS varchar(255) NOT NULL DEFAULT ''::varchar;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		domains, err := cat.Domains(ctx)
		require.NoError(t, err)

		d := domains.Get("public.email")
		require.NotNil(t, d)
		assert.True(t, d.NotNull)
		require.NotNil(t, d.Default)
	})

	t.Run("domain with comment", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE DOMAIN public.pos_int AS integer;
			COMMENT ON DOMAIN public.pos_int IS 'Positive integer';
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		domains, err := cat.Domains(ctx)
		require.NoError(t, err)

		d := domains.Get("public.pos_int")
		require.NotNil(t, d)
		require.NotNil(t, d.Comment)
		assert.Equal(t, "Positive integer", *d.Comment)
	})

	// Constraints are read for every domain at once. A domain must get its own
	// and only its own, in conname order, and one without constraints must stay
	// without them.
	t.Run("multiple domains", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE DOMAIN public.pos_int AS integer
				CONSTRAINT pos_int_max CHECK (VALUE < 100)
				CONSTRAINT pos_int_min CHECK (VALUE > 0);
			CREATE DOMAIN public.lower_text AS text
				CONSTRAINT lower_text_case CHECK (VALUE = lower(VALUE));
			CREATE DOMAIN public.plain AS text;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		domains, err := cat.Domains(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, domains.Len())

		posInt := domains.Get("public.pos_int")
		require.NotNil(t, posInt)
		require.Len(t, posInt.Constraints, 2)
		assert.Equal(t, "pos_int_max", posInt.Constraints[0].Name)
		assert.Equal(t, "pos_int_min", posInt.Constraints[1].Name)
		assert.Contains(t, posInt.Constraints[1].Definition, "> 0")

		lowerText := domains.Get("public.lower_text")
		require.NotNil(t, lowerText)
		require.Len(t, lowerText.Constraints, 1)
		assert.Equal(t, "lower_text_case", lowerText.Constraints[0].Name)

		plain := domains.Get("public.plain")
		require.NotNil(t, plain)
		assert.Empty(t, plain.Constraints)
	})
}
