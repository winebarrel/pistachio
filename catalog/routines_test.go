package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestRoutines(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	newCatalog := func(t *testing.T) *catalog.Catalog {
		t.Helper()
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		return cat
	}

	t.Run("empty", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, "")
		routines, err := newCatalog(t).Routines(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, routines.Len())
	})

	t.Run("single function", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE FUNCTION public.add(a integer, b integer) RETURNS integer
			    LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT a + b $$;
		`)
		routines, err := newCatalog(t).Routines(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, routines.Len())

		r, ok := routines.GetOk("public.add(integer, integer)")
		require.True(t, ok)
		assert.Equal(t, "public", r.Schema)
		assert.Equal(t, "add", r.Name)
		assert.False(t, r.Procedure)
		assert.Equal(t, "integer", r.ReturnType)
		assert.Equal(t, "sql", r.Language)
		assert.Equal(t, " SELECT a + b ", r.Body)
		assert.Equal(t, "IMMUTABLE", r.Volatility)
		assert.True(t, r.Strict)
		assert.NotZero(t, r.OID)
	})

	t.Run("procedure", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE PROCEDURE public.bump(INOUT x integer)
			    LANGUAGE plpgsql AS $$ BEGIN x := x + 1; END $$;
		`)
		routines, err := newCatalog(t).Routines(ctx)
		require.NoError(t, err)

		r, ok := routines.GetOk("public.bump(integer)")
		require.True(t, ok)
		assert.True(t, r.Procedure)
		assert.Equal(t, "PROCEDURE", r.Kind())
	})

	t.Run("function with comment", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
			COMMENT ON FUNCTION public.f() IS 'v1';
		`)
		routines, err := newCatalog(t).Routines(ctx)
		require.NoError(t, err)

		r, ok := routines.GetOk("public.f()")
		require.True(t, ok)
		require.NotNil(t, r.Comment)
		assert.Equal(t, "v1", *r.Comment)
	})

	t.Run("overloads are separate objects", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE FUNCTION public.f(a integer) RETURNS integer LANGUAGE sql AS $$ SELECT a $$;
			CREATE FUNCTION public.f(a text) RETURNS text LANGUAGE sql AS $$ SELECT a $$;
		`)
		routines, err := newCatalog(t).Routines(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, routines.Len())
		assert.Equal(t, []string{"public.f(integer)", "public.f(text)"}, routines.CollectKeys())
	})

	// Aggregates and window functions are not managed, and neither is a
	// routine with a SQL-standard body. The parser leaves out the same ones,
	// so neither side of the diff sees them.
	t.Run("unmanaged routines are left out", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE FUNCTION public.kept() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
			CREATE AGGREGATE public.agg(integer) (SFUNC = int4pl, STYPE = integer);
			CREATE FUNCTION public.atomic() RETURNS integer LANGUAGE sql BEGIN ATOMIC SELECT 1; END;
		`)
		routines, err := newCatalog(t).Routines(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"public.kept()"}, routines.CollectKeys())
	})

	// A routine an extension owns belongs to that extension, not to the
	// desired schema, so it is left out the way every other object type is.
	t.Run("extension routines are left out", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, "")
		if _, err := conn.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS plpgsql"); err != nil {
			t.Skipf("plpgsql extension unavailable: %v", err)
		}
		routines, err := newCatalog(t).Routines(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, routines.Len())
	})

	t.Run("other schemas are left out", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE SCHEMA other;
			CREATE FUNCTION other.f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
			CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
		`)
		defer conn.Exec(ctx, "DROP SCHEMA other CASCADE") //nolint:errcheck

		routines, err := newCatalog(t).Routines(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"public.f()"}, routines.CollectKeys())
	})
}
