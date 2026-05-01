package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestListPoliciesByTable(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("simple SELECT policy", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.documents (
				id bigint NOT NULL,
				owner text NOT NULL,
				CONSTRAINT documents_pkey PRIMARY KEY (id)
			);
			ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
			CREATE POLICY owner_select ON public.documents FOR SELECT USING (owner = current_user);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.documents")
		require.True(t, tbl.RowSecurity)
		assert.False(t, tbl.ForceRowSecurity)
		assert.Equal(t, 1, tbl.Policies.Len())

		pol, ok := tbl.Policies.GetOk("owner_select")
		require.True(t, ok)
		assert.True(t, pol.Permissive)
		assert.Equal(t, "SELECT", pol.Command.String())
		assert.Equal(t, []string{"public"}, pol.Roles)
		require.NotNil(t, pol.Using)
		assert.Contains(t, *pol.Using, "owner")
		assert.Nil(t, pol.WithCheck)
	})

	t.Run("FORCE row security", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.documents (
				id bigint NOT NULL,
				CONSTRAINT documents_pkey PRIMARY KEY (id)
			);
			ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
			ALTER TABLE public.documents FORCE ROW LEVEL SECURITY;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.documents")
		assert.True(t, tbl.RowSecurity)
		assert.True(t, tbl.ForceRowSecurity)
		assert.Equal(t, 0, tbl.Policies.Len())
	})

	t.Run("RESTRICTIVE policy", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.documents (
				id bigint NOT NULL,
				owner text NOT NULL,
				CONSTRAINT documents_pkey PRIMARY KEY (id)
			);
			ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
			CREATE POLICY p ON public.documents AS RESTRICTIVE FOR ALL USING (owner = current_user);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		pol, _ := tables.Get("public.documents").Policies.GetOk("p")
		assert.False(t, pol.Permissive)
		assert.True(t, pol.Command.IsAll())
	})

	t.Run("INSERT policy with WITH CHECK", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.documents (
				id bigint NOT NULL,
				owner text NOT NULL,
				CONSTRAINT documents_pkey PRIMARY KEY (id)
			);
			ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
			CREATE POLICY p ON public.documents FOR INSERT WITH CHECK (owner = current_user);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		pol, _ := tables.Get("public.documents").Policies.GetOk("p")
		assert.Equal(t, "INSERT", pol.Command.String())
		assert.Nil(t, pol.Using)
		require.NotNil(t, pol.WithCheck)
		assert.Contains(t, *pol.WithCheck, "owner")
	})

	// pg_policy.polroles uses OID 0 to represent PUBLIC. The catalog query
	// must merge OID 0 with named-role rows from pg_roles (which has no row
	// for OID 0), and sort the result. Multiple named roles exercise the
	// array_agg ORDER BY path. Note: PostgreSQL itself collapses
	// "TO PUBLIC, named_role" to "TO PUBLIC" with a WARNING ("ignoring
	// specified roles other than PUBLIC"), so the mixed-PUBLIC case is not
	// reachable from real input.
	t.Run("policy with multiple named roles", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.documents (
				id bigint NOT NULL,
				owner text NOT NULL,
				CONSTRAINT documents_pkey PRIMARY KEY (id)
			);
			ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
			CREATE POLICY p ON public.documents FOR SELECT TO postgres, pg_read_all_data USING (owner = current_user);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		pol, _ := tables.Get("public.documents").Policies.GetOk("p")
		assert.Equal(t, []string{"pg_read_all_data", "postgres"}, pol.Roles,
			"named roles should be preserved and sorted alphabetically")
	})

	t.Run("policy with named role", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.documents (
				id bigint NOT NULL,
				owner text NOT NULL,
				CONSTRAINT documents_pkey PRIMARY KEY (id)
			);
			ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
			CREATE POLICY p ON public.documents FOR SELECT TO postgres USING (owner = current_user);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		pol, _ := tables.Get("public.documents").Policies.GetOk("p")
		assert.Equal(t, []string{"postgres"}, pol.Roles)
	})

	t.Run("multiple policies sorted by name", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.documents (
				id bigint NOT NULL,
				owner text NOT NULL,
				CONSTRAINT documents_pkey PRIMARY KEY (id)
			);
			ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
			CREATE POLICY zeta ON public.documents FOR DELETE USING (owner = current_user);
			CREATE POLICY alpha ON public.documents FOR SELECT USING (owner = current_user);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.documents")
		var names []string
		for name := range tbl.Policies.Keys() {
			names = append(names, name)
		}
		assert.Equal(t, []string{"alpha", "zeta"}, names)
	})

	t.Run("table without policies", func(t *testing.T) {
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

		tbl := tables.Get("public.users")
		assert.False(t, tbl.RowSecurity)
		assert.Equal(t, 0, tbl.Policies.Len())
	})
}
