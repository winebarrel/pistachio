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
