package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestListConstraintsByTable(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	t.Run("primary key", func(t *testing.T) {
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
		assert.Equal(t, 1, tbl.Constraints.Len())

		con, ok := tbl.Constraints.GetOk("users_pkey")
		require.True(t, ok)
		assert.True(t, con.Type.IsPrimaryKeyConstraint())
		assert.Contains(t, con.Columns, "id")
		assert.True(t, con.Validated)
	})

	t.Run("unique", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				email text NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id),
				CONSTRAINT users_email_key UNIQUE (email)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.users")
		assert.Equal(t, 2, tbl.Constraints.Len())

		con, ok := tbl.Constraints.GetOk("users_email_key")
		require.True(t, ok)
		assert.True(t, con.Type.IsUniqueConstraint())
		assert.Contains(t, con.Columns, "email")
	})

	t.Run("check", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.products (
				id integer NOT NULL,
				price numeric(10,2) NOT NULL,
				CONSTRAINT products_pkey PRIMARY KEY (id),
				CONSTRAINT products_price_check CHECK (price > 0)
			);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.products")
		con, ok := tbl.Constraints.GetOk("products_price_check")
		require.True(t, ok)
		assert.True(t, con.Type.IsCheckConstraint())
		assert.Contains(t, con.Definition, "price")
	})

	t.Run("foreign key", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE TABLE public.posts (
				id integer NOT NULL,
				user_id integer NOT NULL,
				CONSTRAINT posts_pkey PRIMARY KEY (id)
			);
			ALTER TABLE ONLY public.posts ADD CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id);
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.posts")
		assert.Equal(t, 1, tbl.ForeignKeys.Len())

		fk, ok := tbl.ForeignKeys.GetOk("posts_user_id_fkey")
		require.True(t, ok)
		assert.True(t, fk.Type.IsForeignKeyConstraint())
		assert.Equal(t, "posts", fk.Table)
		assert.Equal(t, "public", fk.Schema)
		require.NotNil(t, fk.RefSchema)
		assert.Equal(t, "public", *fk.RefSchema)
		require.NotNil(t, fk.RefTable)
		assert.Equal(t, "users", *fk.RefTable)
		assert.Contains(t, fk.Columns, "user_id")
	})

	t.Run("foreign key with actions", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE TABLE public.posts (
				id integer NOT NULL,
				user_id integer NOT NULL,
				CONSTRAINT posts_pkey PRIMARY KEY (id)
			);
			ALTER TABLE ONLY public.posts ADD CONSTRAINT posts_user_id_fkey
				FOREIGN KEY (user_id) REFERENCES users(id)
				ON UPDATE CASCADE ON DELETE SET NULL;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.posts")
		fk, ok := tbl.ForeignKeys.GetOk("posts_user_id_fkey")
		require.True(t, ok)
		assert.Contains(t, fk.Definition, "ON UPDATE CASCADE")
		assert.Contains(t, fk.Definition, "ON DELETE SET NULL")
	})

	t.Run("deferrable constraint", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
			CREATE TABLE public.users (
				id integer NOT NULL,
				CONSTRAINT users_pkey PRIMARY KEY (id)
			);
			CREATE TABLE public.posts (
				id integer NOT NULL,
				user_id integer NOT NULL,
				CONSTRAINT posts_pkey PRIMARY KEY (id)
			);
			ALTER TABLE ONLY public.posts ADD CONSTRAINT posts_user_id_fkey
				FOREIGN KEY (user_id) REFERENCES users(id)
				DEFERRABLE INITIALLY DEFERRED;
		`)
		cat, err := catalog.NewCatalog(conn, []string{"public"})
		require.NoError(t, err)
		tables, err := cat.Tables(ctx)
		require.NoError(t, err)

		tbl := tables.Get("public.posts")
		fk, ok := tbl.ForeignKeys.GetOk("posts_user_id_fkey")
		require.True(t, ok)
		assert.True(t, fk.Deferrable)
		assert.True(t, fk.Deferred)
	})
}
