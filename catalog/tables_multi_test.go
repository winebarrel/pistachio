package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
)

// Columns, constraints and policies are read for every table at once. A table
// must get its own rows and only those.
func TestTables_MultipleTables(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
		CREATE TABLE public.users (
			id integer NOT NULL,
			email text NOT NULL,
			CONSTRAINT users_pkey PRIMARY KEY (id),
			CONSTRAINT users_email_key UNIQUE (email)
		);
		ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;
		CREATE POLICY users_sel ON public.users FOR SELECT USING (email = current_user);

		CREATE TABLE public.posts (
			id bigint NOT NULL,
			user_id integer NOT NULL,
			title text,
			CONSTRAINT posts_pkey PRIMARY KEY (id),
			CONSTRAINT posts_title_check CHECK (title <> ''),
			CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id)
		);
		ALTER TABLE public.posts ENABLE ROW LEVEL SECURITY;
		CREATE POLICY posts_ins ON public.posts FOR INSERT WITH CHECK (title <> '');
		CREATE POLICY posts_sel ON public.posts FOR SELECT USING (true);

		-- No constraints and no policies: this table must stay empty.
		CREATE TABLE public.tags (
			name text
		);
	`)

	cat, err := catalog.NewCatalog(conn, []string{"public"})
	require.NoError(t, err)
	tables, err := cat.Tables(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, tables.Len())

	users := tables.Get("public.users")
	require.NotNil(t, users)
	assert.Equal(t, []string{"id", "email"}, users.Columns.CollectKeys())
	assert.Equal(t, []string{"users_pkey", "users_email_key"}, users.Constraints.CollectKeys())
	assert.Equal(t, 0, users.ForeignKeys.Len())
	assert.Equal(t, []string{"users_sel"}, users.Policies.CollectKeys())

	posts := tables.Get("public.posts")
	require.NotNil(t, posts)
	assert.Equal(t, []string{"id", "user_id", "title"}, posts.Columns.CollectKeys())
	assert.Equal(t, []string{"posts_pkey", "posts_title_check"}, posts.Constraints.CollectKeys())
	assert.Equal(t, []string{"posts_ins", "posts_sel"}, posts.Policies.CollectKeys())

	require.Equal(t, 1, posts.ForeignKeys.Len())
	fk, ok := posts.ForeignKeys.GetOk("posts_user_id_fkey")
	require.True(t, ok)
	// The foreign key carries the table it is declared on.
	assert.Equal(t, "public", fk.Schema)
	assert.Equal(t, "posts", fk.Table)
	assert.Equal(t, []string{"user_id"}, fk.Columns)

	// A policy likewise names its own table.
	sel, ok := posts.Policies.GetOk("posts_sel")
	require.True(t, ok)
	assert.Equal(t, "public", sel.Schema)
	assert.Equal(t, "posts", sel.Table)

	tags := tables.Get("public.tags")
	require.NotNil(t, tags)
	assert.Equal(t, []string{"name"}, tags.Columns.CollectKeys())
	assert.Equal(t, 0, tags.Constraints.Len())
	assert.Equal(t, 0, tags.ForeignKeys.Len())
	assert.Equal(t, 0, tags.Policies.Len())
}

// Two schemas can hold tables of the same name. The readers key on OID, so the
// two must not be mixed up.
func TestTables_SameNameInTwoSchemas(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	defer func() {
		_, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS app CASCADE")
		require.NoError(t, err)
	}()

	testutil.SetupDB(t, ctx, conn, `
		DROP SCHEMA IF EXISTS app CASCADE;
		CREATE SCHEMA app;
		CREATE TABLE public.items (
			id integer NOT NULL,
			CONSTRAINT items_pkey PRIMARY KEY (id)
		);
		CREATE TABLE app.items (
			code text NOT NULL,
			label text,
			CONSTRAINT items_code_key UNIQUE (code)
		);
		ALTER TABLE app.items ENABLE ROW LEVEL SECURITY;
		CREATE POLICY items_sel ON app.items FOR SELECT USING (true);
	`)

	cat, err := catalog.NewCatalog(conn, []string{"public", "app"})
	require.NoError(t, err)
	tables, err := cat.Tables(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, tables.Len())

	pub := tables.Get("public.items")
	require.NotNil(t, pub)
	assert.Equal(t, []string{"id"}, pub.Columns.CollectKeys())
	assert.Equal(t, []string{"items_pkey"}, pub.Constraints.CollectKeys())
	assert.Equal(t, 0, pub.Policies.Len())

	app := tables.Get("app.items")
	require.NotNil(t, app)
	assert.Equal(t, []string{"code", "label"}, app.Columns.CollectKeys())
	assert.Equal(t, []string{"items_code_key"}, app.Constraints.CollectKeys())
	assert.Equal(t, []string{"items_sel"}, app.Policies.CollectKeys())
}
