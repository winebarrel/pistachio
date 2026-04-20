package pistachio_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
)

type dumpTestCase struct {
	Init string `yaml:"init"`
	Dump string `yaml:"dump"`
}

func TestDump_InvalidConnString(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	_, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.Error(t, err)
}

func TestDump_UnreachableHost(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "postgres://postgres@192.0.2.1:5432/postgres?connect_timeout=1",
		Schemas:    []string{"public"},
	})

	_, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect")
}

func TestDump_EmptySchemas(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{},
	})

	_, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.Error(t, err)
}

func TestDump_CanceledContext(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	_, err := client.Dump(canceledCtx, &pistachio.DumpOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch tables")
}

func TestDumpResult_String_Empty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	assert.Equal(t, "", got.String())
}

func TestDumpResult_Files_Tables(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Len(t, files, 2)
	assert.Contains(t, files, "public.users.sql")
	assert.Contains(t, files, "public.posts.sql")
	assert.Contains(t, files["public.users.sql"], "CREATE TABLE public.users")
	assert.Contains(t, files["public.posts.sql"], "CREATE TABLE public.posts")
}

func TestDumpResult_Files_Views(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Len(t, files, 2)
	assert.Contains(t, files, "public.users.sql")
	assert.Contains(t, files, "public.active_users.sql")
	assert.Contains(t, files["public.active_users.sql"], "CREATE OR REPLACE VIEW")
}

func TestDumpResult_Files_Empty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Empty(t, files)
}

func TestDumpResult_Files_SpecialCharacters(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public."My Table" (
    id integer NOT NULL
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "public.My_Table.sql")
}

func TestDumpResult_OmitSchema_String(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "CREATE TABLE users")
	assert.NotContains(t, s, "public.users")
	assert.Contains(t, s, "CREATE OR REPLACE VIEW active_users")
	assert.NotContains(t, s, "public.active_users")
}

func TestDumpResult_OmitSchema_Files(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	files := got.Files()
	assert.Contains(t, files, "users.sql")
	assert.NotContains(t, files, "public.users.sql")
	assert.Contains(t, files["users.sql"], "CREATE TABLE users")
}

func TestDumpResult_OmitSchema_ForeignKey(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
ALTER TABLE public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "ALTER TABLE ONLY orders")
	assert.NotContains(t, s, "public.orders")
}

func TestDumpResult_OmitSchema_Comment(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.users IS 'User accounts';`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "COMMENT ON TABLE users")
	assert.NotContains(t, s, "public.users")
}

func TestDump_OmitSchema_MultipleSchemas(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "postgres://postgres@localhost/postgres",
		Schemas:    []string{"public", "other"},
	})

	_, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--omit-schema cannot be used with multiple schemas")
}

func TestDumpResult_OmitSchema_Index(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_id ON public.users (id);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{OmitSchema: true})
	require.NoError(t, err)
	s := got.String()
	assert.Contains(t, s, "CREATE INDEX idx_users_id ON users")
	assert.NotContains(t, s, "ON public.users")
}

func TestDump(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	files, err := filepath.Glob("testdata/dump/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")
		t.Run(name, func(t *testing.T) {
			tc := loadYAML[dumpTestCase](t, file)
			testutil.SetupDB(t, ctx, conn, tc.Init)
			client := pistachio.NewClient(&pistachio.Options{
				ConnString: conn.Config().ConnString(),
				Schemas:    []string{"public"},
			})
			got, err := client.Dump(ctx, &pistachio.DumpOptions{})
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.Dump), strings.TrimSpace(got.String()))
		})
	}
}
