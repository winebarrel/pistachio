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
