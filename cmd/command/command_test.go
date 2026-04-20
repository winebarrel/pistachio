package command_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/cmd/command"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestPlan_Run(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{File: desiredFile}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}

func TestDump_Run(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Dump{}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}

func TestDump_Run_Split(t *testing.T) {
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

	splitDir := filepath.Join(t.TempDir(), "split_output")
	var buf bytes.Buffer
	cmd := &command.Dump{DumpOptions: pistachio.DumpOptions{Split: splitDir}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)

	usersData, err := os.ReadFile(filepath.Join(splitDir, "public.users.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(usersData), "CREATE TABLE public.users")

	postsData, err := os.ReadFile(filepath.Join(splitDir, "public.posts.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(postsData), "CREATE TABLE public.posts")

	assert.Contains(t, buf.String(), "public.users.sql")
	assert.Contains(t, buf.String(), "public.posts.sql")
}

func TestDump_Run_Split_WithView(t *testing.T) {
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

	splitDir := filepath.Join(t.TempDir(), "split_output")
	var buf bytes.Buffer
	cmd := &command.Dump{DumpOptions: pistachio.DumpOptions{Split: splitDir}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)

	usersData, err := os.ReadFile(filepath.Join(splitDir, "public.users.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(usersData), "CREATE TABLE public.users")

	viewData, err := os.ReadFile(filepath.Join(splitDir, "public.active_users.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(viewData), "CREATE OR REPLACE VIEW")
}

func TestDump_Run_Split_Empty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	splitDir := filepath.Join(t.TempDir(), "split_output")
	var buf bytes.Buffer
	cmd := &command.Dump{DumpOptions: pistachio.DumpOptions{Split: splitDir}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)

	entries, err := os.ReadDir(splitDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
	assert.Empty(t, buf.String())
}

func TestDump_Run_Split_SpecialCharacters(t *testing.T) {
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

	splitDir := filepath.Join(t.TempDir(), "split_output")
	var buf bytes.Buffer
	cmd := &command.Dump{DumpOptions: pistachio.DumpOptions{Split: splitDir}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(splitDir, "public.My_Table.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(data), `CREATE TABLE public."My Table"`)
}

func TestPlan_Run_Error(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE t (id int);"), 0o644))

	var buf bytes.Buffer
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{File: desiredFile}}
	err := cmd.Run(ctx, client, &buf)
	require.Error(t, err)
}

func TestDump_Run_Error(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Dump{}
	err := cmd.Run(ctx, client, &buf)
	require.Error(t, err)
}

func TestPlan_Run_NoChanges(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	initSQL := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`
	testutil.SetupDB(t, ctx, conn, initSQL)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(initSQL), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{File: desiredFile}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Equal(t, "No changes\n", buf.String())
}

func TestApply_Run_NoChanges(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	initSQL := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`
	testutil.SetupDB(t, ctx, conn, initSQL)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(initSQL), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{File: desiredFile}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Equal(t, "No changes\n", buf.String())
}

func TestApply_Run_Error(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE t (id int);"), 0o644))

	var buf bytes.Buffer
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{File: desiredFile}}
	err := cmd.Run(ctx, client, &buf)
	require.Error(t, err)
}

func TestApply_Run(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{File: desiredFile}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}
