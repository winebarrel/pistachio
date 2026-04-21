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
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
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

func TestDump_Run_Split_MkdirError(t *testing.T) {
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

	var buf bytes.Buffer
	cmd := &command.Dump{DumpOptions: pistachio.DumpOptions{Split: "/dev/null/invalid"}}
	err := cmd.Run(ctx, client, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create directory")
}

func TestDump_Run_Split_WriteError(t *testing.T) {
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

	splitDir := filepath.Join(t.TempDir(), "split_output")
	require.NoError(t, os.MkdirAll(splitDir, 0o555))

	var buf bytes.Buffer
	cmd := &command.Dump{DumpOptions: pistachio.DumpOptions{Split: splitDir}}
	err := cmd.Run(ctx, client, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write")
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
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
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
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Equal(t, "-- No changes\n", buf.String())
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
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Equal(t, "-- No changes\n", buf.String())
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
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{Files: []string{desiredFile}}}
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
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}

func TestFmt_Run(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.sql")
	require.NoError(t, os.WriteFile(tmpFile, []byte(`CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		Schemas: []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Fmt{FmtOptions: pistachio.FmtOptions{Files: []string{tmpFile}}}
	err := cmd.Run(client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
	assert.Contains(t, buf.String(), "    id integer NOT NULL")
}

func TestFmt_Run_InvalidFile(t *testing.T) {
	client := pistachio.NewClient(&pistachio.Options{
		Schemas: []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Fmt{FmtOptions: pistachio.FmtOptions{Files: []string{"/nonexistent/file.sql"}}}
	err := cmd.Run(client, &buf)
	require.Error(t, err)
}

func TestFmt_Run_Write_InvalidFile(t *testing.T) {
	client := pistachio.NewClient(&pistachio.Options{
		Schemas: []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Fmt{FmtOptions: pistachio.FmtOptions{Files: []string{"/nonexistent/file.sql"}, Write: true}}
	err := cmd.Run(client, &buf)
	require.Error(t, err)
}

func TestFmt_Run_WriteError(t *testing.T) {
	// Create a read-only directory so WriteFile fails
	tmpDir := t.TempDir()
	readonlyDir := filepath.Join(tmpDir, "readonly")
	require.NoError(t, os.MkdirAll(readonlyDir, 0o555))

	// Input file must be readable but the write target must fail.
	// Since -w writes back to the same files, put a readable file in a read-only dir.
	inputFile := filepath.Join(readonlyDir, "test.sql")
	// Write before making it read-only won't work since dir is already 0o555.
	// Instead: create file first, then make dir read-only.
	require.NoError(t, os.Chmod(readonlyDir, 0o755))
	require.NoError(t, os.WriteFile(inputFile, []byte(`CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));`), 0o444))
	require.NoError(t, os.Chmod(readonlyDir, 0o555))
	t.Cleanup(func() { os.Chmod(readonlyDir, 0o755) })

	client := pistachio.NewClient(&pistachio.Options{
		Schemas: []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Fmt{FmtOptions: pistachio.FmtOptions{Files: []string{inputFile}, Write: true}}
	err := cmd.Run(client, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write")
}

func TestFmt_Run_Write(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.sql")
	require.NoError(t, os.WriteFile(tmpFile, []byte(`CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		Schemas: []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Fmt{FmtOptions: pistachio.FmtOptions{Files: []string{tmpFile}, Write: true}}
	err := cmd.Run(client, &buf)
	require.NoError(t, err)

	// stdout should be empty when writing to file
	assert.Empty(t, buf.String())

	// File should be formatted
	content, err := os.ReadFile(tmpFile)
	require.NoError(t, err)
	assert.Contains(t, string(content), "CREATE TABLE public.users")
	assert.Contains(t, string(content), "    id integer NOT NULL")
}
