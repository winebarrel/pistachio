package command_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}}}
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
	assert.Contains(t, buf.String(), "-- Dump of schema public (1 table, 0 views, 0 enums, 0 domains)")
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

	out := buf.String()
	assert.Contains(t, out, "-- Dump of schema public (2 tables, 0 views, 0 enums, 0 domains)")
	assert.Contains(t, out, fmt.Sprintf("-- Wrote 2 file(s) to %s", splitDir))
	assert.NotContains(t, out, "public.users.sql")
	assert.NotContains(t, out, "public.posts.sql")
}

func TestDump_Run_Empty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Dump{}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "-- Dump of schema public (0 tables, 0 views, 0 enums, 0 domains)")
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
	out := buf.String()
	assert.Contains(t, out, "-- Dump of schema public (0 tables, 0 views, 0 enums, 0 domains)")
	assert.Contains(t, out, fmt.Sprintf("-- Wrote 0 file(s) to %s", splitDir))
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
	// MkdirAll runs before the header, so nothing should land on stdout.
	assert.Empty(t, buf.String())
}

func TestDump_Run_Split_WriteError(t *testing.T) {
	// This test forces os.WriteFile to fail by making the split dir read-only.
	// Root bypasses the permission check, so the WriteFile call would succeed
	// and our error-path assertions would spuriously fail. CI containers
	// commonly run as root, so skip there rather than fake-fail.
	if os.Geteuid() == 0 {
		t.Skip("read-only-dir trick does not block root")
	}

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
	// On the write-failure path the header has already been emitted
	// (it precedes the file loop) but the footer must not appear,
	// so partial output is unambiguous from a successful run.
	out := buf.String()
	assert.Contains(t, out, "-- Dump of schema public")
	assert.NotContains(t, out, "-- Wrote")
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
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}}}
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
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "-- Plan for schema public (")
	assert.Contains(t, buf.String(), "-- No changes")
}

func TestPlan_Run_DropDeniedShowsNoChanges(t *testing.T) {
	// When the only diff would be a DROP and --allow-drop is not set,
	// the DROP is emitted as a comment for visibility while the "No changes"
	// message is preserved (since no executable DDL is generated).
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	// Desired schema removes public.users entirely.
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(""), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "-- skipped: DROP TABLE public.users;")
	assert.Contains(t, got, "-- No changes")
	// Plain DROP (no comment) must NOT appear.
	for _, line := range strings.Split(got, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "DROP TABLE") {
			t.Fatalf("unexpected uncommented DROP TABLE: %q", line)
		}
	}
}

func TestPlan_Run_PreSQLBeforeSkippedDrops(t *testing.T) {
	// pre-SQL is prepended to executable SQL and must appear above any
	// "-- skipped:" comments so the output remains a runnable script when
	// piped to psql.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    legacy text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	// Desired removes "legacy" (suppressed by default --allow-drop) and adds
	// "name" so we get both an executable diff and a skipped comment.
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{
		Files:  []string{desiredFile},
		PreSQL: "SELECT 1;",
	}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()

	preSQLPos := strings.Index(got, "SELECT 1;")
	addColPos := strings.Index(got, "ADD COLUMN name")
	skippedPos := strings.Index(got, "-- skipped:")
	require.NotEqual(t, -1, preSQLPos, "pre-SQL should be present")
	require.NotEqual(t, -1, addColPos, "executable diff should be present")
	require.NotEqual(t, -1, skippedPos, "skipped comment should be present")
	assert.Less(t, preSQLPos, addColPos, "pre-SQL must precede executable diff")
	assert.Less(t, addColPos, skippedPos, "skipped comments must follow executable SQL")
}

func TestPlan_Run_ExecuteOnly_NotNoChanges(t *testing.T) {
	// When the only diff is a -- pista:execute statement (no schema diff),
	// the plan must surface the execute SQL and must NOT print "-- No changes".
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	initSQL := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`
	testutil.SetupDB(t, ctx, conn, initSQL)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(initSQL+`
-- pista:execute
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "CREATE OR REPLACE FUNCTION public.test_func")
	assert.NotContains(t, got, "-- No changes")
}

func TestApply_Run_ExecuteOnly_NotNoChanges(t *testing.T) {
	// CLI Apply counterpart: only -- pista:execute runs (no schema diff).
	// "No changes" must not be printed since the function gets emitted and
	// executed.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	initSQL := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`
	testutil.SetupDB(t, ctx, conn, initSQL)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(initSQL+`
-- pista:execute
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "CREATE OR REPLACE FUNCTION public.test_func")
	assert.NotContains(t, got, "-- No changes")

	// Function should actually exist in DB.
	var exists bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')").Scan(&exists))
	assert.True(t, exists)
}

func TestPlan_Run_ExecuteCheckFalse_StillPrintsExecute(t *testing.T) {
	// Plan does not evaluate check SQL — it shows every -- pista:execute
	// statement regardless. So even when the check would return false at
	// apply time, plan output must contain the execute SQL and must NOT
	// say "-- No changes".
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	initSQL := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`
	testutil.SetupDB(t, ctx, conn, initSQL)

	// Check SQL returns false (function already exists), so apply would skip.
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "CREATE OR REPLACE FUNCTION public.test_func", "plan should always show execute SQL")
	assert.Contains(t, got, "-- pista:execute", "plan should annotate with the directive")
	assert.NotContains(t, got, "-- No changes")
}

func TestApply_Run_ExecuteCheckFalse_ShowsNoChanges(t *testing.T) {
	// Apply DOES evaluate check SQL. When it returns false, the execute is
	// skipped (not printed, not executed). With no schema diff either, the
	// buffer is empty so the CLI prints "-- No changes".
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	initSQL := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`
	testutil.SetupDB(t, ctx, conn, initSQL)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.NotContains(t, got, "CREATE OR REPLACE FUNCTION", "skipped execute must not be printed")
	assert.Contains(t, got, "-- No changes", "buffer is empty so CLI prints No changes")
}

func TestApply_Run_DropDeniedShowsNoChanges(t *testing.T) {
	// Apply CLI counterpart of TestPlan_Run_DropDeniedShowsNoChanges.
	// When the only diff is a suppressed DROP, the apply prints the skipped
	// DROP as a comment AND reports "-- No changes" (no DDL was executed).
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(""), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "-- skipped: DROP TABLE public.users;")
	assert.Contains(t, got, "-- No changes")

	// The table must still exist in the database (skip is only emitted as a
	// comment; no DDL is actually executed).
	var n int
	require.NoError(t, conn.QueryRow(ctx, "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'users'").Scan(&n))
	assert.Equal(t, 1, n)
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
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "-- Apply to schema public (")
	assert.Contains(t, buf.String(), "-- No changes")
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
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}}}
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
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}

func TestApply_Run_ExecutedWithSkippedDrops(t *testing.T) {
	// Apply executes some changes AND has suppressed drops: both the executed
	// SQL and the "-- skipped:" comment must appear in the output.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    legacy text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
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
	got := buf.String()

	addColPos := strings.Index(got, "ADD COLUMN name")
	skippedPos := strings.Index(got, "-- skipped: ALTER TABLE public.users DROP COLUMN legacy;")
	require.NotEqual(t, -1, addColPos, "executed DDL should be present")
	require.NotEqual(t, -1, skippedPos, "skipped drop comment should be present")
	// Apply mirrors Plan: executed SQL first, then "-- skipped:" comments.
	assert.Less(t, addColPos, skippedPos, "executed DDL must precede skipped drop comment")
	assert.NotContains(t, got, "-- No changes")
}

// writeDumpFiles is the helper that backs `pista dump --split`. The tests
// below exercise it without a database, focusing on the layered guard that
// requires every dump filename to be a flat basename directly under the
// --split directory: no path separators ("/" or "\"), no ".."/absolute/empty
// paths (filepath.IsLocal), and the name must already be in Clean form so
// the on-disk filename matches the map key.

func TestWriteDumpFiles_HappyPath(t *testing.T) {
	dir := t.TempDir()
	count, err := command.WriteDumpFiles(dir, map[string]string{
		"public.users.sql":    "CREATE TABLE public.users (id integer);\n",
		"public.products.sql": "CREATE TABLE public.products (id integer);\n",
	})
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	got, err := os.ReadFile(filepath.Join(dir, "public.users.sql"))
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE public.users (id integer);\n", string(got))

	got, err = os.ReadFile(filepath.Join(dir, "public.products.sql"))
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE public.products (id integer);\n", string(got))
}

func TestWriteDumpFiles_Empty(t *testing.T) {
	dir := t.TempDir()
	count, err := command.WriteDumpFiles(dir, map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestWriteDumpFiles_RejectsUnsafeNames(t *testing.T) {
	cases := []struct {
		desc string
		name string
	}{
		// Traversal / absolute / empty: rejected by filepath.IsLocal.
		{"parent traversal", "../escape.sql"},
		{"bare dotdot", ".."},
		{"absolute path", "/etc/passwd"},
		{"empty name", ""},
		{"cleaned traversal", "foo/../../escape.sql"},
		{"deep traversal", "a/b/../../../escape.sql"},
		// Non-canonical names: IsLocal would allow these, but Clean
		// reshapes them and would alias other entries on disk.
		{"internal cancel", "foo/../bar.sql"},
		{"leading dot slash", "./foo.sql"},
		{"trailing slash", "foo.sql/"},
		{"redundant slash", "a//b.sql"},
		// Plain nested paths: IsLocal *does* accept these, and they
		// could be redirected outside dir via a pre-existing symlink.
		// The explicit separator check refuses them.
		{"forward separator", "subdir/file.sql"},
		{"backslash separator", `subdir\file.sql`},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			// Build the split dir under a controlled parent so we can
			// verify both that the split dir stays empty AND that the
			// parent contains only the split dir — i.e. no file slipped
			// out via traversal.
			parent := t.TempDir()
			dir := filepath.Join(parent, "split")
			require.NoError(t, os.MkdirAll(dir, 0o755))

			count, err := command.WriteDumpFiles(dir, map[string]string{tc.name: "x"})
			require.Error(t, err)
			assert.Equal(t, 0, count)
			assert.Contains(t, err.Error(), "unsafe name")
			assert.Contains(t, err.Error(), "--split")

			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			assert.Empty(t, entries, "split dir must remain empty")

			parentEntries, err := os.ReadDir(parent)
			require.NoError(t, err)
			require.Len(t, parentEntries, 1, "no file should escape into the parent dir")
			assert.Equal(t, "split", parentEntries[0].Name())
		})
	}
}

func TestWriteDumpFiles_AllowsLiteralDotsInName(t *testing.T) {
	// PostgreSQL identifiers may legitimately contain dots in the middle
	// (quoted names like "v1.0"). These must pass IsLocal because they
	// don't escape the target directory.
	dir := t.TempDir()
	files := map[string]string{
		"..leading.sql":   "x\n",
		"foo..bar.sql":    "y\n",
		"public.v1.0.sql": "z\n",
	}
	count, err := command.WriteDumpFiles(dir, files)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
	for name := range files {
		_, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err, "expected %q to be written", name)
	}
}

func TestWriteDumpFiles_PreservesLiteralLeadingDots(t *testing.T) {
	// Sanity check that the canonical-form guard does not over-restrict:
	// names like "..foo.sql" or "foo..bar.sql" are already in Clean form
	// (the ".."s are part of the filename, not path elements) and must
	// still be written through.
	dir := t.TempDir()
	files := map[string]string{
		"..foo.sql":    "a\n",
		"foo..bar.sql": "b\n",
	}
	count, err := command.WriteDumpFiles(dir, files)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	for name := range files {
		_, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err, "expected %q to be written", name)
	}
}

func TestWriteDumpFiles_WriteFileError(t *testing.T) {
	// Skip when running as root: a read-only directory still permits writes
	// for uid 0 on Linux, so the failure path we're trying to hit cannot
	// be triggered. The other CI hosts run as a regular user and exercise
	// this branch.
	if os.Geteuid() == 0 {
		t.Skip("read-only dir trick does not block root")
	}
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	count, err := command.WriteDumpFiles(dir, map[string]string{"safe.sql": "x"})
	require.Error(t, err)
	assert.Equal(t, 0, count)
	assert.Contains(t, err.Error(), "failed to write")
}

func TestWriteDumpFiles_StopsOnUnsafeName(t *testing.T) {
	// The check is per-iteration; when an unsafe name is encountered the
	// loop returns immediately. Map iteration order is unspecified, so we
	// can only assert that the unsafe file was not written and that the
	// returned count matches the number of files actually written before
	// the abort.
	//
	// Use a self-controlled parent directory so the negative assertion
	// (no "escape.sql" was written above the split dir) is not confused
	// by unrelated files in the shared OS temp directory.
	parent := t.TempDir()
	dir := filepath.Join(parent, "split")
	require.NoError(t, os.MkdirAll(dir, 0o755))

	count, err := command.WriteDumpFiles(dir, map[string]string{
		"safe.sql":      "ok\n",
		"../escape.sql": "bad\n",
	})
	require.Error(t, err)
	assert.LessOrEqual(t, count, 1, "at most the safe file may have been written")

	_, err = os.Stat(filepath.Join(parent, "escape.sql"))
	assert.True(t, os.IsNotExist(err), "escape.sql must not appear next to split dir")
}
