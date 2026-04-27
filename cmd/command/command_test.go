package command_test

import (
	"bytes"
	"context"
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

	assert.Contains(t, buf.String(), "public.users.sql")
	assert.Contains(t, buf.String(), "public.posts.sql")
	assert.NotContains(t, buf.String(), "-- Dump of")
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
	// When the only diff is a -- pist:execute statement (no schema diff),
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
-- pist:execute
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
	// CLI Apply counterpart: only -- pist:execute runs (no schema diff).
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
-- pist:execute
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
	// Plan does not evaluate check SQL — it shows every -- pist:execute
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
-- pist:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')
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
	assert.Contains(t, got, "-- pist:execute", "plan should annotate with the directive")
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
-- pist:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')
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

func TestFmt_Run_WriteError(t *testing.T) {
	tmpDir := t.TempDir()
	readonlyDir := filepath.Join(tmpDir, "readonly")
	require.NoError(t, os.MkdirAll(readonlyDir, 0o755))
	inputFile := filepath.Join(readonlyDir, "test.sql")
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
	assert.Empty(t, buf.String())

	content, err := os.ReadFile(tmpFile)
	require.NoError(t, err)
	assert.Contains(t, string(content), "CREATE TABLE public.users")
	assert.Contains(t, string(content), "    id integer NOT NULL")
}

func TestFmt_Run_Check_Formatted(t *testing.T) {
	// Already formatted file
	tmpFile := filepath.Join(t.TempDir(), "test.sql")
	formatted := "-- public.users\nCREATE TABLE public.users (\n    id integer NOT NULL,\n    CONSTRAINT users_pkey PRIMARY KEY (id)\n);\n"
	require.NoError(t, os.WriteFile(tmpFile, []byte(formatted), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		Schemas: []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Fmt{FmtOptions: pistachio.FmtOptions{Files: []string{tmpFile}, Check: true}}
	err := cmd.Run(client, &buf)
	require.NoError(t, err)
	assert.Empty(t, buf.String())
}

func TestFmt_Run_Check_NotFormatted(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.sql")
	require.NoError(t, os.WriteFile(tmpFile, []byte(`CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		Schemas: []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Fmt{FmtOptions: pistachio.FmtOptions{Files: []string{tmpFile}, Check: true}}
	err := cmd.Run(client, &buf)
	require.Error(t, err)

	var notFormatted *command.ErrNotFormatted
	require.ErrorAs(t, err, &notFormatted)
	assert.Len(t, notFormatted.Files, 1)
	assert.Contains(t, notFormatted.Error(), tmpFile)
	assert.Contains(t, buf.String(), tmpFile)
}
