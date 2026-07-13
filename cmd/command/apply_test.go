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
	assert.Contains(t, buf.String(), "-- Apply finished in ")
	assertConnectedCommentFirst(t, buf.String(), conn.Config())
}

func TestApply_Run_IgnoredComment(t *testing.T) {
	// apply reports an ignored object as an -- ignored: comment and leaves it
	// untouched while applying the managed change.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.legacy (
    id integer NOT NULL,
    name text,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
CREATE TABLE public.users (
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
	got := buf.String()
	assert.Contains(t, got, "CREATE TABLE public.users")
	assert.Contains(t, got, "-- ignored: public.legacy")

	// The ignored table keeps its extra column.
	var hasName bool
	require.NoError(t, conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='legacy' AND column_name='name')`).Scan(&hasName))
	assert.True(t, hasName, "ignored table's column must survive")
}

func TestApply_Run_IgnoredOnlyShowsNoChanges(t *testing.T) {
	// When the only object is ignored, apply prints the -- ignored: comment
	// and reports no changes.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.legacy (
    id integer NOT NULL,
    name text,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
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
	assert.Contains(t, got, "-- ignored: public.legacy")
	assert.Contains(t, got, "-- No changes")
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

func TestApply_Run_WithTx_FlushesBufferOnError(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	tmpDir := t.TempDir()
	desiredFile := filepath.Join(tmpDir, "desired.sql")
	preSQLFile := filepath.Join(tmpDir, "pre.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))
	require.NoError(t, os.WriteFile(preSQLFile, []byte(`SELECT * FROM public.missing_table;`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{
		Files:      []string{desiredFile},
		PreSQLFile: preSQLFile,
		WithTx:     true,
	}}
	err := cmd.Run(ctx, client, &buf)
	require.Error(t, err)

	out := buf.String()
	assert.Contains(t, out, "-- Transaction started")
	assert.Contains(t, out, "-- Transaction rolled back")
	assert.NotContains(t, out, "-- Transaction committed")
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
	// No timing line when nothing was applied.
	assert.NotContains(t, buf.String(), "-- Apply finished in ")
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
	// The check SQL ran but nothing was applied, so no timing line is printed.
	assert.NotContains(t, got, "-- Apply finished in ")
}

func TestApply_Run_WithTxExecuteCheckFalse_ShowsNoChanges(t *testing.T) {
	// --with-tx variant of the case above. The transaction comments fill the
	// output buffer, but no schema change is applied (the only -- pista:execute
	// is skipped by its check SQL). The CLI must still print "-- No changes"
	// and omit the timing line, driven by result.Applied rather than the
	// buffer length.
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
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{Files: []string{desiredFile}, WithTx: true}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "-- Transaction started")
	assert.Contains(t, got, "-- Transaction committed")
	assert.NotContains(t, got, "CREATE OR REPLACE FUNCTION", "skipped execute must not be printed")
	assert.Contains(t, got, "-- No changes", "nothing was applied, so No changes is printed even with --with-tx")
	assert.NotContains(t, got, "-- Apply finished in ")
}

func TestApply_Run_PreSQLOnly_ShowsNoChanges(t *testing.T) {
	// pre-SQL is a setup step, not a schema change. With no schema diff and the
	// only -- pista:execute skipped by its check SQL, the pre-SQL still runs and
	// is printed, but the apply reports "-- No changes" and omits the timing
	// line (pre-SQL does not count as applied).
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
	cmd := &command.Apply{ApplyOptions: pistachio.ApplyOptions{Files: []string{desiredFile}, PreSQL: "SET statement_timeout = '5s'"}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "SET statement_timeout", "pre-SQL still runs and is printed")
	assert.NotContains(t, got, "CREATE OR REPLACE FUNCTION", "skipped execute must not be printed")
	assert.Contains(t, got, "-- No changes", "pre-SQL is not a schema change, so No changes is printed")
	assert.NotContains(t, got, "-- Apply finished in ")
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
