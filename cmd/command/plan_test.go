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
	assertConnectedCommentFirst(t, buf.String(), conn.Config())
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

func TestPlan_Run_IgnoredWithChange(t *testing.T) {
	// An ignored object is reported as an -- ignored: comment alongside the
	// executable diff for the managed objects.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.legacy (
    id integer NOT NULL,
    name text,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
CREATE TABLE public.users (
    id integer NOT NULL,
    email text,
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
	got := buf.String()
	assert.Contains(t, got, "ADD COLUMN email")
	assert.Contains(t, got, "-- ignored: public.legacy")
}

func TestPlan_Run_IgnoredOnlyShowsNoChanges(t *testing.T) {
	// When the only object is ignored, the plan prints the -- ignored:
	// comment and still reports no changes.
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
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "-- ignored: public.legacy")
	assert.Contains(t, got, "-- No changes")
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

func TestPlan_Run_CheckWithDiff(t *testing.T) {
	// --check returns ErrPlanDiff when the plan contains executable DDL.
	// The normal plan output is still written before the error is returned.
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
	cmd := &command.Plan{Check: true, PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.ErrorIs(t, err, command.ErrPlanDiff)
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}

func TestPlan_Run_CheckNoChanges(t *testing.T) {
	// --check returns nil when the schema is already in the desired state.
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
	cmd := &command.Plan{Check: true, PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "-- No changes")
}

func TestPlan_Run_CheckSkippedDropOnly(t *testing.T) {
	// Suppressed drops produce no executable DDL, so --check treats them as
	// no changes (consistent with the "-- No changes" output).
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
	cmd := &command.Plan{Check: true, PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "-- skipped: DROP TABLE public.users;")
	assert.Contains(t, got, "-- No changes")
}

func TestPlan_Run_CheckExecuteOnly(t *testing.T) {
	// A -- pista:execute statement is executable SQL, so --check returns
	// ErrPlanDiff even without a schema diff.
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
	cmd := &command.Plan{Check: true, PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.ErrorIs(t, err, command.ErrPlanDiff)
}

func TestPlan_Run_CheckExecuteOnlyCheckFalse(t *testing.T) {
	// A -- pista:execute statement whose check is false is not executable,
	// so --check reports no diff rather than ErrPlanDiff.
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
-- pista:execute SELECT false
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Plan{Check: true, PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	require.NoError(t, cmd.Run(ctx, client, &buf))
	assert.Contains(t, buf.String(), "-- No changes")
}

func TestPlan_Run_ExecuteCheckUnevaluable_KeepsStatement(t *testing.T) {
	// A check that references a table the same run creates cannot be answered
	// before the managed DDL. plan keeps the statement, records why, and still
	// shows the unrelated DDL rather than failing the whole command.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM public.audit_log)
INSERT INTO public.audit_log (id, note) VALUES (1, 'seed');
CREATE TABLE public.audit_log (
    id integer NOT NULL,
    note text,
    CONSTRAINT audit_log_pkey PRIMARY KEY (id)
);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Plan{Check: true, PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	// Undetermined counts as a pending change, so --check still reports a diff.
	require.ErrorIs(t, cmd.Run(ctx, client, &buf), command.ErrPlanDiff)

	got := buf.String()
	assert.Contains(t, got, "CREATE TABLE public.audit_log", "unrelated DDL must still appear")
	assert.Contains(t, got, "INSERT INTO public.audit_log")
	assert.Contains(t, got, "check SQL could not be evaluated at plan time")
	assert.Contains(t, got, "apply will decide")
}

func TestPlan_Run_ExecuteCheckWrites_KeepsStatement(t *testing.T) {
	// plan holds a read-only connection, so a check that writes cannot be
	// evaluated there. That is a property of plan, not of the schema file, so
	// it degrades the same way.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "CREATE SEQUENCE public.s;")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE SEQUENCE public.s;
-- pista:execute SELECT nextval('public.s') > 0
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	cmd := &command.Plan{PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	require.NoError(t, cmd.Run(ctx, client, &buf))

	got := buf.String()
	assert.Contains(t, got, "CREATE OR REPLACE FUNCTION public.test_func")
	assert.Contains(t, got, "read-only transaction")
}

func TestPlan_Run_CheckPreSQLNoChanges(t *testing.T) {
	// Pre-SQL is prepended only when the plan has statements, so it does
	// not turn an empty plan into a diff.
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
	cmd := &command.Plan{Check: true, PlanOptions: pistachio.PlanOptions{
		Files:  []string{desiredFile},
		PreSQL: "SELECT 1;",
	}}
	err := cmd.Run(ctx, client, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "-- No changes")
	assert.NotContains(t, got, "SELECT 1;")
}

func TestPlan_Run_CheckError(t *testing.T) {
	// Connection failures must surface as ordinary errors, not ErrPlanDiff.
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE t (id int);"), 0o644))

	var buf bytes.Buffer
	cmd := &command.Plan{Check: true, PlanOptions: pistachio.PlanOptions{Files: []string{desiredFile}}}
	err := cmd.Run(ctx, client, &buf)
	require.Error(t, err)
	require.NotErrorIs(t, err, command.ErrPlanDiff)
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

func TestPlan_Run_ExecuteCheckFalse_OmitsExecute(t *testing.T) {
	// Plan evaluates check SQL and leaves out the statements apply would
	// skip. A check that returns false therefore produces "-- No changes"
	// rather than SQL that would never run.
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
	assert.NotContains(t, got, "CREATE OR REPLACE FUNCTION public.test_func", "plan should omit execute SQL apply would skip")
	assert.NotContains(t, got, "-- pista:execute")
	assert.Contains(t, got, "-- No changes")
}
