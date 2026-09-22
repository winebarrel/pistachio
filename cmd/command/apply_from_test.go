package command_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/cmd/command"
	"github.com/winebarrel/pistachio/internal/testutil"
)

// planTo writes a plan file for the given desired schema and returns its path.
// allowDrop is the drop policy the plan runs under, so a test can leave a drop
// suppressed in the file.
func planTo(t *testing.T, ctx context.Context, connString, desiredSQL string, allowDrop ...string) string {
	t.Helper()
	dir := t.TempDir()
	desiredFile := filepath.Join(dir, "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(desiredSQL), 0o644))
	out := filepath.Join(dir, "plan.json")

	plan := &command.Plan{
		ConnString: connString, Schemas: []string{"public"},
		AllowDrop: allowDrop,
		Files:     []string{desiredFile},
		Out:       out,
	}
	require.NoError(t, plan.Run(ctx, &bytes.Buffer{}))
	return out
}

func TestApplyFrom_Run(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	path := planTo(t, ctx, conn.Config().ConnString(), `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`, "all")

	var buf bytes.Buffer
	cmd := &command.ApplyFrom{
		ConnString: conn.Config().ConnString(),
		PlanFile:   path,
	}
	require.NoError(t, cmd.Run(ctx, &buf))
	assert.Contains(t, buf.String(), "-- Apply to schema public")
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
	assert.Contains(t, buf.String(), "-- Apply finished in ")
	assertConnectedCommentFirst(t, buf.String(), conn.Config())
}

func TestApplyFrom_Run_NoChanges(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	path := planTo(t, ctx, conn.Config().ConnString(), `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`, "all")

	var buf bytes.Buffer
	cmd := &command.ApplyFrom{
		ConnString: conn.Config().ConnString(),
		PlanFile:   path,
	}
	require.NoError(t, cmd.Run(ctx, &buf))
	assert.Contains(t, buf.String(), "-- No changes")
	assert.NotContains(t, buf.String(), "-- Apply finished in ")
}

// A failed apply still shows what ran before it stopped.
func TestApplyFrom_Run_Error(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	path := planTo(t, ctx, conn.Config().ConnString(), `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`, "all")

	_, err := conn.Exec(ctx, "CREATE TABLE public.users (id integer NOT NULL)")
	require.NoError(t, err)

	var buf bytes.Buffer
	cmd := &command.ApplyFrom{
		ConnString: conn.Config().ConnString(),
		PlanFile:   path, Force: true,
	}
	err = cmd.Run(ctx, &buf)
	require.Error(t, err)
	assert.Contains(t, buf.String(), "-- Warning:")
}

// An ignored object and a suppressed drop are decided when the plan is
// written, and apply-from reports them from the file.
func TestApplyFrom_Run_IgnoredAndSkipped(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.legacy (
    id integer NOT NULL,
    name text,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
CREATE TABLE public.old (
    id integer NOT NULL
);`)

	// No --allow-drop, so public.old is a suppressed drop rather than a DROP.
	path := planTo(t, ctx, conn.Config().ConnString(), `-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	var buf bytes.Buffer
	cmd := &command.ApplyFrom{
		ConnString: conn.Config().ConnString(),
		PlanFile:   path,
	}
	require.NoError(t, cmd.Run(ctx, &buf))
	assert.Contains(t, buf.String(), "-- ignored: public.legacy")
	assert.Contains(t, buf.String(), "-- skipped: DROP TABLE public.old;")
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}
