package pistachio_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pgstub"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
)

// dsqlRecording holds what an Aurora DSQL cluster answered to TestDSQL. The
// test replays it, so it runs without a cluster.
const dsqlRecording = "testdata/dsql/dsql.yaml"

// dsqlClient returns a client that talks to a stub replaying dsqlRecording.
// It skips the test unless TEST_PISTA_DSQL is set: the recording has to be
// made again whenever a statement pistachio sends changes, so `make test`
// leaves it out and `make test-dsql` runs it.
//
// With TEST_PISTA_DSQL_CONN_STR set, the stub sends every statement to that
// cluster instead and records the answers, which is how `make record-dsql`
// makes the recording. The password is an IAM token, which the stub reads
// from PGPASSWORD. The cluster has to hold nothing in public when the
// recording starts.
func dsqlClient(t *testing.T) *pistachio.Client {
	t.Helper()
	connString := os.Getenv("TEST_PISTA_DSQL_CONN_STR")
	if os.Getenv("TEST_PISTA_DSQL") == "" && connString == "" {
		t.Skip("TEST_PISTA_DSQL is not set; run make test-dsql")
	}
	opts := []pgstub.Option{pgstub.Replay(dsqlRecording)}
	if connString != "" {
		// The cluster gives each object a new OID and each index build a new
		// job ID. Numbering them keeps a recording made again the same.
		opts = []pgstub.Option{
			pgstub.Record(connString, dsqlRecording),
			pgstub.NormalizeOIDs(),
			pgstub.NormalizeColumn("job_id", "job-"),
		}
	}
	stub := pgstub.Start(t, opts...)

	searchPath := pistachio.DefaultSearchPath
	return pistachio.NewClient(&pistachio.Options{
		ConnString: stub.ConnString(),
		Schemas:    []string{"public"},
		SearchPath: &searchPath,
		Engine:     pistachio.EngineDSQL,
	})
}

func writeDSQLSchema(t *testing.T, sql string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(path, []byte(sql), 0o644))
	return path
}

func dsqlApply(t *testing.T, client *pistachio.Client, sql string, exec pistachio.ExecOptions) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	_, err := client.Apply(context.Background(), &pistachio.ApplyOptions{
		AllowDrop:   []string{"all"},
		Files:       []string{writeDSQLSchema(t, sql)},
		ExecOptions: exec,
	}, &buf)
	return buf.String(), err
}

func dsqlPlan(t *testing.T, client *pistachio.Client, sql string) (*pistachio.PlanResult, error) {
	t.Helper()
	return client.Plan(context.Background(), &pistachio.PlanOptions{
		AllowDrop: []string{"all"},
		Files:     []string{writeDSQLSchema(t, sql)},
	})
}

func requireNoDSQLDrift(t *testing.T, client *pistachio.Client, sql string) {
	t.Helper()
	result, err := dsqlPlan(t, client, sql)
	require.NoError(t, err)
	assert.False(t, result.HasChanges, "drift:\n%s", result.SQL)
}

const dsqlUsers = `CREATE TABLE public.users (
    id bigint GENERATED ALWAYS AS IDENTITY (CACHE 1),
    name text NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX users_name_idx ON public.users (name);
`

const dsqlUsersChanged = `CREATE TABLE public.users (
    id bigint GENERATED ALWAYS AS IDENTITY (CACHE 1),
    name text NOT NULL,
    email text,
    status text DEFAULT 'active',
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_email_key UNIQUE (email)
);
CREATE INDEX users_name_idx ON public.users (name);
CREATE INDEX users_status_idx ON public.users (status, name);
`

// TestDSQL covers what only a DSQL cluster can answer: that it takes the
// statements --engine dsql writes, that apply waits for its index builds, and
// that what it reports reads back without drift. Behavior decided before
// anything DSQL-specific is sent is tested against local PostgreSQL below.
func TestDSQL(t *testing.T) {
	client := dsqlClient(t)
	ctx := context.Background()

	// The versions the recording was made against, for reference: DSQL can
	// change its answers without changing them.
	conn, err := pgx.Connect(ctx, client.ConnString)
	require.NoError(t, err)
	var major int
	require.NoError(t, conn.QueryRow(ctx, "SELECT sys.dsql_major_version()").Scan(&major))
	t.Logf("DSQL major version %d, server version %s", major, conn.PgConn().ParameterStatus("server_version"))
	require.NoError(t, conn.Close(ctx))

	_, err = dsqlApply(t, client, "", pistachio.ExecOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := dsqlApply(t, client, "", pistachio.ExecOptions{})
		assert.NoError(t, err)
	})

	t.Run("create", func(t *testing.T) {
		out, err := dsqlApply(t, client, dsqlUsers, pistachio.ExecOptions{})
		require.NoError(t, err)
		assert.Contains(t, out, "CREATE INDEX ASYNC users_name_idx ON public.users (name);")
		requireNoDSQLDrift(t, client, dsqlUsers)
	})

	t.Run("add a column with a default and a unique constraint", func(t *testing.T) {
		out, err := dsqlApply(t, client, dsqlUsersChanged, pistachio.ExecOptions{})
		require.NoError(t, err)
		assert.Contains(t, out, "ALTER TABLE public.users ADD COLUMN status text;\nALTER TABLE public.users ALTER COLUMN status SET DEFAULT 'active';")
		assert.Contains(t, out, "CREATE UNIQUE INDEX ASYNC users_email_key ON public.users (email);\nALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE USING INDEX users_email_key;")
		requireNoDSQLDrift(t, client, dsqlUsersChanged)
	})

	t.Run("dump plans clean", func(t *testing.T) {
		dumped, err := client.Dump(ctx, &pistachio.DumpOptions{})
		require.NoError(t, err)
		dump := dumped.String()
		assert.Contains(t, dump, "GENERATED ALWAYS AS IDENTITY (CACHE 1)")
		assert.NotContains(t, dump, "INCLUDE")
		assert.NotContains(t, dump, "btree_index")
		assert.NotContains(t, dump, "COMPRESSION")
		requireNoDSQLDrift(t, client, dump)

		// dump --split writes the same CACHE 1.
		assert.Contains(t, dumped.Files()["public.users.sql"], "GENERATED ALWAYS AS IDENTITY (CACHE 1)")
	})

	t.Run("change an index without waiting for the build", func(t *testing.T) {
		changed := strings.Replace(dsqlUsersChanged, "users_status_idx ON public.users (status, name)", "users_status_idx ON public.users (status)", 1)
		out, err := dsqlApply(t, client, changed, pistachio.ExecOptions{DSQLNoWaitIndexBuild: true})
		require.NoError(t, err)
		assert.Contains(t, out, "DROP INDEX public.users_status_idx;\nCREATE INDEX ASYNC users_status_idx ON public.users (status);")
		requireNoDSQLDrift(t, client, changed)

		_, err = dsqlApply(t, client, dsqlUsersChanged, pistachio.ExecOptions{})
		require.NoError(t, err)
	})
}

// The -- pista:bulk-alter directive is refused like --bulk-alter. The refusal
// comes before anything DSQL-specific is sent, so local PostgreSQL serves.
func TestDSQL_BulkAlterDirective(t *testing.T) {
	client := localDSQLClient(t, "")
	_, err := client.Plan(context.Background(), &pistachio.PlanOptions{Files: []string{writeDSQLSchema(t, "-- pista:bulk-alter\n"+dsqlUsers)}})
	require.EqualError(t, err, "-- pista:bulk-alter cannot be used with --engine dsql")
}

// localDSQLClient returns a --engine dsql client for local PostgreSQL, loaded
// with initSQL. It serves the tests whose outcome is decided before anything
// DSQL-specific reaches the server.
func localDSQLClient(t *testing.T, initSQL string) *pistachio.Client {
	t.Helper()
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, initSQL)

	return pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
		Engine:     pistachio.EngineDSQL,
	})
}

// A change DSQL cannot make is refused at plan, before anything is applied.
func TestDSQL_RefusedAtPlan(t *testing.T) {
	client := localDSQLClient(t, dsqlUsersChanged)
	_, err := dsqlPlan(t, client, dsqlUsers)
	require.ErrorContains(t, err, "DSQL does not support DROP COLUMN")
}

// A unique constraint cannot take over an index nobody waits for, so apply
// refuses before it runs a statement.
func TestDSQL_NoWaitUniqueRefused(t *testing.T) {
	client := localDSQLClient(t, dsqlUsers)
	changed := dsqlUsers + "ALTER TABLE public.users ADD CONSTRAINT users_name_key UNIQUE (name);\n"
	_, err := dsqlApply(t, client, changed, pistachio.ExecOptions{DSQLNoWaitIndexBuild: true})
	require.ErrorContains(t, err, "--dsql-no-wait-index-build cannot be used")
}

// The plan file records the engine, and apply-from runs the plan under it.
func TestDSQL_ApplyFrom(t *testing.T) {
	ctx := context.Background()
	client := localDSQLClient(t, dsqlUsers)
	changed := dsqlUsers + "COMMENT ON TABLE public.users IS 'people';\n"

	out := filepath.Join(t.TempDir(), "plan.json")
	_, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{writeDSQLSchema(t, changed)}, Out: out})
	require.NoError(t, err)

	require.NoError(t, pistachio.ValidateApplyFromEngine(&pistachio.ApplyFromOptions{PlanFile: out}))
	require.EqualError(t, pistachio.ValidateApplyFromEngine(&pistachio.ApplyFromOptions{PlanFile: out, WithTx: true}), "--with-tx cannot be used with --engine dsql")

	var buf bytes.Buffer
	_, err = pistachio.NewClient(&pistachio.Options{ConnOptions: client.ConnOptions}).ApplyFrom(ctx, &pistachio.ApplyFromOptions{PlanFile: out}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "COMMENT ON TABLE public.users IS 'people';")
	requireNoDSQLDrift(t, client, changed)
}

// dump writes CACHE 1 on an identity column, in one file and split.
func TestDSQL_DumpIdentityCache(t *testing.T) {
	client := localDSQLClient(t, dsqlUsers)
	dumped, err := client.Dump(context.Background(), &pistachio.DumpOptions{})
	require.NoError(t, err)
	assert.Contains(t, dumped.String(), "GENERATED ALWAYS AS IDENTITY (CACHE 1)")
	assert.Contains(t, dumped.Files()["public.users.sql"], "GENERATED ALWAYS AS IDENTITY (CACHE 1)")
}
