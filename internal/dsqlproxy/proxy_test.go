package dsqlproxy_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/dsqlproxy"
	"github.com/winebarrel/pistachio/internal/testutil"
)

// connect starts a proxy in front of a database of its own and connects to
// it.
func connect(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()

	admin := testutil.ConnectDB(t)
	defer admin.Close(ctx) //nolint:errcheck
	_, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS pista_dsqlproxy WITH (FORCE)")
	require.NoError(t, err)
	_, err = admin.Exec(ctx, "CREATE DATABASE pista_dsqlproxy")
	require.NoError(t, err)

	p, err := dsqlproxy.Start(ctx, "127.0.0.1:0", testutil.ConnStringFor(t, "pista_dsqlproxy"))
	require.NoError(t, err)
	t.Cleanup(func() { p.Close() }) //nolint:errcheck

	conn, err := pgx.Connect(ctx, "postgres://admin@"+p.Addr().String()+"/postgres?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(ctx) }) //nolint:errcheck
	return conn
}

func requireCode(t *testing.T, err error, code, msg string) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, code, pgErr.Code)
	assert.Equal(t, msg, pgErr.Message)
}

func exec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	require.NoError(t, err)
}

func TestProxy_AsyncIndex(t *testing.T) {
	ctx := context.Background()
	conn := connect(t)
	exec(t, conn, "CREATE TABLE t (id bigint PRIMARY KEY, c text)")

	// The simple protocol.
	results, err := conn.PgConn().Exec(ctx, "CREATE INDEX ASYNC t_c_idx ON t (c)").ReadAll()
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "CREATE INDEX", results[0].CommandTag.String())
	require.Len(t, results[0].Rows, 1)
	jobID := string(results[0].Rows[0][0])

	// The index is invalid until its job is waited for.
	var valid bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = 't_c_idx'::regclass").Scan(&valid))
	assert.False(t, valid)

	var succeeded bool
	require.NoError(t, conn.QueryRow(ctx, "CALL sys.wait_for_job('"+jobID+"')").Scan(&succeeded))
	assert.True(t, succeeded)
	require.NoError(t, conn.QueryRow(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = 't_c_idx'::regclass").Scan(&valid))
	assert.True(t, valid)

	// The extended protocol.
	rows, err := conn.Query(ctx, "CREATE INDEX ASYNC t_c2_idx ON t (c)")
	require.NoError(t, err)
	for rows.Next() {
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, "CREATE INDEX", rows.CommandTag().String())
}

func TestProxy_ValidateJob(t *testing.T) {
	ctx := context.Background()
	conn := connect(t)
	exec(t, conn, "CREATE TABLE t (id bigint PRIMARY KEY, n int)")
	exec(t, conn, "INSERT INTO t VALUES (1, -1)")
	exec(t, conn, "ALTER TABLE t ADD CONSTRAINT t_n_check CHECK (n > 0) NOT VALID")

	// A violating row fails the job, not the statement.
	var jobID string
	require.NoError(t, conn.QueryRow(ctx, "ALTER TABLE ASYNC t VALIDATE CONSTRAINT t_n_check").Scan(&jobID))
	var succeeded bool
	require.NoError(t, conn.QueryRow(ctx, "CALL sys.wait_for_job('"+jobID+"')").Scan(&succeeded))
	assert.False(t, succeeded)

	exec(t, conn, "DELETE FROM t")
	require.NoError(t, conn.QueryRow(ctx, "ALTER TABLE ASYNC t VALIDATE CONSTRAINT t_n_check").Scan(&jobID))
	require.NoError(t, conn.QueryRow(ctx, "CALL sys.wait_for_job('"+jobID+"')").Scan(&succeeded))
	assert.True(t, succeeded)
	var validated bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT convalidated FROM pg_constraint WHERE conname = 't_n_check'").Scan(&validated))
	assert.True(t, validated)
}

func TestProxy_Refusal(t *testing.T) {
	ctx := context.Background()
	conn := connect(t)
	exec(t, conn, "CREATE TABLE t (id bigint PRIMARY KEY, c text)")

	_, err := conn.Exec(ctx, "CREATE INDEX t_c_idx ON t (c)")
	requireCode(t, err, "0A000", "unsupported mode. please use CREATE INDEX ASYNC.")

	_, err = conn.Exec(ctx, "ALTER TABLE t DROP CONSTRAINT t_pkey")
	requireCode(t, err, "0A000", "unsupported ALTER TABLE DROP CONSTRAINT statement")

	// A constraint other than the primary key drops.
	exec(t, conn, "ALTER TABLE t ADD CONSTRAINT t_c_check CHECK (c <> '') NOT VALID")
	tag, err := conn.Exec(ctx, "ALTER TABLE t DROP CONSTRAINT t_c_check")
	require.NoError(t, err)
	assert.Equal(t, "ALTER TABLE", tag.String())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) //nolint:errcheck
	_, err = tx.Exec(ctx, "CREATE TABLE u (id bigint PRIMARY KEY)")
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "CREATE TABLE v (id bigint PRIMARY KEY)")
	requireCode(t, err, "0A000", "multiple ddl statements not supported in a transaction")
}

func TestProxy_Catalog(t *testing.T) {
	ctx := context.Background()
	conn := connect(t)
	exec(t, conn, "CREATE TABLE t (id bigint PRIMARY KEY, name text, n int)")
	var jobID string
	require.NoError(t, conn.QueryRow(ctx, "CREATE INDEX ASYNC t_name_idx ON t (name)").Scan(&jobID))

	var def string
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_catalog.pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint WHERE conname = 't_pkey'").Scan(&def))
	assert.Equal(t, "PRIMARY KEY (id) INCLUDE (name, n)", def)

	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_catalog.pg_get_indexdef(c.oid) FROM pg_catalog.pg_class c WHERE c.relname = 't_name_idx'").Scan(&def))
	assert.Equal(t, "CREATE INDEX t_name_idx ON public.t USING btree_index (name)", def)

	rows, err := conn.Query(ctx, "SELECT a.attname, a.attcompression::text FROM pg_catalog.pg_attribute a WHERE a.attrelid = 't'::regclass AND a.attnum > 0 ORDER BY a.attnum")
	require.NoError(t, err)
	got, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([2]string, error) {
		var r [2]string
		return r, row.Scan(&r[0], &r[1])
	})
	require.NoError(t, err)
	assert.Equal(t, [][2]string{{"id", ""}, {"name", "l"}, {"n", ""}}, got)

	// DSQL reports the bare version number.
	assert.Regexp(t, `^\d+\.\d+$`, conn.PgConn().ParameterStatus("server_version"))
}

func TestProxy_RejectsReadOnlyStartup(t *testing.T) {
	ctx := context.Background()
	conn := connect(t)
	cfg, err := pgx.ParseConfig(conn.Config().ConnString())
	require.NoError(t, err)
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	_, err = pgx.ConnectConfig(ctx, cfg)
	require.ErrorContains(t, err, `setting configuration parameter "default_transaction_read_only" not supported`)
	require.ErrorContains(t, err, "SQLSTATE 0A000")

	// The read-only statement after connecting works.
	exec(t, conn, "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
	_, err = conn.Exec(ctx, "CREATE TABLE t (id bigint PRIMARY KEY)")
	require.ErrorContains(t, err, "read-only transaction")
}

func TestProxy_DeclinesTLS(t *testing.T) {
	ctx := context.Background()
	conn := connect(t)
	cfg, err := pgx.ParseConfig(conn.Config().ConnString())
	require.NoError(t, err)

	// sslmode=prefer asks for TLS first and goes on without it.
	prefer, err := pgx.Connect(ctx, cfg.ConnString()[:len(cfg.ConnString())-len("disable")]+"prefer")
	require.NoError(t, err)
	defer prefer.Close(ctx) //nolint:errcheck
	exec(t, prefer, "SELECT 1")
}
