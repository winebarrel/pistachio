package pistachio

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/testutil"
)

const lockRetryInit = `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

const lockRetryDesired = `CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

// lockRetryWriter closes retrying when apply writes that it is about to retry
// on a lock timeout. A test that releases the lock on that signal, rather than
// after a fixed sleep, cannot release it before the first attempt has failed.
type lockRetryWriter struct {
	io.Writer
	once     sync.Once
	retrying chan struct{}
}

func newLockRetryWriter(w io.Writer) *lockRetryWriter {
	return &lockRetryWriter{Writer: w, retrying: make(chan struct{})}
}

func (w *lockRetryWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("-- Lock timeout: retrying")) {
		w.once.Do(func() { close(w.retrying) })
	}
	return w.Writer.Write(p)
}

// holdTableLock opens a transaction on a connection of its own that holds a
// lock on public.users in the given mode until the returned function ends it.
func holdTableLock(t *testing.T, ctx context.Context, mode string) func() {
	t.Helper()
	holder := testutil.ConnectDB(t)
	tx, err := holder.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "LOCK TABLE public.users IN "+mode+" MODE")
	require.NoError(t, err)

	var once sync.Once
	release := func() {
		once.Do(func() {
			tx.Rollback(ctx)  //nolint:errcheck
			holder.Close(ctx) //nolint:errcheck
		})
	}
	t.Cleanup(release)
	return release
}

// releaseOnRetry ends the lock once apply announces its first retry.
func releaseOnRetry(w *lockRetryWriter, release func()) {
	go func() {
		<-w.retrying
		release()
	}()
}

func lockRetryClient(conn *pgx.Conn) *Client {
	return NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})
}

func lockTimeoutOptions(retries uint, delay time.Duration) ExecOptions {
	return ExecOptions{
		LockTimeout:    durationPtr(100 * time.Millisecond),
		LockRetries:    retries,
		LockRetryDelay: UnsignedDuration(delay),
	}
}

func usersHasColumn(t *testing.T, ctx context.Context, conn *pgx.Conn, column string) bool {
	t.Helper()
	var n int
	err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_attribute
		WHERE attrelid = 'public.users'::regclass AND attname = $1 AND NOT attisdropped`, column).Scan(&n)
	require.NoError(t, err)
	return n == 1
}

// Without a transaction, the statement that timed out is the one run again.
func TestApply_LockRetry_Statement(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)

	release := holdTableLock(t, ctx, "ACCESS SHARE")
	var buf bytes.Buffer
	w := newLockRetryWriter(&buf)
	releaseOnRetry(w, release)

	result, err := lockRetryClient(conn).Apply(ctx, &ApplyOptions{
		Files:       []string{writeDesiredFile(t, lockRetryDesired)},
		PreSQL:      "SELECT 1;",
		ExecOptions: lockTimeoutOptions(3, 500*time.Millisecond),
	}, w)
	require.NoError(t, err)
	assert.True(t, result.Applied)
	assert.Equal(t, `SET lock_timeout = '100ms';
SELECT 1;
ALTER TABLE public.users ADD COLUMN name text;
-- Lock timeout: retrying in 500ms (1/3)
ALTER TABLE public.users ADD COLUMN name text;`, strings.TrimSpace(buf.String()))
	assert.True(t, usersHasColumn(t, ctx, conn, "name"))
}

// When every retry times out too, the last error is returned and nothing is
// applied.
func TestApply_LockRetry_Exhausted(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)
	holdTableLock(t, ctx, "ACCESS SHARE")

	var buf bytes.Buffer
	_, err := lockRetryClient(conn).Apply(ctx, &ApplyOptions{
		Files:       []string{writeDesiredFile(t, lockRetryDesired)},
		ExecOptions: lockTimeoutOptions(2, 10*time.Millisecond),
	}, &buf)
	require.Error(t, err)
	assert.True(t, isLockTimeout(err))
	assert.Equal(t, `SET lock_timeout = '100ms';
ALTER TABLE public.users ADD COLUMN name text;
-- Lock timeout: retrying in 10ms (1/2)
ALTER TABLE public.users ADD COLUMN name text;
-- Lock timeout: retrying in 10ms (2/2)
ALTER TABLE public.users ADD COLUMN name text;`, strings.TrimSpace(buf.String()))
	assert.False(t, usersHasColumn(t, ctx, conn, "name"))
}

// The timeouts are SET inside the transaction like the pre-SQL.
func TestApply_LockTimeout_WithTx(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)

	opts := ExecOptions{
		WithTx:           true,
		LockTimeout:      durationPtr(100 * time.Millisecond),
		StatementTimeout: durationPtr(time.Minute),
	}
	var buf bytes.Buffer
	_, err := lockRetryClient(conn).Apply(ctx, &ApplyOptions{
		Files:       []string{writeDesiredFile(t, lockRetryDesired)},
		PreSQL:      "SELECT 1;",
		ExecOptions: opts,
	}, &buf)
	require.NoError(t, err)
	assert.Equal(t, `-- Transaction started
SET lock_timeout = '100ms';
SET statement_timeout = '60000ms';
SELECT 1;
ALTER TABLE public.users ADD COLUMN name text;
-- Transaction committed`, strings.TrimSpace(buf.String()))
}

// --lock-timeout alone sets the timeout and stops at the first one.
func TestApply_LockTimeout_NoRetries(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)
	holdTableLock(t, ctx, "ACCESS SHARE")

	var buf bytes.Buffer
	_, err := lockRetryClient(conn).Apply(ctx, &ApplyOptions{
		Files:       []string{writeDesiredFile(t, lockRetryDesired)},
		ExecOptions: lockTimeoutOptions(0, time.Second),
	}, &buf)
	require.Error(t, err)
	assert.True(t, isLockTimeout(err))
	assert.Equal(t, `SET lock_timeout = '100ms';
ALTER TABLE public.users ADD COLUMN name text;`, strings.TrimSpace(buf.String()))
}

// A CONCURRENTLY index build is not run again: one that fails part way leaves
// an invalid index behind.
func TestApply_LockRetry_SkipsConcurrently(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)
	holdTableLock(t, ctx, "SHARE")

	var buf bytes.Buffer
	_, err := lockRetryClient(conn).Apply(ctx, &ApplyOptions{
		Files: []string{writeDesiredFile(t, lockRetryInit+`
-- pista:concurrently
CREATE INDEX users_id_idx ON public.users (id);`)},
		ExecOptions: lockTimeoutOptions(3, 10*time.Millisecond),
	}, &buf)
	require.Error(t, err)
	assert.True(t, isLockTimeout(err))
	assert.NotContains(t, buf.String(), "-- Lock timeout: retrying")
	assert.Equal(t, 1, strings.Count(buf.String(), "CREATE INDEX CONCURRENTLY"))
}

// A -- pista:execute statement is SQL the user wrote, so it is not retried.
func TestApply_LockRetry_SkipsExecuteStatement(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)
	holdTableLock(t, ctx, "ACCESS SHARE")

	var buf bytes.Buffer
	_, err := lockRetryClient(conn).Apply(ctx, &ApplyOptions{
		Files: []string{writeDesiredFile(t, lockRetryInit+`
-- pista:execute
TRUNCATE public.users;`)},
		ExecOptions: lockTimeoutOptions(3, 10*time.Millisecond),
	}, &buf)
	require.Error(t, err)
	assert.True(t, isLockTimeout(err))
	assert.Equal(t, `SET lock_timeout = '100ms';
-- pista:execute
TRUNCATE public.users;`, strings.TrimSpace(buf.String()))
}

// statement_timeout stops the apply and is never retried, even with
// --lock-retries.
func TestApply_StatementTimeout(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)

	opts := lockTimeoutOptions(3, 10*time.Millisecond)
	opts.StatementTimeout = durationPtr(100 * time.Millisecond)
	var buf bytes.Buffer
	_, err := lockRetryClient(conn).Apply(ctx, &ApplyOptions{
		Files: []string{writeDesiredFile(t, lockRetryInit+`
-- pista:execute
SELECT pg_sleep(5);`)},
		ExecOptions: opts,
	}, &buf)
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "57014", pgErr.Code)
	assert.Equal(t, `SET lock_timeout = '100ms';
SET statement_timeout = '100ms';
-- pista:execute
SELECT pg_sleep(5);`, strings.TrimSpace(buf.String()))
}

// apply-from runs a plan file's statements the same way.
func TestApplyFrom_LockRetry(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)
	path := writePlan(t, ctx, lockRetryDesired, ScopeOptions{Schemas: []string{"public"}})

	release := holdTableLock(t, ctx, "ACCESS SHARE")
	var buf bytes.Buffer
	w := newLockRetryWriter(&buf)
	releaseOnRetry(w, release)

	client := NewClient(&Options{ConnString: testutil.ConnString()})
	result, err := client.ApplyFrom(ctx, &ApplyFromOptions{
		PlanFile:    path,
		ExecOptions: lockTimeoutOptions(3, 500*time.Millisecond),
	}, w)
	require.NoError(t, err)
	assert.True(t, result.Applied)
	assert.Equal(t, `SET lock_timeout = '100ms';
ALTER TABLE public.users ADD COLUMN name text;
-- Lock timeout: retrying in 500ms (1/3)
ALTER TABLE public.users ADD COLUMN name text;`, strings.TrimSpace(buf.String()))
}

// Cancelling the run, Ctrl-C for example, ends the wait before a retry.
func TestApply_LockRetry_CancelledWhileWaiting(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)
	holdTableLock(t, ctx, "ACCESS SHARE")

	applyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var buf bytes.Buffer
	w := newLockRetryWriter(&buf)
	go func() {
		<-w.retrying
		cancel()
	}()

	start := time.Now()
	_, err := lockRetryClient(conn).Apply(applyCtx, &ApplyOptions{
		Files:       []string{writeDesiredFile(t, lockRetryDesired)},
		ExecOptions: lockTimeoutOptions(3, time.Hour),
	}, w)
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Minute)
	assert.Equal(t, `SET lock_timeout = '100ms';
ALTER TABLE public.users ADD COLUMN name text;
-- Lock timeout: retrying in 1h0m0s (1/3)`, strings.TrimSpace(buf.String()))
}

// The pre-SQL is setup, not a change, so a lock timeout there stops the run.
func TestApply_LockRetry_SkipsPreSQL(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)
	holdTableLock(t, ctx, "ACCESS SHARE")

	var buf bytes.Buffer
	_, err := lockRetryClient(conn).Apply(ctx, &ApplyOptions{
		Files:       []string{writeDesiredFile(t, lockRetryDesired)},
		PreSQL:      "TRUNCATE public.users;",
		ExecOptions: lockTimeoutOptions(3, 10*time.Millisecond),
	}, &buf)
	require.ErrorContains(t, err, "failed to execute pre-SQL")
	assert.True(t, isLockTimeout(err))
	assert.Equal(t, `SET lock_timeout = '100ms';
TRUNCATE public.users;`, strings.TrimSpace(buf.String()))
}

// An attempt that fails gets no time, so --timing times only the one that ran.
func TestApply_LockRetry_Timing(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)

	release := holdTableLock(t, ctx, "ACCESS SHARE")
	var buf bytes.Buffer
	w := newLockRetryWriter(&buf)
	releaseOnRetry(w, release)

	opts := lockTimeoutOptions(3, 500*time.Millisecond)
	opts.Timing = true
	_, err := lockRetryClient(conn).Apply(ctx, &ApplyOptions{
		Files:       []string{writeDesiredFile(t, lockRetryDesired)},
		ExecOptions: opts,
	}, w)
	require.NoError(t, err)
	assert.Equal(t, `SET lock_timeout = '100ms';
-- Time: <elapsed>
ALTER TABLE public.users ADD COLUMN name text;
-- Lock timeout: retrying in 500ms (1/3)
ALTER TABLE public.users ADD COLUMN name text;
-- Time: <elapsed>`, strings.TrimSpace(normalizeTimings(buf.String())))
}

// A value PostgreSQL does not accept fails at the SET, before anything runs.
func TestApply_LockTimeout_OutOfRange(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	testutil.SetupDB(t, ctx, conn, lockRetryInit)

	var buf bytes.Buffer
	_, err := lockRetryClient(conn).Apply(ctx, &ApplyOptions{
		Files:       []string{writeDesiredFile(t, lockRetryDesired)},
		PreSQL:      "SELECT 1;",
		LockTimeout: durationPtr(1000 * time.Hour),
	}, &buf)
	require.ErrorContains(t, err, "failed to execute SQL: SET lock_timeout = '3600000000ms';")
	assert.Equal(t, "SET lock_timeout = '3600000000ms';", strings.TrimSpace(buf.String()))
	assert.False(t, usersHasColumn(t, ctx, conn, "name"))
}
