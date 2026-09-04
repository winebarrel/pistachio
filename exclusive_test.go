package pistachio_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
)

const exclusiveDesired = `CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

func writeDesiredFile(t *testing.T, sql string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(file, []byte(sql), 0o644))
	return file
}

func holdExclusive(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	_, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1, hashtext(current_database()))", pistachio.ExclusiveLockClassID)
	require.NoError(t, err)
}

func releaseExclusive(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1, hashtext(current_database()))", pistachio.ExclusiveLockClassID)
	return err
}

func durationPtr(d time.Duration) *pistachio.UnsignedDuration {
	w := pistachio.UnsignedDuration(d)
	return &w
}

func TestApplyExclusive(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	result, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:     []string{writeDesiredFile(t, exclusiveDesired)},
		Exclusive: true,
	}, &buf)
	require.NoError(t, err)
	assert.True(t, result.Applied)
	assert.Contains(t, buf.String(), "CREATE TABLE")

	// Apply's connection is closed, so the exclusion must be free again.
	var acquired bool
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1, hashtext(current_database()))", pistachio.ExclusiveLockClassID).Scan(&acquired)
	require.NoError(t, err)
	assert.True(t, acquired)
	require.NoError(t, releaseExclusive(ctx, conn))
}

func TestApplyExclusiveConflict(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	holder := testutil.ConnectDB(t)
	defer holder.Close(ctx)
	holdExclusive(t, ctx, holder)
	defer releaseExclusive(ctx, holder) //nolint:errcheck

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:     []string{writeDesiredFile(t, exclusiveDesired)},
		Exclusive: true,
	}, &buf)
	require.ErrorContains(t, err, "another exclusive apply is running")
	assert.Empty(t, buf.String())

	// Nothing may have been applied.
	var count int
	err = conn.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'").Scan(&count)
	require.NoError(t, err)
	assert.Zero(t, count)
}

func TestApplyExclusiveWaitTimeout(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	holder := testutil.ConnectDB(t)
	defer holder.Close(ctx)
	holdExclusive(t, ctx, holder)
	defer releaseExclusive(ctx, holder) //nolint:errcheck

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	wait := 300 * time.Millisecond
	start := time.Now()
	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:         []string{writeDesiredFile(t, exclusiveDesired)},
		ExclusiveWait: durationPtr(wait),
	}, &buf)
	elapsed := time.Since(start)
	require.ErrorContains(t, err, "did not finish within")
	assert.GreaterOrEqual(t, elapsed, wait)
	// The wait ends on its own deadline, not on the next poll.
	assert.Less(t, elapsed, pistachio.ExclusivePollInterval)
	assert.Contains(t, buf.String(), "-- Waiting for another exclusive apply to finish")
}

func TestApplyExclusiveWait(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	holder := testutil.ConnectDB(t)
	defer holder.Close(ctx)
	holdExclusive(t, ctx, holder)

	release := make(chan error, 1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		release <- releaseExclusive(ctx, holder)
	}()

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	// 0 waits without limit.
	var buf bytes.Buffer
	result, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:         []string{writeDesiredFile(t, exclusiveDesired)},
		ExclusiveWait: durationPtr(0),
	}, &buf)
	require.NoError(t, err)
	require.NoError(t, <-release)
	assert.True(t, result.Applied)
	assert.Contains(t, buf.String(), "-- Waiting for another exclusive apply to finish")
	assert.Contains(t, buf.String(), "CREATE TABLE")
}

func TestApplyExclusiveWaitWithinDeadline(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	holder := testutil.ConnectDB(t)
	defer holder.Close(ctx)
	holdExclusive(t, ctx, holder)

	release := make(chan error, 1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		release <- releaseExclusive(ctx, holder)
	}()

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	result, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:         []string{writeDesiredFile(t, exclusiveDesired)},
		ExclusiveWait: durationPtr(10 * time.Second),
	}, &buf)
	require.NoError(t, err)
	require.NoError(t, <-release)
	assert.True(t, result.Applied)
	assert.Contains(t, buf.String(), "-- Waiting for another exclusive apply to finish")
}

func TestApplyExclusiveWaitUncontended(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	result, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:         []string{writeDesiredFile(t, exclusiveDesired)},
		ExclusiveWait: durationPtr(5 * time.Second),
	}, &buf)
	require.NoError(t, err)
	assert.True(t, result.Applied)
	assert.NotContains(t, buf.String(), "Waiting")
}

func TestApplyExclusiveWithTx(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	// The exclusion is session-level: it must coexist with --with-tx and be
	// released when the connection closes, not before.
	var buf bytes.Buffer
	result, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:     []string{writeDesiredFile(t, exclusiveDesired)},
		Exclusive: true,
		WithTx:    true,
	}, &buf)
	require.NoError(t, err)
	assert.True(t, result.Applied)
	assert.Contains(t, buf.String(), "-- Transaction committed")

	var acquired bool
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1, hashtext(current_database()))", pistachio.ExclusiveLockClassID).Scan(&acquired)
	require.NoError(t, err)
	assert.True(t, acquired)
	require.NoError(t, releaseExclusive(ctx, conn))
}

func TestApplyExclusiveWaitCanceled(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	holder := testutil.ConnectDB(t)
	defer holder.Close(ctx)
	holdExclusive(t, ctx, holder)
	defer releaseExclusive(ctx, holder) //nolint:errcheck

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	// Canceling the caller's context (e.g. Ctrl-C) must stop an unlimited
	// wait, and must not be reported as a wait timeout.
	applyCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	_, err := client.Apply(applyCtx, &pistachio.ApplyOptions{
		Files:         []string{writeDesiredFile(t, exclusiveDesired)},
		ExclusiveWait: durationPtr(0),
	}, io.Discard)
	require.ErrorContains(t, err, "failed to acquire the apply exclusion")
	assert.NotContains(t, err.Error(), "did not finish within")
}

func TestUnsignedDuration(t *testing.T) {
	var d pistachio.UnsignedDuration
	require.NoError(t, d.UnmarshalText([]byte("5m")))
	assert.Equal(t, pistachio.UnsignedDuration(5*time.Minute), d)
	require.NoError(t, d.UnmarshalText([]byte("0")))
	assert.Equal(t, pistachio.UnsignedDuration(0), d)
	require.ErrorContains(t, d.UnmarshalText([]byte("-3s")), "negative")
	require.Error(t, d.UnmarshalText([]byte("bad")))
}

const exclusiveIndexDesired = `CREATE TABLE items (
    id integer NOT NULL
);
CREATE INDEX items_id_idx ON items (id);
CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`

func TestApplyExclusiveWaitDuringConcurrentIndexBuild(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "CREATE TABLE items (id integer NOT NULL);")

	holder := testutil.ConnectDB(t)
	defer holder.Close(ctx)
	holdExclusive(t, ctx, holder)

	// The holder builds an index CONCURRENTLY while it owns the exclusion.
	// CREATE INDEX CONCURRENTLY waits for every backend that holds an older
	// snapshot, whatever table that backend is touching, so a waiter that
	// sits inside a statement of its own closes a lock cycle: the index build
	// waits for the waiter's snapshot, and the waiter waits for the exclusion
	// the index build's session holds. The wait must therefore hold no
	// snapshot between attempts.
	done := make(chan error, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		if _, err := holder.Exec(ctx, "CREATE INDEX CONCURRENTLY items_id_idx ON items (id)"); err != nil {
			done <- err
			return
		}
		done <- releaseExclusive(ctx, holder)
	}()

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	result, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:         []string{writeDesiredFile(t, exclusiveIndexDesired)},
		ExclusiveWait: durationPtr(30 * time.Second),
	}, &buf)
	require.NoError(t, err)
	require.NoError(t, <-done)
	assert.True(t, result.Applied)
	assert.Contains(t, buf.String(), "CREATE TABLE")
}

func backendPID(t *testing.T, ctx context.Context, conn *pgx.Conn) int32 {
	t.Helper()
	var pid int32
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))
	return pid
}

// waiterPID returns the backend of the apply that is waiting for the
// exclusion: the one that ran an advisory lock function and is neither the
// holder nor the backend asking.
func waiterPID(t *testing.T, ctx context.Context, conn *pgx.Conn, holderPID int32) int32 {
	t.Helper()
	for range 100 {
		var pid int32
		err := conn.QueryRow(ctx, `SELECT pid FROM pg_stat_activity
			WHERE datname = current_database()
			AND pid NOT IN (pg_backend_pid(), $1)
			AND query LIKE '%advisory_lock%'`, holderPID).Scan(&pid)
		if err == nil {
			return pid
		}
		require.ErrorIs(t, err, pgx.ErrNoRows)
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the waiting backend did not appear")
	return 0
}

func TestApplyExclusiveWaitHoldsNoSnapshot(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	holder := testutil.ConnectDB(t)
	defer holder.Close(ctx)
	holderPID := backendPID(t, ctx, holder)
	holdExclusive(t, ctx, holder)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	desired := writeDesiredFile(t, exclusiveDesired)
	applied := make(chan error, 1)
	go func() {
		_, err := client.Apply(ctx, &pistachio.ApplyOptions{
			Files:         []string{desired},
			ExclusiveWait: durationPtr(0),
		}, io.Discard)
		applied <- err
	}()

	// A backend that holds a snapshot is one CREATE INDEX CONCURRENTLY waits
	// for, so the wait has to leave none behind between attempts. Sampling
	// can land on an attempt, which is a statement like any other, so this
	// looks for a sample without one rather than requiring every sample to be
	// free of it.
	pid := waiterPID(t, ctx, conn, holderPID)
	idle := false
	for range 50 {
		var state string
		var noSnapshot bool
		err := conn.QueryRow(ctx,
			"SELECT state, backend_xmin IS NULL FROM pg_stat_activity WHERE pid = $1", pid).
			Scan(&state, &noSnapshot)
		require.NoError(t, err)
		if state == "idle" && noSnapshot {
			idle = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.True(t, idle, "the wait held a snapshot the whole time")

	require.NoError(t, releaseExclusive(ctx, holder))
	require.NoError(t, <-applied)
}

func TestApplyExclusiveWaitConnectionLost(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	holder := testutil.ConnectDB(t)
	defer holder.Close(ctx)
	holderPID := backendPID(t, ctx, holder)
	holdExclusive(t, ctx, holder)
	defer releaseExclusive(ctx, holder) //nolint:errcheck

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	desired := writeDesiredFile(t, exclusiveDesired)
	applied := make(chan error, 1)
	go func() {
		_, err := client.Apply(ctx, &pistachio.ApplyOptions{
			Files:         []string{desired},
			ExclusiveWait: durationPtr(30 * time.Second),
		}, io.Discard)
		applied <- err
	}()

	// A connection that dies during the wait is reported as the failure it is,
	// not as the wait running out.
	pid := waiterPID(t, ctx, conn, holderPID)
	_, err := conn.Exec(ctx, "SELECT pg_terminate_backend($1)", pid)
	require.NoError(t, err)

	err = <-applied
	require.ErrorContains(t, err, "failed to acquire the apply exclusion")
	assert.NotContains(t, err.Error(), "did not finish within")
}
