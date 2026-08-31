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

func durationPtr(d time.Duration) *time.Duration {
	return &d
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
		ExclusiveWait: &wait,
	}, &buf)
	require.ErrorContains(t, err, "did not finish within")
	assert.GreaterOrEqual(t, time.Since(start), wait)
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

func TestApplyExclusiveWaitNegative(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:         []string{writeDesiredFile(t, exclusiveDesired)},
		ExclusiveWait: durationPtr(-time.Second),
	}, io.Discard)
	require.ErrorContains(t, err, "negative")
}
