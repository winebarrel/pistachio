package pistachio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecOptionsTimeoutStmts(t *testing.T) {
	assert.Empty(t, (&ExecOptions{}).timeoutStmts())

	assert.Equal(t, []string{
		"SET lock_timeout = '1500ms';",
		"SET statement_timeout = '300000ms';",
	}, (&ExecOptions{
		LockTimeout:      durationPtr(1500 * time.Millisecond),
		StatementTimeout: durationPtr(5 * time.Minute),
	}).timeoutStmts())

	// Below a millisecond rounds up: 0 would turn the setting off. 0 given
	// as 0 does turn it off.
	assert.Equal(t, []string{"SET lock_timeout = '1ms';", "SET statement_timeout = '0ms';"}, (&ExecOptions{
		LockTimeout:      durationPtr(time.Microsecond),
		StatementTimeout: durationPtr(0),
	}).timeoutStmts())
}

func TestIsLockTimeout(t *testing.T) {
	lockErr := &pgconn.PgError{Code: "55P03"}
	assert.True(t, isLockTimeout(lockErr))
	assert.True(t, isLockTimeout(fmt.Errorf("failed to execute SQL: %w", lockErr)))
	assert.False(t, isLockTimeout(&pgconn.PgError{Code: "57014"}))
	assert.False(t, isLockTimeout(errors.New("55P03")))
	assert.False(t, isLockTimeout(nil))
}

func TestRetryable(t *testing.T) {
	assert.True(t, retryable("ALTER TABLE t ADD COLUMN c integer;"))
	assert.True(t, retryable("CREATE INDEX i ON t (c);"))
	assert.True(t, retryable("UPDATE t SET c = 1; UPDATE t SET c = 2;"))
	assert.False(t, retryable("CREATE INDEX CONCURRENTLY i ON t (c);"))
	assert.False(t, retryable("DROP INDEX CONCURRENTLY public.i;"))
	// Text the parser cannot read is not known to be safe to run again.
	assert.False(t, retryable("NOT SQL"))
}

func TestWaitLockRetry(t *testing.T) {
	var buf bytes.Buffer
	opts := &ExecOptions{LockRetries: 3, LockRetryDelay: UnsignedDuration(time.Millisecond)}
	require.NoError(t, waitLockRetry(context.Background(), &buf, opts, 1))
	assert.Equal(t, "-- Lock timeout: retrying in 1ms (1/3)\n", buf.String())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opts.LockRetryDelay = UnsignedDuration(time.Hour)
	require.ErrorIs(t, waitLockRetry(ctx, &buf, opts, 2), context.Canceled)
}
