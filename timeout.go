package pistachio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// sqlstateLockNotAvailable is the SQLSTATE of a statement that waited for a
// lock longer than lock_timeout allows.
const sqlstateLockNotAvailable = "55P03"

func isLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == sqlstateLockNotAvailable
}

// timeoutStmts returns the SET statements --lock-timeout and
// --statement-timeout ask for. They run before the pre-SQL, so a SET in the
// pre-SQL overrides them.
func (o *ExecOptions) timeoutStmts() []string {
	var stmts []string
	if o.LockTimeout != nil {
		stmts = append(stmts, "SET lock_timeout = "+millis(*o.LockTimeout)+";")
	}
	if o.StatementTimeout != nil {
		stmts = append(stmts, "SET statement_timeout = "+millis(*o.StatementTimeout)+";")
	}
	return stmts
}

// millis renders d in milliseconds, the unit of both settings, rounded up: a
// value below a millisecond would otherwise become 0 and turn the setting off.
// 0 itself stays 0, which turns it off as the user asked.
func millis(d UnsignedDuration) string {
	ms := (time.Duration(d) + time.Millisecond - 1) / time.Millisecond
	return fmt.Sprintf("'%dms'", ms)
}

// retryable reports whether a statement that failed on lock_timeout can be
// run again as it is. A CONCURRENTLY index build or drop runs in several
// transactions of its own, and one that fails after the first leaves an
// invalid index behind, so running it again fails on that index. Text the
// parser cannot read is not known to be safe, so it is not retried either.
func retryable(stmt string) bool {
	result, err := pg_query.Parse(stmt)
	if err != nil {
		return false
	}
	for _, raw := range result.Stmts {
		if is := raw.Stmt.GetIndexStmt(); is != nil && is.Concurrent {
			return false
		}
		if ds := raw.Stmt.GetDropStmt(); ds != nil && ds.Concurrent {
			return false
		}
	}
	return true
}

// waitLockRetry writes that the retry-th retry is coming and sleeps for
// --lock-retry-delay. Cancelling ctx ends the sleep.
func waitLockRetry(ctx context.Context, w io.Writer, o *ExecOptions, retry uint) error {
	delay := time.Duration(o.LockRetryDelay)
	fmt.Fprintf(w, "-- Lock timeout: retrying in %s (%d/%d)\n", delay, retry, o.LockRetries) //nolint:errcheck

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
