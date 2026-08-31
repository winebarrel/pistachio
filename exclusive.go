package pistachio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
)

// exclusiveLockClassID is the first key of the two-key session-level advisory
// lock that makes exclusive apply runs mutually exclusive ("Pist" in ASCII).
// The second key is hashtext(current_database()): the advisory lock namespace
// is cluster-wide, so hashing the database name scopes the exclusion to one
// database.
const exclusiveLockClassID = 0x50697374

// acquireExclusive makes this apply run mutually exclusive with other
// exclusive apply runs on the same database. It runs right after connecting,
// before the catalog is read, so the diff cannot be computed against a state
// another exclusive apply is still changing.
//
// The lock is session-level on purpose: it is unaffected by transaction
// boundaries, so it behaves the same with --with-tx, without it, and across
// CONCURRENTLY index DDL, and it is released when the connection closes,
// including on a crash.
//
// With wait == nil a held lock is an immediate error. Otherwise a second,
// blocking attempt waits for the holder, bounded by *wait through a context
// deadline (0 waits without limit); a server-side setting like lock_timeout
// is deliberately not used, so nothing leaks into the session the DDL then
// runs on.
func acquireExclusive(ctx context.Context, conn *pgx.Conn, wait *time.Duration, w io.Writer) error {
	if wait != nil && *wait < 0 {
		return fmt.Errorf("pistachio: negative exclusive wait duration: %s", *wait)
	}

	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1, hashtext(current_database()))", exclusiveLockClassID).Scan(&acquired); err != nil {
		return fmt.Errorf("pistachio: failed to acquire the apply exclusion: %w", err)
	}
	if acquired {
		return nil
	}
	if wait == nil {
		return errors.New("pistachio: another exclusive apply is running (--exclusive-wait waits for it)")
	}

	fmt.Fprintln(w, "-- Waiting for another exclusive apply to finish") //nolint:errcheck

	waitCtx := ctx
	if *wait > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, *wait)
		defer cancel()
	}
	if _, err := conn.Exec(waitCtx, "SELECT pg_advisory_lock($1, hashtext(current_database()))", exclusiveLockClassID); err != nil {
		if waitCtx.Err() != nil && ctx.Err() == nil {
			return fmt.Errorf("pistachio: another exclusive apply did not finish within %s", *wait)
		}
		return fmt.Errorf("pistachio: failed to acquire the apply exclusion: %w", err)
	}
	return nil
}
