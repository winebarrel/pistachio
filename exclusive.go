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

// exclusivePollInterval is how long a wait sleeps between attempts. The runs
// it waits for take minutes, so a second of latency costs nothing, and one
// statement a second costs the database nothing.
const exclusivePollInterval = time.Second

// UnsignedDuration is a time.Duration that rejects a negative value at
// parse time, so a flag, env var or config key using it cannot be set below
// zero. kong decodes it through UnmarshalText like any other duration.
type UnsignedDuration time.Duration

func (d *UnsignedDuration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	if v < 0 {
		return fmt.Errorf("must not be negative: %s", v)
	}
	*d = UnsignedDuration(v)
	return nil
}

// tryExclusive makes one non-blocking attempt at the exclusion.
func tryExclusive(ctx context.Context, conn *pgx.Conn) (bool, error) {
	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1, hashtext(current_database()))", exclusiveLockClassID).Scan(&acquired); err != nil {
		return false, fmt.Errorf("pistachio: failed to acquire the apply exclusion: %w", err)
	}
	return acquired, nil
}

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
// With wait == nil a held lock is an immediate error. Otherwise the attempt is
// retried on a timer until the holder is gone, bounded by *wait through a
// context deadline (0 waits without limit); a server-side setting like
// lock_timeout is deliberately not used, so nothing leaks into the session the
// DDL then runs on.
//
// The wait retries rather than blocking in pg_advisory_lock because a blocked
// statement holds a snapshot while it waits, and the apply it waits for may run
// CREATE INDEX CONCURRENTLY, whose build waits for every backend that holds
// one. That closes a cycle, and PostgreSQL breaks a cycle by killing a session
// in it, the waiter being the one it picks. Advisory locks sit in the same lock
// manager as table locks, so the detector sees it. Between attempts this
// session is idle and holds no snapshot, so there is no cycle.
func acquireExclusive(ctx context.Context, conn *pgx.Conn, wait *UnsignedDuration, w io.Writer) error {
	acquired, err := tryExclusive(ctx, conn)
	if err != nil {
		return err
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
		waitCtx, cancel = context.WithTimeout(ctx, time.Duration(*wait))
		defer cancel()
	}

	ticker := time.NewTicker(exclusivePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			// The caller's own context going away, Ctrl-C for example, is not
			// the wait running out.
			if ctx.Err() != nil {
				return fmt.Errorf("pistachio: failed to acquire the apply exclusion: %w", ctx.Err())
			}
			return fmt.Errorf("pistachio: another exclusive apply did not finish within %s", time.Duration(*wait))
		case <-ticker.C:
		}

		// ctx, not waitCtx: an attempt is a single fast statement, and letting
		// the wait deadline cancel it mid-flight would report the query rather
		// than the wait.
		acquired, err := tryExclusive(ctx, conn)
		if err != nil {
			return err
		}
		if acquired {
			return nil
		}
	}
}
