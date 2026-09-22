package pistachio

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/parser"
)

type ApplyOptions struct {
	DropPolicy
	Files                    []string `arg:"" help:"Path to the desired schema SQL file(s)."`
	PreSQL                   string   `xor:"pre-sql" env:"PISTA_PRE_SQL" help:"SQL to execute before applying changes."`
	PreSQLFile               string   `type:"path" xor:"pre-sql" env:"PISTA_PRE_SQL_FILE" help:"Path to a SQL file to execute before applying changes."`
	ConcurrentlyPreSQL       string   `xor:"concurrently-pre-sql" env:"PISTA_CONCURRENTLY_PRE_SQL" help:"SQL to execute before CONCURRENTLY index DDL (e.g. SET lock_timeout). Runs outside any transaction, only when the diff contains CONCURRENTLY index DDL."`
	ConcurrentlyPreSQLFile   string   `type:"path" xor:"concurrently-pre-sql" env:"PISTA_CONCURRENTLY_PRE_SQL_FILE" help:"Path to a SQL file to execute before CONCURRENTLY index DDL."`
	DisableIndexConcurrently bool     `xor:"index-concurrently" env:"PISTA_DISABLE_INDEX_CONCURRENTLY" help:"Ignore CONCURRENTLY opt-ins (directive and inline) and emit plain CREATE/DROP INDEX."`
	ForceIndexConcurrently   bool     `xor:"index-concurrently,tx-mode" env:"PISTA_FORCE_INDEX_CONCURRENTLY" help:"Force CONCURRENTLY on every CREATE/DROP INDEX, including pure drops."`
	BulkAlter                bool     `env:"PISTA_BULK_ALTER" help:"Combine consecutive ALTER TABLE actions on the same table into a single statement. FK changes, RENAME, VALIDATE CONSTRAINT, RLS toggles, and skipped DROPs stay separate."`
	AssumeValidated          bool     `env:"PISTA_ASSUME_VALIDATED" help:"Treat every table constraint, domain constraint, and foreign key as validated: ignore NOT VALID and never emit VALIDATE CONSTRAINT."`
	ExecOptions
}

// ExecOptions decides how the statements are run rather than what they are, so
// apply and apply-from, which takes its statements from a plan file, share it.
type ExecOptions struct {
	WithTx bool `xor:"tx-mode,tx-choice" env:"PISTA_WITH_TX" help:"Execute pre-SQL and schema changes in a transaction."`
	TryTx  bool `xor:"tx-choice" env:"PISTA_TRY_TX" help:"Execute pre-SQL and schema changes in a transaction when possible. A diff containing CONCURRENTLY index DDL runs without a transaction instead of failing."`
	Timing bool `env:"PISTA_TIMING" help:"Write each statement's elapsed time after it as a comment. Measured on the client, so it covers the round trip and any lock wait."`
	// Exclusive and ExclusiveWait guard the database rather than one apply, so
	// apply-from takes them too: a plan file executed beside another apply
	// would be checked against a state that apply is still changing.
	Exclusive bool `xor:"exclusive" env:"PISTA_EXCLUSIVE" help:"Make apply runs on the same database mutually exclusive: fail immediately when another exclusive apply is running."`
	// ExclusiveWait enables the same mutual exclusion as Exclusive and waits
	// for the other apply instead of failing. A pointer because 0 is a valid
	// value (wait without limit) and must be distinguishable from "not set".
	// The type rejects a negative value at parse time.
	ExclusiveWait *UnsignedDuration `xor:"exclusive" env:"PISTA_EXCLUSIVE_WAIT" placeholder:"DURATION" help:"Like --exclusive, but wait up to the given duration (0 waits without limit) for the other apply to finish."`
	// WaitWriter receives the line that says apply is waiting for another
	// exclusive apply. The CLI buffers the output writer until the apply is
	// done, which would hold that line back until the wait it announces is
	// over, so it passes the terminal here. nil writes the line to the
	// output writer.
	WaitWriter io.Writer `kong:"-"`
}

// applyInput is the statement set an apply runs, whichever it came from: the
// diff this run computed, or the plan file an earlier run wrote. They are held
// apart rather than joined into one script because apply treats each kind
// differently: pre-SQL runs before the search_path is set, the CONCURRENTLY
// pre-SQL only where there is CONCURRENTLY index DDL, and an execute statement
// writes its directive to the output.
type applyInput struct {
	PreSQL               string
	ConcurrentlyPreSQL   string
	HasConcurrentlyIndex bool
	Stmts                []string
	ExecuteStmts         []*parser.ExecuteStmt
}

// ApplyResult holds the result of an Apply operation.
type ApplyResult struct {
	Count           ObjectCount
	DisallowedDrops string
	Ignored         string
	// Applied reports whether any schema change was actually applied: schema
	// DDL or an executed -- pista:execute statement. Pre-SQL,
	// concurrently-pre-SQL, transaction control, search_path setup, and
	// -- pista:execute directives skipped by their check SQL do not count.
	Applied bool
	// Duration is the elapsed time of the apply phase: every statement sent to
	// the database (transaction BEGIN/COMMIT, pre-SQL, schema DDL, search_path
	// setup, -- pista:execute check SQL, and execute statements) plus the time
	// writing them to the output writer. It excludes connection setup and diff
	// computation, and is zero unless Applied is true. With a fast writer it is
	// dominated by database execution time.
	Duration time.Duration
}

// timingComment renders an elapsed time as psql's \timing does, in
// milliseconds with three decimals. A Go duration rounded to milliseconds
// reports the sub-millisecond time of most DDL as "0s".
func timingComment(elapsed time.Duration) string {
	return fmt.Sprintf("-- Time: %.3f ms", float64(elapsed.Nanoseconds())/float64(time.Millisecond))
}

func (client *Client) Apply(ctx context.Context, options *ApplyOptions, w io.Writer) (*ApplyResult, error) {
	if err := client.validateSchemas(); err != nil {
		return nil, err
	}
	desired, err := client.loadDesiredInput(
		options.Files,
		options.PreSQL, options.PreSQLFile,
		options.ConcurrentlyPreSQL, options.ConcurrentlyPreSQLFile,
	)
	if err != nil {
		return nil, err
	}

	conn, err := client.connect(ctx, false)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx) //nolint:errcheck

	// Acquire the exclusion before the catalog is read, so the diff below
	// cannot be computed against a state another exclusive apply is still
	// changing. Released when the connection closes.
	if err := acquireExclusiveIfAsked(ctx, conn, &options.ExecOptions, w); err != nil {
		return nil, err
	}

	result, err := client.diffAll(ctx, conn, &diffAllOptions{
		FilterOptions:            client.FilterOptions,
		DropPolicy:               options.DropPolicy,
		Desired:                  desired,
		DisableIndexConcurrently: options.DisableIndexConcurrently,
		ForceIndexConcurrently:   options.ForceIndexConcurrently,
		BulkAlter:                options.BulkAlter,
		AssumeValidated:          options.AssumeValidated,
	})
	if err != nil {
		return nil, err
	}

	applyResult := &ApplyResult{
		Count:           result.Count,
		DisallowedDrops: strings.Join(result.DisallowedDrops, "\n"),
		Ignored:         strings.Join(result.Ignored, "\n"),
	}

	if err := client.applyStmts(ctx, conn, &applyInput{
		PreSQL:               result.PreSQL,
		ConcurrentlyPreSQL:   result.ConcurrentlyPreSQL,
		HasConcurrentlyIndex: result.HasConcurrentlyIndex,
		Stmts:                result.Stmts,
		ExecuteStmts:         result.ExecuteStmts,
	}, &options.ExecOptions, w, applyResult); err != nil {
		return nil, err
	}

	return applyResult, nil
}

// acquireExclusiveIfAsked takes the exclusion when one was asked for. Both
// entry points call it before they read the catalog, so neither is checked
// against, or computed against, a state another exclusive apply is still
// changing.
func acquireExclusiveIfAsked(ctx context.Context, conn *pgx.Conn, options *ExecOptions, w io.Writer) error {
	if !options.Exclusive && options.ExclusiveWait == nil {
		return nil
	}
	waitWriter := options.WaitWriter
	if waitWriter == nil {
		waitWriter = w
	}
	return acquireExclusive(ctx, conn, options.ExclusiveWait, waitWriter)
}

// applyStmts runs the statements and writes them to w, filling in the Applied
// and Duration of result. Apply hands it the diff it just computed and
// apply-from the plan file it read, so the two run a change the same way.
func (client *Client) applyStmts(
	ctx context.Context,
	conn *pgx.Conn,
	input *applyInput,
	options *ExecOptions,
	w io.Writer,
	result *ApplyResult,
) error {
	// --with-tx is an explicit all-or-nothing request, so CONCURRENTLY index
	// DDL (which PostgreSQL cannot run inside a transaction) stays an error.
	// --try-tx asks for a transaction only when one is possible, so the same
	// diff runs without one.
	if options.WithTx && input.HasConcurrentlyIndex {
		return fmt.Errorf("--with-tx cannot be used with CONCURRENTLY index operations")
	}
	withTx := options.WithTx || (options.TryTx && !input.HasConcurrentlyIndex)

	if len(input.Stmts) == 0 && len(input.ExecuteStmts) == 0 {
		return nil
	}

	start := time.Now()
	applied := false

	exec := conn.Exec
	queryRow := conn.QueryRow
	commit := func(context.Context) error { return nil }

	// writeTiming reports how long the statement just written to w took. It is
	// a no-op without --timing.
	writeTiming := func(elapsed time.Duration) {
		if options.Timing {
			fmt.Fprintln(w, timingComment(elapsed)) //nolint:errcheck
		}
	}

	// execTimed runs a statement already written to w and writes its elapsed
	// time after it. A statement that fails is left without a time, so the
	// output names where the apply stopped.
	execTimed := func(ctx context.Context, stmt string) error {
		stmtStart := time.Now()
		if _, err := exec(ctx, stmt); err != nil {
			return err
		}
		writeTiming(time.Since(stmtStart))
		return nil
	}

	if withTx {
		txStart := time.Now()
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		fmt.Fprintln(w, "-- Transaction started") //nolint:errcheck
		writeTiming(time.Since(txStart))
		committed := false
		defer func() {
			tx.Rollback(ctx) //nolint:errcheck
			if !committed {
				fmt.Fprintln(w, "-- Transaction rolled back") //nolint:errcheck
			}
		}()
		exec = tx.Exec
		queryRow = tx.QueryRow
		commit = func(ctx context.Context) error {
			commitStart := time.Now()
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			committed = true
			fmt.Fprintln(w, "-- Transaction committed") //nolint:errcheck
			writeTiming(time.Since(commitStart))
			return nil
		}
	} else if options.TryTx {
		// Record why the requested transaction was not opened, in the same
		// sentence form as the comments above.
		fmt.Fprintln(w, "-- Transaction skipped: plan contains CONCURRENTLY index DDL") //nolint:errcheck
	}

	// Pre-SQL and concurrently-pre-SQL are setup steps (e.g. SET lock_timeout),
	// not schema changes, so they do not mark the apply as applied. Whether
	// "-- No changes" is reported depends only on actual schema DDL and
	// executed -- pista:execute statements.
	if input.PreSQL != "" {
		fmt.Fprintln(w, input.PreSQL) //nolint:errcheck
		if err := execTimed(ctx, input.PreSQL); err != nil {
			return fmt.Errorf("failed to execute pre-SQL: %w", err)
		}
	}

	// concurrently-pre-SQL is gated on HasConcurrentlyIndex so it only runs
	// when there is CONCURRENTLY index DDL to apply. WithTx + HasConcurrentlyIndex
	// is rejected above and TryTx opens no transaction in that case, so this
	// always runs outside a transaction.
	if input.ConcurrentlyPreSQL != "" && input.HasConcurrentlyIndex {
		fmt.Fprintln(w, input.ConcurrentlyPreSQL) //nolint:errcheck
		if err := execTimed(ctx, input.ConcurrentlyPreSQL); err != nil {
			return fmt.Errorf("failed to execute concurrently-pre-SQL: %w", err)
		}
	}

	// Set search_path to the target schemas before running the managed DDL and
	// the -- pista:execute statements, so an unqualified user-type reference in
	// a column or attribute definition (e.g. "home addr") resolves. It is not
	// written to the output writer, matching the emitted-SQL contract.
	if _, err := exec(ctx, client.searchPathSQL()); err != nil {
		return fmt.Errorf("failed to set search_path: %w", err)
	}

	// runExecuteStmts runs the -- pista:execute statements whose First flag
	// matches. search_path is already set above. Check SQL is evaluated at the
	// point the statement runs, so an execute-first check sees the pre-change
	// schema while a plain execute check sees the post-change schema.
	runExecuteStmts := func(first bool) error {
		for _, es := range input.ExecuteStmts {
			if es.First != first {
				continue
			}

			shouldExecute := true

			if es.CheckSQL != "" {
				if err := queryRow(ctx, es.CheckSQL).Scan(&shouldExecute); err != nil {
					return fmt.Errorf("failed to evaluate check SQL: %s: %w", es.CheckSQL, err)
				}
			}

			if shouldExecute {
				fmt.Fprintln(w, parser.FormatExecuteStmt(es)) //nolint:errcheck
				if err := execTimed(ctx, es.SQL); err != nil {
					return fmt.Errorf("failed to execute SQL: %s: %w", es.SQL, err)
				}
				applied = true
			}
		}
		return nil
	}

	// Execute -- pista:execute-first statements before schema changes.
	if err := runExecuteStmts(true); err != nil {
		return err
	}

	for _, stmt := range input.Stmts {
		fmt.Fprintln(w, stmt) //nolint:errcheck
		if err := execTimed(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute SQL: %s: %w", stmt, err)
		}
		applied = true
	}

	// Execute -- pista:execute statements after schema changes.
	if err := runExecuteStmts(false); err != nil {
		return err
	}

	if err := commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	result.Applied = applied
	if applied {
		result.Duration = time.Since(start)
	}

	return nil
}
