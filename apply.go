package pistachio

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/winebarrel/pistachio/parser"
)

type ApplyOptions struct {
	FilterOptions
	DropPolicy
	Files                    []string `arg:"" help:"Path to the desired schema SQL file(s)."`
	PreSQL                   string   `xor:"pre-sql" env:"PISTA_PRE_SQL" help:"SQL to execute before applying changes."`
	PreSQLFile               string   `type:"path" xor:"pre-sql" env:"PISTA_PRE_SQL_FILE" help:"Path to a SQL file to execute before applying changes."`
	ConcurrentlyPreSQL       string   `xor:"concurrently-pre-sql" env:"PISTA_CONCURRENTLY_PRE_SQL" help:"SQL to execute before CONCURRENTLY index DDL (e.g. SET lock_timeout). Runs outside any transaction, only when the diff contains CONCURRENTLY index DDL."`
	ConcurrentlyPreSQLFile   string   `type:"path" xor:"concurrently-pre-sql" env:"PISTA_CONCURRENTLY_PRE_SQL_FILE" help:"Path to a SQL file to execute before CONCURRENTLY index DDL."`
	WithTx                   bool     `xor:"tx-mode,tx-choice" env:"PISTA_WITH_TX" help:"Execute pre-SQL and schema changes in a transaction."`
	TryTx                    bool     `xor:"tx-choice" env:"PISTA_TRY_TX" help:"Execute pre-SQL and schema changes in a transaction when possible. A diff containing CONCURRENTLY index DDL runs without a transaction instead of failing."`
	DisableIndexConcurrently bool     `xor:"index-concurrently" env:"PISTA_DISABLE_INDEX_CONCURRENTLY" help:"Ignore CONCURRENTLY opt-ins (directive and inline) and emit plain CREATE/DROP INDEX."`
	ForceIndexConcurrently   bool     `xor:"index-concurrently,tx-mode" env:"PISTA_FORCE_INDEX_CONCURRENTLY" help:"Force CONCURRENTLY on every CREATE/DROP INDEX, including pure drops. Cannot be combined with --with-tx."`
	BulkAlter                bool     `env:"PISTA_BULK_ALTER" help:"Combine consecutive ALTER TABLE actions on the same table into a single statement. FK changes, RENAME, VALIDATE CONSTRAINT, RLS toggles, and skipped DROPs stay separate."`
	AssumeValidated          bool     `env:"PISTA_ASSUME_VALIDATED" help:"Treat every table constraint, domain constraint, and foreign key as validated: ignore NOT VALID and never emit VALIDATE CONSTRAINT."`
	Exclusive                bool     `xor:"exclusive" env:"PISTA_EXCLUSIVE" help:"Make apply runs on the same database mutually exclusive: fail immediately when another exclusive apply is running."`
	// ExclusiveWait enables the same mutual exclusion as Exclusive and waits
	// for the other apply instead of failing. A pointer because 0 is a valid
	// value (wait without limit) and must be distinguishable from "not set".
	// The type rejects a negative value at parse time.
	ExclusiveWait *UnsignedDuration `xor:"exclusive" env:"PISTA_EXCLUSIVE_WAIT" placeholder:"DURATION" help:"Like --exclusive, but wait up to the given duration (0 waits without limit) for the other apply to finish."`
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

func (client *Client) Apply(ctx context.Context, options *ApplyOptions, w io.Writer) (*ApplyResult, error) {
	if err := client.validateSchemas(); err != nil {
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
	if options.Exclusive || options.ExclusiveWait != nil {
		if err := acquireExclusive(ctx, conn, options.ExclusiveWait, w); err != nil {
			return nil, err
		}
	}

	result, err := client.diffAll(ctx, conn, &diffAllOptions{
		FilterOptions:            options.FilterOptions,
		DropPolicy:               options.DropPolicy,
		Files:                    options.Files,
		PreSQL:                   options.PreSQL,
		PreSQLFile:               options.PreSQLFile,
		ConcurrentlyPreSQL:       options.ConcurrentlyPreSQL,
		ConcurrentlyPreSQLFile:   options.ConcurrentlyPreSQLFile,
		DisableIndexConcurrently: options.DisableIndexConcurrently,
		ForceIndexConcurrently:   options.ForceIndexConcurrently,
		BulkAlter:                options.BulkAlter,
		AssumeValidated:          options.AssumeValidated,
	})
	if err != nil {
		return nil, err
	}

	// --with-tx is an explicit all-or-nothing request, so CONCURRENTLY index
	// DDL (which PostgreSQL cannot run inside a transaction) stays an error.
	// --try-tx asks for a transaction only when one is possible, so the same
	// diff runs without one.
	if options.WithTx && result.HasConcurrentlyIndex {
		return nil, fmt.Errorf("--with-tx cannot be used with CONCURRENTLY index operations")
	}
	withTx := options.WithTx || (options.TryTx && !result.HasConcurrentlyIndex)

	applyResult := &ApplyResult{
		Count:           result.Count,
		DisallowedDrops: strings.Join(result.DisallowedDrops, "\n"),
		Ignored:         strings.Join(result.Ignored, "\n"),
	}

	if len(result.Stmts) == 0 && len(result.ExecuteStmts) == 0 {
		return applyResult, nil
	}

	start := time.Now()
	applied := false

	exec := conn.Exec
	queryRow := conn.QueryRow
	commit := func(context.Context) error { return nil }

	if withTx {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		fmt.Fprintln(w, "-- Transaction started") //nolint:errcheck
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
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			committed = true
			fmt.Fprintln(w, "-- Transaction committed") //nolint:errcheck
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
	if result.PreSQL != "" {
		fmt.Fprintln(w, result.PreSQL) //nolint:errcheck
		if _, err := exec(ctx, result.PreSQL); err != nil {
			return nil, fmt.Errorf("failed to execute pre-SQL: %w", err)
		}
	}

	// concurrently-pre-SQL is gated on HasConcurrentlyIndex so it only runs
	// when there is CONCURRENTLY index DDL to apply. WithTx + HasConcurrentlyIndex
	// is rejected above and TryTx opens no transaction in that case, so this
	// always runs outside a transaction.
	if result.ConcurrentlyPreSQL != "" && result.HasConcurrentlyIndex {
		fmt.Fprintln(w, result.ConcurrentlyPreSQL) //nolint:errcheck
		if _, err := exec(ctx, result.ConcurrentlyPreSQL); err != nil {
			return nil, fmt.Errorf("failed to execute concurrently-pre-SQL: %w", err)
		}
	}

	// Set search_path to the target schemas before running the managed DDL and
	// the -- pista:execute statements, so an unqualified user-type reference in
	// a column or attribute definition (e.g. "home addr") resolves. It is not
	// written to the output writer, matching the emitted-SQL contract.
	if _, err := exec(ctx, client.searchPathSQL()); err != nil {
		return nil, fmt.Errorf("failed to set search_path: %w", err)
	}

	// runExecuteStmts runs the -- pista:execute statements whose First flag
	// matches. search_path is already set above. Check SQL is evaluated at the
	// point the statement runs, so an execute-first check sees the pre-change
	// schema while a plain execute check sees the post-change schema.
	runExecuteStmts := func(first bool) error {
		for _, es := range result.ExecuteStmts {
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
				if _, err := exec(ctx, es.SQL); err != nil {
					return fmt.Errorf("failed to execute SQL: %s: %w", es.SQL, err)
				}
				applied = true
			}
		}
		return nil
	}

	// Execute -- pista:execute-first statements before schema changes.
	if err := runExecuteStmts(true); err != nil {
		return nil, err
	}

	for _, stmt := range result.Stmts {
		fmt.Fprintln(w, stmt) //nolint:errcheck
		if _, err := exec(ctx, stmt); err != nil {
			return nil, fmt.Errorf("failed to execute SQL: %s: %w", stmt, err)
		}
		applied = true
	}

	// Execute -- pista:execute statements after schema changes.
	if err := runExecuteStmts(false); err != nil {
		return nil, err
	}

	if err := commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	applyResult.Applied = applied
	if applied {
		applyResult.Duration = time.Since(start)
	}

	return applyResult, nil
}
