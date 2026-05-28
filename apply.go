package pistachio

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/winebarrel/pistachio/model"
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
	WithTx                   bool     `env:"PISTA_WITH_TX" help:"Execute pre-SQL and schema changes in a transaction."`
	DisableIndexConcurrently bool     `env:"PISTA_DISABLE_INDEX_CONCURRENTLY" help:"Ignore CONCURRENTLY opt-ins (directive and inline) and emit plain CREATE/DROP INDEX."`
	BulkAlter                bool     `env:"PISTA_BULK_ALTER" help:"Combine consecutive ALTER TABLE actions on the same table into a single statement. FK changes, RENAME, VALIDATE CONSTRAINT, RLS toggles, and skipped DROPs stay separate."`
}

// ApplyResult holds the result of an Apply operation.
type ApplyResult struct {
	Count           ObjectCount
	DisallowedDrops string
}

func (client *Client) Apply(ctx context.Context, options *ApplyOptions, w io.Writer) (*ApplyResult, error) {
	if err := client.validateSchemas(); err != nil {
		return nil, err
	}
	conn, err := client.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx) //nolint:errcheck

	result, err := client.diffAll(ctx, conn, &diffAllOptions{
		FilterOptions:            options.FilterOptions,
		DropPolicy:               options.DropPolicy,
		Files:                    options.Files,
		PreSQL:                   options.PreSQL,
		PreSQLFile:               options.PreSQLFile,
		ConcurrentlyPreSQL:       options.ConcurrentlyPreSQL,
		ConcurrentlyPreSQLFile:   options.ConcurrentlyPreSQLFile,
		DisableIndexConcurrently: options.DisableIndexConcurrently,
		BulkAlter:                options.BulkAlter,
	})
	if err != nil {
		return nil, err
	}

	if options.WithTx && result.HasConcurrentlyIndex {
		return nil, fmt.Errorf("--with-tx cannot be used with CONCURRENTLY index operations")
	}

	applyResult := &ApplyResult{
		Count:           result.Count,
		DisallowedDrops: strings.Join(result.DisallowedDrops, "\n"),
	}

	if len(result.Stmts) == 0 && len(result.ExecuteStmts) == 0 {
		return applyResult, nil
	}

	exec := conn.Exec
	queryRow := conn.QueryRow
	commit := func(context.Context) error { return nil }

	if options.WithTx {
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
	}

	if result.PreSQL != "" {
		fmt.Fprintln(w, result.PreSQL) //nolint:errcheck
		if _, err := exec(ctx, result.PreSQL); err != nil {
			return nil, fmt.Errorf("failed to execute pre-SQL: %w", err)
		}
	}

	// concurrently-pre-SQL is gated on HasConcurrentlyIndex so it only runs
	// when there is CONCURRENTLY index DDL to apply. WithTx + HasConcurrentlyIndex
	// is rejected above, so this always runs outside a transaction.
	if result.ConcurrentlyPreSQL != "" && result.HasConcurrentlyIndex {
		fmt.Fprintln(w, result.ConcurrentlyPreSQL) //nolint:errcheck
		if _, err := exec(ctx, result.ConcurrentlyPreSQL); err != nil {
			return nil, fmt.Errorf("failed to execute concurrently-pre-SQL: %w", err)
		}
	}

	for _, stmt := range result.Stmts {
		fmt.Fprintln(w, stmt) //nolint:errcheck
		if _, err := exec(ctx, stmt); err != nil {
			return nil, fmt.Errorf("failed to execute SQL: %s: %w", stmt, err)
		}
	}

	// Execute -- pista:execute statements after schema changes.
	// Set search_path so unqualified names resolve to the configured schemas.
	if len(result.ExecuteStmts) > 0 && len(client.Schemas) > 0 {
		quoted := make([]string, len(client.Schemas))
		for i, s := range client.Schemas {
			quoted[i] = model.Ident(s)
		}
		searchPath := "SET search_path TO " + strings.Join(quoted, ", ")
		if _, err := exec(ctx, searchPath); err != nil {
			return nil, fmt.Errorf("failed to set search_path: %w", err)
		}
	}

	for _, es := range result.ExecuteStmts {
		shouldExecute := true

		if es.CheckSQL != "" {
			if err := queryRow(ctx, es.CheckSQL).Scan(&shouldExecute); err != nil {
				return nil, fmt.Errorf("failed to evaluate check SQL: %s: %w", es.CheckSQL, err)
			}
		}

		if shouldExecute {
			fmt.Fprintln(w, parser.FormatExecuteStmt(es)) //nolint:errcheck
			if _, err := exec(ctx, es.SQL); err != nil {
				return nil, fmt.Errorf("failed to execute SQL: %s: %w", es.SQL, err)
			}
		}
	}

	if err := commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return applyResult, nil
}
