package pistachio

import (
	"context"
	"fmt"
	"io"

	"github.com/winebarrel/pistachio/parser"
)

type ApplyOptions struct {
	FilterOptions
	DropPolicy
	Files      []string `arg:"" help:"Path to the desired schema SQL file(s)."`
	PreSQL     string   `xor:"pre-sql" help:"SQL to execute before applying changes."`
	PreSQLFile string   `type:"path" xor:"pre-sql" help:"Path to a SQL file to execute before applying changes."`
	WithTx     bool     `help:"Execute the pre-SQL and schema changes in a transaction."`
}

func (client *Client) Apply(ctx context.Context, options *ApplyOptions, w io.Writer) (*ObjectCount, error) {
	conn, err := client.connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx) //nolint:errcheck

	result, err := client.diffAll(ctx, conn, &diffAllOptions{
		FilterOptions: options.FilterOptions,
		DropPolicy:    options.DropPolicy,
		Files:         options.Files,
		PreSQL:        options.PreSQL,
		PreSQLFile:    options.PreSQLFile,
	})
	if err != nil {
		return nil, err
	}

	count := &result.Count

	if len(result.Stmts) == 0 && len(result.ExecuteStmts) == 0 {
		return count, nil
	}

	exec := conn.Exec
	queryRow := conn.QueryRow
	commit := func(context.Context) error { return nil }

	if options.WithTx {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		exec = tx.Exec
		queryRow = tx.QueryRow
		commit = tx.Commit
	}

	if result.PreSQL != "" {
		fmt.Fprintln(w, result.PreSQL) //nolint:errcheck
		if _, err := exec(ctx, result.PreSQL); err != nil {
			return nil, fmt.Errorf("failed to execute pre-SQL: %w", err)
		}
	}

	for _, stmt := range result.Stmts {
		fmt.Fprintln(w, stmt) //nolint:errcheck
		if _, err := exec(ctx, stmt); err != nil {
			return nil, fmt.Errorf("failed to execute SQL: %s: %w", stmt, err)
		}
	}

	// Execute -- pist:execute statements after schema changes
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

	return count, nil
}
