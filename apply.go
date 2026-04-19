package pistachio

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/parser"
)

type ApplyOptions struct {
	File       string `arg:"" help:"Path to the desired schema SQL file."`
	PreSQLFile string `type:"path" help:"Path to a SQL file to execute before applying changes."`
	WithTx     bool   `help:"Execute the pre-SQL and schema changes in a transaction."`
}

func (client *Client) Apply(ctx context.Context, options *ApplyOptions, w io.Writer) (bool, error) {
	conn, err := client.connect()
	if err != nil {
		return false, err
	}
	defer conn.Close(ctx) //nolint:errcheck

	cat, err := catalog.NewCatalog(conn, client.Schemas)
	if err != nil {
		return false, fmt.Errorf("failed to create catalog: %w", err)
	}

	currentTables, err := cat.Tables(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to fetch tables: %w", err)
	}

	currentViews, err := cat.Views(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to fetch views: %w", err)
	}

	desired, err := parser.ParseSQLFile(options.File)
	if err != nil {
		return false, fmt.Errorf("failed to parse SQL file: %w", err)
	}

	stmts := diff.DiffTables(currentTables, desired.Tables)
	stmts = append(stmts, diff.DiffViews(currentViews, desired.Views)...)

	var preSQL string
	if options.PreSQLFile != "" {
		rawPreSQL, err := os.ReadFile(options.PreSQLFile)
		if err != nil {
			return false, fmt.Errorf("failed to read pre-SQL file: %s: %w", options.PreSQLFile, err)
		}
		preSQL = string(rawPreSQL)
	}

	if len(stmts) == 0 {
		return false, nil
	}

	exec := conn.Exec
	commit := func(context.Context) error { return nil }

	if options.WithTx {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		exec = tx.Exec
		commit = tx.Commit
	}

	if preSQL != "" {
		if _, err := exec(ctx, preSQL); err != nil {
			return false, fmt.Errorf("failed to execute pre-SQL: %w", err)
		}
	}

	for _, stmt := range stmts {
		fmt.Fprintln(w, stmt) //nolint:errcheck
		if _, err := exec(ctx, stmt); err != nil {
			return false, fmt.Errorf("failed to execute SQL: %s: %w", stmt, err)
		}
	}

	if err := commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return true, nil
}
