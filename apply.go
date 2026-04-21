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
	Files      []string `arg:"" help:"Path to the desired schema SQL file(s)."`
	PreSQLFile string   `type:"path" help:"Path to a SQL file to execute before applying changes."`
	WithTx     bool     `help:"Execute the pre-SQL and schema changes in a transaction."`
}

func (client *Client) Apply(ctx context.Context, options *ApplyOptions, w io.Writer) error {
	conn, err := client.connect()
	if err != nil {
		return err
	}
	defer conn.Close(ctx) //nolint:errcheck

	cat, err := catalog.NewCatalog(conn, client.Schemas)
	if err != nil {
		return fmt.Errorf("failed to create catalog: %w", err)
	}

	currentTables, err := cat.Tables(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch tables: %w", err)
	}

	currentViews, err := cat.Views(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch views: %w", err)
	}

	currentEnums, err := cat.Enums(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch enums: %w", err)
	}

	desired, err := parser.ParseSQLFilesWithSchema(options.Files, client.Schemas[0])
	if err != nil {
		return fmt.Errorf("failed to parse SQL file: %w", err)
	}

	enumDiff, err := diff.DiffEnums(client.filterEnums(currentEnums), client.filterEnums(client.reverseRemapEnumSchemas(desired.Enums)))
	if err != nil {
		return err
	}
	stmts := enumDiff.Stmts

	tableStmts, err := diff.DiffTables(client.filterTables(currentTables), client.filterTables(client.reverseRemapTableSchemas(desired.Tables)))
	if err != nil {
		return err
	}
	stmts = append(stmts, tableStmts...)

	viewStmts, err := diff.DiffViews(client.filterViews(currentViews), client.filterViews(client.reverseRemapViewSchemas(desired.Views)))
	if err != nil {
		return err
	}
	stmts = append(stmts, viewStmts...)

	stmts = append(stmts, enumDiff.DropStmts...)

	var preSQL string
	if options.PreSQLFile != "" {
		rawPreSQL, err := os.ReadFile(options.PreSQLFile)
		if err != nil {
			return fmt.Errorf("failed to read pre-SQL file: %s: %w", options.PreSQLFile, err)
		}
		preSQL = string(rawPreSQL)
	}

	if len(stmts) == 0 {
		return nil
	}

	exec := conn.Exec
	commit := func(context.Context) error { return nil }

	if options.WithTx {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		exec = tx.Exec
		commit = tx.Commit
	}

	if preSQL != "" {
		fmt.Fprintln(w, preSQL) //nolint:errcheck
		if _, err := exec(ctx, preSQL); err != nil {
			return fmt.Errorf("failed to execute pre-SQL: %w", err)
		}
	}

	for _, stmt := range stmts {
		fmt.Fprintln(w, stmt) //nolint:errcheck
		if _, err := exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute SQL: %s: %w", stmt, err)
		}
	}

	if err := commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
