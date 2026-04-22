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
	FilterOptions
	DropPolicy
	Files      []string `arg:"" help:"Path to the desired schema SQL file(s)."`
	PreSQLFile string   `type:"path" help:"Path to a SQL file to execute before applying changes."`
	WithTx     bool     `help:"Execute the pre-SQL and schema changes in a transaction."`
}

func (client *Client) Apply(ctx context.Context, options *ApplyOptions, w io.Writer) (*ObjectCount, error) {
	conn, err := client.connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx) //nolint:errcheck

	cat, err := catalog.NewCatalog(conn, client.Schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	currentTables, err := cat.Tables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tables: %w", err)
	}

	currentViews, err := cat.Views(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch views: %w", err)
	}

	currentEnums, err := cat.Enums(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch enums: %w", err)
	}

	currentDomains, err := cat.Domains(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch domains: %w", err)
	}

	desired, err := parser.ParseSQLFilesWithSchema(options.Files, client.Schemas[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL file: %w", err)
	}

	filterDesiredBySchemas(desired, client.Schemas, client.SchemaMap)

	filteredTables := options.filterTables(currentTables)
	filteredViews := options.filterViews(currentViews)
	filteredEnums := options.filterEnums(currentEnums)
	filteredDomains := options.filterDomains(currentDomains)

	count := &ObjectCount{
		Tables:  filteredTables.Len(),
		Views:   filteredViews.Len(),
		Enums:   filteredEnums.Len(),
		Domains: filteredDomains.Len(),
	}

	enumDiff, err := diff.DiffEnums(filteredEnums, options.filterEnums(client.reverseRemapEnumSchemas(desired.Enums)), &options.DropPolicy)
	if err != nil {
		return nil, err
	}
	stmts := enumDiff.Stmts

	domainDiff, err := diff.DiffDomains(filteredDomains, options.filterDomains(client.reverseRemapDomainSchemas(desired.Domains)), &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff domains: %w", err)
	}
	stmts = append(stmts, domainDiff.Stmts...)

	tableStmts, err := diff.DiffTables(filteredTables, options.filterTables(client.reverseRemapTableSchemas(desired.Tables)), &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff tables: %w", err)
	}
	stmts = append(stmts, tableStmts...)

	viewStmts, err := diff.DiffViews(filteredViews, options.filterViews(client.reverseRemapViewSchemas(desired.Views)), &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff views: %w", err)
	}
	stmts = append(stmts, viewStmts...)

	stmts = append(stmts, domainDiff.DropStmts...)
	stmts = append(stmts, enumDiff.DropStmts...)

	var preSQL string
	if options.PreSQLFile != "" {
		rawPreSQL, err := os.ReadFile(options.PreSQLFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read pre-SQL file: %s: %w", options.PreSQLFile, err)
		}
		preSQL = string(rawPreSQL)
	}

	if len(stmts) == 0 {
		return count, nil
	}

	exec := conn.Exec
	commit := func(context.Context) error { return nil }

	if options.WithTx {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		exec = tx.Exec
		commit = tx.Commit
	}

	if preSQL != "" {
		fmt.Fprintln(w, preSQL) //nolint:errcheck
		if _, err := exec(ctx, preSQL); err != nil {
			return nil, fmt.Errorf("failed to execute pre-SQL: %w", err)
		}
	}

	for _, stmt := range stmts {
		fmt.Fprintln(w, stmt) //nolint:errcheck
		if _, err := exec(ctx, stmt); err != nil {
			return nil, fmt.Errorf("failed to execute SQL: %s: %w", stmt, err)
		}
	}

	if err := commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return count, nil
}
