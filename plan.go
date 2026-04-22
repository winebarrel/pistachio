package pistachio

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/parser"
)

type PlanOptions struct {
	FilterOptions
	DropPolicy
	Files      []string `arg:"" help:"Path to the desired schema SQL file(s)."`
	PreSQLFile string   `type:"path" help:"Path to a SQL file to execute before applying changes."`
}

func (client *Client) Plan(ctx context.Context, options *PlanOptions) (string, error) {
	conn, err := client.connect()
	if err != nil {
		return "", err
	}
	defer conn.Close(ctx) //nolint:errcheck

	cat, err := catalog.NewCatalog(conn, client.Schemas)
	if err != nil {
		return "", fmt.Errorf("failed to create catalog: %w", err)
	}

	currentTables, err := cat.Tables(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch tables: %w", err)
	}

	currentViews, err := cat.Views(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch views: %w", err)
	}

	currentEnums, err := cat.Enums(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch enums: %w", err)
	}

	currentDomains, err := cat.Domains(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch domains: %w", err)
	}

	desired, err := parser.ParseSQLFilesWithSchema(options.Files, client.Schemas[0])
	if err != nil {
		return "", fmt.Errorf("failed to parse SQL file: %w", err)
	}

	enumDiff, err := diff.DiffEnums(options.filterEnums(currentEnums), options.filterEnums(client.reverseRemapEnumSchemas(desired.Enums)), &options.DropPolicy)
	if err != nil {
		return "", err
	}
	stmts := enumDiff.Stmts

	domainDiff, err := diff.DiffDomains(options.filterDomains(currentDomains), options.filterDomains(client.reverseRemapDomainSchemas(desired.Domains)), &options.DropPolicy)
	if err != nil {
		return "", fmt.Errorf("failed to diff domains: %w", err)
	}
	stmts = append(stmts, domainDiff.Stmts...)

	tableStmts, err := diff.DiffTables(options.filterTables(currentTables), options.filterTables(client.reverseRemapTableSchemas(desired.Tables)), &options.DropPolicy)
	if err != nil {
		return "", fmt.Errorf("failed to diff tables: %w", err)
	}
	stmts = append(stmts, tableStmts...)

	viewStmts, err := diff.DiffViews(options.filterViews(currentViews), options.filterViews(client.reverseRemapViewSchemas(desired.Views)), &options.DropPolicy)
	if err != nil {
		return "", fmt.Errorf("failed to diff views: %w", err)
	}
	stmts = append(stmts, viewStmts...)

	stmts = append(stmts, domainDiff.DropStmts...)
	stmts = append(stmts, enumDiff.DropStmts...)

	if options.PreSQLFile != "" && len(stmts) > 0 {
		rawPreSQL, err := os.ReadFile(options.PreSQLFile)
		if err != nil {
			return "", fmt.Errorf("failed to read pre-SQL file: %s: %w", options.PreSQLFile, err)
		}
		stmts = append([]string{string(rawPreSQL)}, stmts...)
	}

	return strings.Join(stmts, "\n"), nil
}
