package pistachio

import (
	"context"
	"fmt"
	"strings"

	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/parser"
)

type PlanOptions struct {
	Files []string `arg:"" help:"Path to the desired schema SQL file(s)."`
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

	desired, err := parser.ParseSQLFilesWithSchema(options.Files, client.Schemas[0])
	if err != nil {
		return "", fmt.Errorf("failed to parse SQL file: %w", err)
	}

	stmts := diff.DiffTables(client.filterTables(currentTables), client.filterTables(client.reverseRemapTableSchemas(desired.Tables)))
	stmts = append(stmts, diff.DiffViews(client.filterViews(currentViews), client.filterViews(client.reverseRemapViewSchemas(desired.Views)))...)

	return strings.Join(stmts, "\n"), nil
}
