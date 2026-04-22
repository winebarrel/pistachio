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

// ObjectCount holds the number of objects inspected by type.
type ObjectCount struct {
	Tables  int
	Views   int
	Enums   int
	Domains int
}

func (c ObjectCount) String() string {
	return fmt.Sprintf("%d tables, %d views, %d enums, %d domains", c.Tables, c.Views, c.Enums, c.Domains)
}

// PlanResult holds the result of a Plan operation.
type PlanResult struct {
	SQL   string
	Count ObjectCount
}

func (client *Client) Plan(ctx context.Context, options *PlanOptions) (*PlanResult, error) {
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

	count := ObjectCount{
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

	if options.PreSQLFile != "" && len(stmts) > 0 {
		rawPreSQL, err := os.ReadFile(options.PreSQLFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read pre-SQL file: %s: %w", options.PreSQLFile, err)
		}
		stmts = append([]string{string(rawPreSQL)}, stmts...)
	}

	return &PlanResult{
		SQL:   strings.Join(stmts, "\n"),
		Count: count,
	}, nil
}
