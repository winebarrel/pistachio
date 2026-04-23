package pistachio

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/parser"
)

// diffAllOptions holds the common options for diffAll.
type diffAllOptions struct {
	FilterOptions
	DropPolicy
	Files      []string
	PreSQL     string
	PreSQLFile string
}

// diffAllResult holds the result of diffAll.
type diffAllResult struct {
	Stmts  []string
	PreSQL string
	Count  ObjectCount
}

// diffAll performs the common catalog fetch, parse, diff, and statement
// ordering logic shared by Plan and Apply.
func (client *Client) diffAll(ctx context.Context, conn *pgx.Conn, options *diffAllOptions) (*diffAllResult, error) {
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
		Schemas: client.Schemas,
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

	tableDiff, err := diff.DiffTables(filteredTables, options.filterTables(client.reverseRemapTableSchemas(desired.Tables)), &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff tables: %w", err)
	}
	viewDiff, err := diff.DiffViews(filteredViews, options.filterViews(client.reverseRemapViewSchemas(desired.Views)), &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff views: %w", err)
	}

	// Drop views before table/column changes (views may depend on columns being dropped)
	stmts = append(stmts, viewDiff.DropStmts...)

	// FK drops before table/column changes
	stmts = append(stmts, tableDiff.FKDropStmts...)
	stmts = append(stmts, tableDiff.Stmts...)

	// Drops: tables before domains/enums (dependency order)
	stmts = append(stmts, tableDiff.DropStmts...)
	stmts = append(stmts, domainDiff.DropStmts...)
	stmts = append(stmts, enumDiff.DropStmts...)

	// FK adds after all tables exist
	stmts = append(stmts, tableDiff.FKAddStmts...)

	// View creates last (views may reference new tables/columns/FKs)
	stmts = append(stmts, viewDiff.CreateStmts...)

	preSQL, err := resolvePreSQL(options.PreSQL, options.PreSQLFile)
	if err != nil {
		return nil, err
	}

	return &diffAllResult{
		Stmts:  stmts,
		PreSQL: preSQL,
		Count:  count,
	}, nil
}
