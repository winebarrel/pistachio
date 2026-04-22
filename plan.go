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
	FilterOptions
	DropPolicy
	Files      []string `arg:"" help:"Path to the desired schema SQL file(s)."`
	PreSQL     string   `xor:"pre-sql" help:"SQL to prepend to the plan output."`
	PreSQLFile string   `type:"path" xor:"pre-sql" help:"Path to a SQL file to prepend to the plan output."`
}

// ObjectCount holds the number of objects inspected by type.
type ObjectCount struct {
	Schemas []string
	Tables  int
	Views   int
	Enums   int
	Domains int
}

func (c ObjectCount) SchemaLabel() string {
	if len(c.Schemas) == 1 {
		return "schema " + c.Schemas[0]
	}
	return "schemas " + strings.Join(c.Schemas, ", ")
}

func (c ObjectCount) Summary() string {
	return fmt.Sprintf("%s, %s, %s, %s",
		pluralize(c.Tables, "table"),
		pluralize(c.Views, "view"),
		pluralize(c.Enums, "enum"),
		pluralize(c.Domains, "domain"),
	)
}

func pluralize(n int, singular string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, singular)
	}
	return fmt.Sprintf("%d %ss", n, singular)
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
	if preSQL != "" && len(stmts) > 0 {
		stmts = append([]string{preSQL}, stmts...)
	}

	return &PlanResult{
		SQL:   strings.Join(stmts, "\n"),
		Count: count,
	}, nil
}
