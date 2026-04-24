package pistachio

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/diff"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
	"github.com/winebarrel/pistachio/toposort"
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

	desiredEnums := options.filterEnums(client.reverseRemapEnumSchemas(desired.Enums))
	desiredDomains := options.filterDomains(client.reverseRemapDomainSchemas(desired.Domains))
	desiredTables := options.filterTables(client.reverseRemapTableSchemas(desired.Tables))
	desiredViews := options.filterViews(client.reverseRemapViewSchemas(desired.Views))

	enumDiff, err := diff.DiffEnums(filteredEnums, desiredEnums, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff enums: %w", err)
	}

	domainDiff, err := diff.DiffDomains(filteredDomains, desiredDomains, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff domains: %w", err)
	}

	tableDiff, err := diff.DiffTables(filteredTables, desiredTables, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff tables: %w", err)
	}

	viewDiff, err := diff.DiffViews(filteredViews, desiredViews, &options.DropPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to diff views: %w", err)
	}

	stmts := orderStatements(
		desiredEnums, desiredDomains, desiredTables, desiredViews,
		enumDiff, domainDiff, tableDiff, viewDiff,
	)

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

// orderStatements uses topological sort to determine the correct execution
// order for diff statements based on object dependencies.
// Falls back to the default category-based ordering if topological sort fails.
func orderStatements(
	desiredEnums *orderedmap.Map[string, *model.Enum],
	desiredDomains *orderedmap.Map[string, *model.Domain],
	desiredTables *orderedmap.Map[string, *model.Table],
	desiredViews *orderedmap.Map[string, *model.View],
	enumDiff *diff.EnumDiffResult,
	domainDiff *diff.DomainDiffResult,
	tableDiff *diff.TableDiffResult,
	viewDiff *diff.ViewDiffResult,
) []string {
	// Build topological order from desired schema
	order, err := toposort.OrderFromSchema(
		desiredEnums,
		desiredDomains,
		desiredTables,
		desiredViews,
	)
	if err != nil {
		// Fallback to hardcoded order (e.g., cyclic FK dependencies)
		return fallbackOrder(enumDiff, domainDiff, tableDiff, viewDiff)
	}

	posMap := make(map[string]int, len(order))
	for i, name := range order {
		posMap[name] = i
	}

	// Phase 1: Creates/modifications in topological order
	var createStmts []taggedStmt
	createStmts = append(createStmts, tagStatements(enumDiff.Stmts, posMap)...)
	createStmts = append(createStmts, tagStatements(domainDiff.Stmts, posMap)...)
	createStmts = append(createStmts, tagStatements(tableDiff.Stmts, posMap)...)
	sort.SliceStable(createStmts, func(i, j int) bool {
		return createStmts[i].pos < createStmts[j].pos
	})

	// Phase 2: Drops in reverse topological order (dependents dropped first)
	var dropStmts []taggedStmt
	dropStmts = append(dropStmts, tagStatements(viewDiff.DropStmts, posMap)...)
	dropStmts = append(dropStmts, tagStatements(tableDiff.DropStmts, posMap)...)
	dropStmts = append(dropStmts, tagStatements(domainDiff.DropStmts, posMap)...)
	dropStmts = append(dropStmts, tagStatements(enumDiff.DropStmts, posMap)...)
	sort.SliceStable(dropStmts, func(i, j int) bool {
		return dropStmts[i].pos > dropStmts[j].pos // reverse order
	})

	// Phase 3: View creates in topological order
	var viewCreateStmts []taggedStmt
	viewCreateStmts = append(viewCreateStmts, tagStatements(viewDiff.CreateStmts, posMap)...)
	sort.SliceStable(viewCreateStmts, func(i, j int) bool {
		return viewCreateStmts[i].pos < viewCreateStmts[j].pos
	})

	// Assemble: FK drops → drops → creates → FK adds → view creates
	var stmts []string
	for _, ts := range tagStatements(tableDiff.FKDropStmts, posMap) {
		stmts = append(stmts, ts.sql)
	}
	for _, ts := range dropStmts {
		stmts = append(stmts, ts.sql)
	}
	for _, ts := range createStmts {
		stmts = append(stmts, ts.sql)
	}
	for _, ts := range tagStatements(tableDiff.FKAddStmts, posMap) {
		stmts = append(stmts, ts.sql)
	}
	for _, ts := range viewCreateStmts {
		stmts = append(stmts, ts.sql)
	}

	return stmts
}

// fallbackOrder is the original hardcoded ordering logic used as fallback.
func fallbackOrder(
	enumDiff *diff.EnumDiffResult,
	domainDiff *diff.DomainDiffResult,
	tableDiff *diff.TableDiffResult,
	viewDiff *diff.ViewDiffResult,
) []string {
	var stmts []string
	stmts = append(stmts, enumDiff.Stmts...)
	stmts = append(stmts, domainDiff.Stmts...)
	stmts = append(stmts, viewDiff.DropStmts...)
	stmts = append(stmts, tableDiff.FKDropStmts...)
	stmts = append(stmts, tableDiff.Stmts...)
	stmts = append(stmts, tableDiff.DropStmts...)
	stmts = append(stmts, domainDiff.DropStmts...)
	stmts = append(stmts, enumDiff.DropStmts...)
	stmts = append(stmts, tableDiff.FKAddStmts...)
	stmts = append(stmts, viewDiff.CreateStmts...)
	return stmts
}

// taggedStmt pairs a SQL statement with a sort position derived from
// the topological order of the object it affects.
type taggedStmt struct {
	sql string
	pos int
}

// tagStatements extracts object names from SQL statements and assigns
// topological positions. Statements whose object can't be identified
// get position -1 (sorted first) to preserve safety.
func tagStatements(stmts []string, posMap map[string]int) []taggedStmt {
	tagged := make([]taggedStmt, len(stmts))
	for i, sql := range stmts {
		name := extractObjectName(sql)
		pos := -1
		if p, ok := posMap[name]; ok {
			pos = p
		}
		tagged[i] = taggedStmt{sql: sql, pos: pos}
	}
	return tagged
}

// extractObjectName extracts the primary schema-qualified object name from a DDL statement.
func extractObjectName(sql string) string {
	sql = strings.TrimSpace(sql)

	// Try common DDL patterns
	patterns := []struct {
		prefix string
	}{
		{"CREATE TABLE "},
		{"CREATE UNLOGGED TABLE "},
		{"CREATE TYPE "},
		{"CREATE DOMAIN "},
		{"CREATE OR REPLACE VIEW "},
		{"CREATE VIEW "},
		{"CREATE INDEX "}, // special: name is after ON
		{"ALTER TABLE ONLY "},
		{"ALTER TABLE "},
		{"ALTER TYPE "},
		{"ALTER DOMAIN "},
		{"ALTER VIEW "},
		{"DROP TABLE "},
		{"DROP VIEW "},
		{"DROP TYPE "},
		{"DROP DOMAIN "},
		{"COMMENT ON TABLE "},
		{"COMMENT ON VIEW "},
		{"COMMENT ON TYPE "},
		{"COMMENT ON DOMAIN "},
		{"COMMENT ON COLUMN "}, // schema.table.column → take schema.table
	}

	upper := strings.ToUpper(sql)
	for _, p := range patterns {
		if !strings.HasPrefix(upper, strings.ToUpper(p.prefix)) {
			continue
		}

		if strings.HasPrefix(upper, "CREATE INDEX ") {
			// CREATE INDEX name ON [ONLY] schema.table ...
			return extractIndexTable(sql)
		}

		rest := sql[len(p.prefix):]
		name := extractFirstIdentifier(rest)

		if strings.HasPrefix(upper, "COMMENT ON COLUMN ") {
			// schema.table.column → schema.table
			parts := strings.SplitN(name, ".", 3)
			if len(parts) >= 2 {
				return parts[0] + "." + parts[1]
			}
		}

		return name
	}

	return ""
}

// extractFirstIdentifier extracts a possibly schema-qualified identifier
// from the beginning of a string.
func extractFirstIdentifier(s string) string {
	s = strings.TrimSpace(s)
	var result strings.Builder
	inQuote := false

	for _, ch := range s {
		if ch == '"' {
			inQuote = !inQuote
			result.WriteRune(ch)
			continue
		}
		if inQuote {
			result.WriteRune(ch)
			continue
		}
		if ch == '.' || ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
			result.WriteRune(ch)
			continue
		}
		break
	}

	name := result.String()
	// Remove quotes for matching
	name = strings.ReplaceAll(name, "\"", "")
	return name
}

// extractIndexTable extracts the table name from a CREATE INDEX statement.
func extractIndexTable(sql string) string {
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, " ON ")
	if idx < 0 {
		return ""
	}
	rest := sql[idx+4:]
	rest = strings.TrimSpace(rest)

	// Skip optional ONLY keyword
	if strings.HasPrefix(strings.ToUpper(rest), "ONLY ") {
		rest = rest[5:]
	}

	return extractFirstIdentifier(rest)
}
