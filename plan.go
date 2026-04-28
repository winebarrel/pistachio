package pistachio

import (
	"context"
	"fmt"
	"strings"

	"github.com/winebarrel/pistachio/parser"
)

type PlanOptions struct {
	FilterOptions
	DropPolicy
	Files                    []string `arg:"" help:"Path to the desired schema SQL file(s)."`
	PreSQL                   string   `xor:"pre-sql" env:"PIST_PRE_SQL" help:"SQL to prepend to the plan output."`
	PreSQLFile               string   `type:"path" xor:"pre-sql" env:"PIST_PRE_SQL_FILE" help:"Path to a SQL file to prepend to the plan output."`
	ConcurrentlyPreSQL       string   `xor:"concurrently-pre-sql" env:"PIST_CONCURRENTLY_PRE_SQL" help:"SQL to run before CONCURRENTLY index operations (e.g. SET lock_timeout). Only emitted when the diff includes CONCURRENTLY index DDL."`
	ConcurrentlyPreSQLFile   string   `type:"path" xor:"concurrently-pre-sql" env:"PIST_CONCURRENTLY_PRE_SQL_FILE" help:"Path to a SQL file to run before CONCURRENTLY index operations."`
	DisableIndexConcurrently bool     `env:"PIST_DISABLE_INDEX_CONCURRENTLY" help:"Ignore all CONCURRENTLY opt-ins (both -- pist:concurrently directives and inline CREATE/DROP INDEX CONCURRENTLY) and emit plain CREATE/DROP INDEX."`
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
	SQL             string
	DisallowedDrops string
	Count           ObjectCount
}

func (client *Client) Plan(ctx context.Context, options *PlanOptions) (*PlanResult, error) {
	conn, err := client.connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx) //nolint:errcheck

	result, err := client.diffAll(ctx, conn, &diffAllOptions{
		FilterOptions:            options.FilterOptions,
		DropPolicy:               options.DropPolicy,
		Files:                    options.Files,
		PreSQL:                   options.PreSQL,
		PreSQLFile:               options.PreSQLFile,
		ConcurrentlyPreSQL:       options.ConcurrentlyPreSQL,
		ConcurrentlyPreSQLFile:   options.ConcurrentlyPreSQLFile,
		DisableIndexConcurrently: options.DisableIndexConcurrently,
	})
	if err != nil {
		return nil, err
	}

	stmts := result.Stmts

	// Append execute statements after schema changes
	for _, es := range result.ExecuteStmts {
		stmts = append(stmts, parser.FormatExecuteStmt(es))
	}

	// Prefix order matches apply: PreSQL → concurrently-pre-SQL → DDL.
	// Skipped entirely when there is nothing to execute, so an empty plan
	// stays empty instead of leaking a bare SET / pre-SQL line.
	if len(stmts) > 0 {
		var prefix []string
		if result.PreSQL != "" {
			prefix = append(prefix, result.PreSQL)
		}
		if result.ConcurrentlyPreSQL != "" && result.HasConcurrentlyIndex {
			prefix = append(prefix, result.ConcurrentlyPreSQL)
		}
		stmts = append(prefix, stmts...)
	}

	return &PlanResult{
		SQL:             strings.Join(stmts, "\n"),
		DisallowedDrops: strings.Join(result.DisallowedDrops, "\n"),
		Count:           result.Count,
	}, nil
}
