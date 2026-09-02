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
	PreSQL                   string   `xor:"pre-sql" env:"PISTA_PRE_SQL" help:"SQL to prepend to the plan output."`
	PreSQLFile               string   `type:"path" xor:"pre-sql" env:"PISTA_PRE_SQL_FILE" help:"Path to a SQL file to prepend to the plan output."`
	ConcurrentlyPreSQL       string   `xor:"concurrently-pre-sql" env:"PISTA_CONCURRENTLY_PRE_SQL" help:"SQL to run before CONCURRENTLY index DDL (e.g. SET lock_timeout). Emitted only when the diff contains CONCURRENTLY index DDL."`
	ConcurrentlyPreSQLFile   string   `type:"path" xor:"concurrently-pre-sql" env:"PISTA_CONCURRENTLY_PRE_SQL_FILE" help:"Path to a SQL file to run before CONCURRENTLY index DDL."`
	DisableIndexConcurrently bool     `xor:"index-concurrently" env:"PISTA_DISABLE_INDEX_CONCURRENTLY" help:"Ignore CONCURRENTLY opt-ins (directive and inline) and emit plain CREATE/DROP INDEX."`
	ForceIndexConcurrently   bool     `xor:"index-concurrently" env:"PISTA_FORCE_INDEX_CONCURRENTLY" help:"Force CONCURRENTLY on every CREATE/DROP INDEX, including pure drops."`
	BulkAlter                bool     `env:"PISTA_BULK_ALTER" help:"Combine consecutive ALTER TABLE actions on the same table into a single statement. FK changes, RENAME, VALIDATE CONSTRAINT, RLS toggles, and skipped DROPs stay separate."`
	AssumeValidated          bool     `env:"PISTA_ASSUME_VALIDATED" help:"Treat every table constraint, domain constraint, and foreign key as validated: ignore NOT VALID and never emit VALIDATE CONSTRAINT."`
	NoReadOnly               bool     `env:"PISTA_NO_READ_ONLY" help:"Open the database connection read-write. By default plan uses a read-only connection."`
}

// ObjectCount holds the number of objects inspected by type.
type ObjectCount struct {
	Schemas        []string
	Tables         int
	Views          int
	Enums          int
	Domains        int
	CompositeTypes int
	Sequences      int
	// Routines is nil unless --manage-routine is set. A nil value leaves the
	// slot out of Summary entirely, so the line reads exactly as it did
	// before routines were managed.
	Routines *int
}

func (c ObjectCount) SchemaLabel() string {
	if len(c.Schemas) == 1 {
		return "schema " + c.Schemas[0]
	}
	return "schemas " + strings.Join(c.Schemas, ", ")
}

func (c ObjectCount) Summary() string {
	parts := []string{
		pluralize(c.Tables, "table"),
		pluralize(c.Views, "view"),
		pluralize(c.Enums, "enum"),
		pluralize(c.Domains, "domain"),
		pluralize(c.CompositeTypes, "composite type"),
		pluralize(c.Sequences, "sequence"),
	}
	if c.Routines != nil {
		parts = append(parts, pluralize(*c.Routines, "routine"))
	}
	return strings.Join(parts, ", ")
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
	Ignored         string
	Count           ObjectCount
	// HasChanges is true when the plan contains executable statements
	// (DDL or execute directives). Suppressed drops do not count.
	HasChanges bool
}

func (client *Client) Plan(ctx context.Context, options *PlanOptions) (*PlanResult, error) {
	if err := client.validateSchemas(); err != nil {
		return nil, err
	}
	desired, err := client.loadDesiredInput(
		options.Files,
		options.PreSQL, options.PreSQLFile,
		options.ConcurrentlyPreSQL, options.ConcurrentlyPreSQLFile,
	)
	if err != nil {
		return nil, err
	}

	conn, err := client.connect(ctx, !options.NoReadOnly)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx) //nolint:errcheck

	result, err := client.diffAll(ctx, conn, &diffAllOptions{
		FilterOptions:            options.FilterOptions,
		DropPolicy:               options.DropPolicy,
		Desired:                  desired,
		DisableIndexConcurrently: options.DisableIndexConcurrently,
		ForceIndexConcurrently:   options.ForceIndexConcurrently,
		BulkAlter:                options.BulkAlter,
		AssumeValidated:          options.AssumeValidated,
	})
	if err != nil {
		return nil, err
	}

	// A check SQL is evaluated under the same search_path apply gives it, so an
	// unqualified name in it resolves to the same object in both commands.
	// Without it a check reading a table in a non-public target schema failed
	// here and was reported as undetermined, while apply answered it. The
	// catalog has already been read, so this cannot change what the diff
	// compared, and only a run that has a check to evaluate pays for the
	// statement.
	if hasCheckSQL(result.ExecuteStmts) {
		if _, err := conn.Exec(ctx, client.searchPathSQL()); err != nil {
			return nil, fmt.Errorf("failed to set search_path: %w", err)
		}
	}

	// execute-first statements run before the schema changes, plain execute
	// statements after them. Order within each group follows the source file.
	// A statement whose check SQL is false is left out, so the plan shows what
	// apply would run rather than everything the file holds.
	//
	// A check that cannot be evaluated is undetermined, not fatal. plan runs
	// before the managed DDL and on a read-only connection, so a check may
	// reference a table this run creates, or write. Both answer fine at apply
	// time. Such a statement is kept in the plan, as it was before checks were
	// evaluated at all, with the reason recorded; failing here would take the
	// unrelated DDL down with it.
	appendExecuteStmts := func(stmts []string, first bool) []string {
		for _, es := range result.ExecuteStmts {
			if es.First != first {
				continue
			}
			shouldExecute := true
			note := ""
			if es.CheckSQL != "" {
				if err := conn.QueryRow(ctx, es.CheckSQL).Scan(&shouldExecute); err != nil {
					shouldExecute = true
					note = "check SQL could not be evaluated at plan time: " + err.Error() + "; apply will decide"
				}
			}
			if shouldExecute {
				stmts = append(stmts, parser.FormatExecuteStmtWithNote(es, note))
			}
		}
		return stmts
	}

	var stmts []string
	stmts = appendExecuteStmts(stmts, true)
	stmts = append(stmts, result.Stmts...)
	stmts = appendExecuteStmts(stmts, false)

	hasChanges := len(stmts) > 0

	// Prefix order matches apply: PreSQL -> concurrently-pre-SQL -> DDL.
	// Skipped entirely when there is nothing to execute, so an empty plan
	// stays empty instead of leaking a bare SET / pre-SQL line.
	if hasChanges {
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
		Ignored:         strings.Join(result.Ignored, "\n"),
		Count:           result.Count,
		HasChanges:      hasChanges,
	}, nil
}

// hasCheckSQL reports whether any execute statement carries a condition to
// evaluate, so plan sets the search_path only when one is going to run.
func hasCheckSQL(stmts []*parser.ExecuteStmt) bool {
	for _, es := range stmts {
		if es.CheckSQL != "" {
			return true
		}
	}
	return false
}
