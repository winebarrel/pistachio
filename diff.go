package pistachio

import (
	"strings"

	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

type DiffOptions struct {
	FilterOptions
	DropPolicy
	Current                  string `arg:"" help:"Path to the current schema SQL file."`
	Desired                  string `arg:"" help:"Path to the desired schema SQL file."`
	DisableIndexConcurrently bool   `xor:"index-concurrently" env:"PISTA_DISABLE_INDEX_CONCURRENTLY" help:"Ignore CONCURRENTLY opt-ins (directive and inline) and emit plain CREATE/DROP INDEX."`
	ForceIndexConcurrently   bool   `xor:"index-concurrently" env:"PISTA_FORCE_INDEX_CONCURRENTLY" help:"Force CONCURRENTLY on every CREATE/DROP INDEX, including pure drops."`
	BulkAlter                bool   `env:"PISTA_BULK_ALTER" help:"Combine consecutive ALTER TABLE actions on the same table into a single statement. FK changes, RENAME, VALIDATE CONSTRAINT, RLS toggles, and skipped DROPs stay separate."`
	AssumeValidated          bool   `env:"PISTA_ASSUME_VALIDATED" help:"Treat every table constraint, domain constraint, and foreign key as validated: ignore NOT VALID and never emit VALIDATE CONSTRAINT."`
}

// Diff diffs two schema SQL files without a database: the first file stands in
// for the current state the catalog gives Plan, the second is the desired
// state. The output is the DDL that takes the first schema to the second,
// and nothing else: -- pista:execute statements are left out.
func (client *Client) Diff(options *DiffOptions) (*PlanResult, error) {
	if err := client.validateSchemas(); err != nil {
		return nil, err
	}

	// Not wrapped: the parser's message already names the file.
	current, err := parser.ParseSQLFilesWithSchema([]string{options.Current}, client.Schemas[0])
	if err != nil {
		return nil, err
	}
	desired, err := parser.ParseSQLFilesWithSchema([]string{options.Desired}, client.Schemas[0])
	if err != nil {
		return nil, err
	}

	// The current file plays the catalog's role, and the catalog reads only
	// the target schemas, so an object outside them is out of scope rather
	// than a drop. No schema map applies: its names are already current-side.
	filterDesiredBySchemas(current, client.Schemas, nil)

	// Routines mirror the catalog side: read only when --manage-routine asked
	// for it. Owned sequences are excluded the way the desired side excludes
	// them, so a sequence tied to a column stays unmanaged on both sides.
	currentRoutines := orderedmap.New[string, *model.Routine]()
	if options.ManageRoutine {
		currentRoutines = current.Routines
	}

	result, err := client.diffObjects(&currentObjects{
		Tables:         current.Tables,
		Views:          current.Views,
		Enums:          current.Enums,
		Domains:        current.Domains,
		CompositeTypes: current.CompositeTypes,
		Sequences:      standaloneSequences(current.Sequences),
		Routines:       currentRoutines,
	}, &diffAllOptions{
		FilterOptions:            options.FilterOptions,
		DropPolicy:               options.DropPolicy,
		Desired:                  &desiredInput{schema: desired},
		DisableIndexConcurrently: options.DisableIndexConcurrently,
		ForceIndexConcurrently:   options.ForceIndexConcurrently,
		BulkAlter:                options.BulkAlter,
		AssumeValidated:          options.AssumeValidated,
	})
	if err != nil {
		return nil, err
	}

	// A -- pista:execute statement is not part of the diff: it is not schema
	// state, and its check SQL cannot be evaluated without a database. apply
	// runs execute statements as usual.
	return &PlanResult{
		SQL:             strings.Join(result.Stmts, "\n"),
		DisallowedDrops: strings.Join(result.DisallowedDrops, "\n"),
		Ignored:         strings.Join(result.Ignored, "\n"),
		Count:           result.Count,
		HasChanges:      len(result.Stmts) > 0,
	}, nil
}
