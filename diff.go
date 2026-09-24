package pistachio

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/internal/gitfile"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

type DiffOptions struct {
	FilterOptions
	DropPolicy
	Files                    []string `arg:"" help:"Paths to the schema SQL files. Without --git, exactly two: the current schema and the desired schema."`
	Git                      string   `short:"g" env:"PISTA_GIT" placeholder:"RANGE" help:"Read the files from git. A..B compares the two revisions, A...B compares B against its merge base with A, and A alone compares A against the working tree."`
	DisableIndexConcurrently bool     `xor:"index-concurrently" env:"PISTA_DISABLE_INDEX_CONCURRENTLY" help:"Ignore CONCURRENTLY opt-ins (directive and inline) and emit plain CREATE/DROP INDEX."`
	ForceIndexConcurrently   bool     `xor:"index-concurrently" env:"PISTA_FORCE_INDEX_CONCURRENTLY" help:"Force CONCURRENTLY on every CREATE/DROP INDEX, including pure drops."`
	BulkAlter                bool     `env:"PISTA_BULK_ALTER" help:"Combine consecutive ALTER TABLE actions on the same table into a single statement. FK changes, RENAME, VALIDATE CONSTRAINT, RLS toggles, and skipped DROPs stay separate."`
	AssumeValidated          bool     `env:"PISTA_ASSUME_VALIDATED" help:"Treat every table constraint, domain constraint, and foreign key as validated: ignore NOT VALID and never emit VALIDATE CONSTRAINT."`
	Explain                  bool     `env:"PISTA_EXPLAIN" help:"Comment each statement that scans or rewrites a table with what it does and what its lock blocks. No database is read, so no size is shown, and a type change or a default that calls a function reads as may rewrite."`
}

// Diff diffs two schemas without a database: the first stands in for the
// current state the catalog gives Plan, the second is the desired state.
// Without --git the two are the files named on the command line; with it both
// come out of a git repository. The output is the DDL that takes the first
// schema to the second, and nothing else: -- pista:execute statements are
// left out.
func (client *Client) Diff(options *DiffOptions) (*PlanResult, error) {
	if err := client.validateSchemas(); err != nil {
		return nil, err
	}

	current, desired, err := client.diffInput(options)
	if err != nil {
		return nil, err
	}

	// The current schema plays the catalog's role, and the catalog reads only
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

	result, err := client.diffObjects(&schemaObjects{
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

	if options.Explain {
		result.Stmts = client.explainStmtsOffline(result.Stmts, result.CurrentTables, result.DesiredTables, result.DesiredDomains)
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

// diffInput parses the two sides of the diff. Without --git they are the two
// files on the command line, one per side. With it every file is read at both
// ends of the range, and the schema of a side is all of its files together,
// the way plan reads the files of a desired schema.
func (client *Client) diffInput(options *DiffOptions) (*parser.ParseResult, *parser.ParseResult, error) {
	if options.Git == "" {
		if len(options.Files) != 2 {
			return nil, nil, fmt.Errorf("diff takes a current and a desired schema SQL file, got %d file(s); --git reads them from git instead", len(options.Files))
		}

		// Not wrapped: the parser's message already names the file.
		current, err := parser.ParseSQLFilesWithSchema(options.Files[:1], client.Schemas[0])
		if err != nil {
			return nil, nil, err
		}
		desired, err := parser.ParseSQLFilesWithSchema(options.Files[1:], client.Schemas[0])
		if err != nil {
			return nil, nil, err
		}

		return current, desired, nil
	}

	revs, err := gitfile.ParseRange(options.Git)
	if err != nil {
		return nil, nil, err
	}

	currentSources, desiredSources, err := gitSources(revs, options.Files)
	if err != nil {
		return nil, nil, err
	}

	current, err := parser.ParseSQLSourcesWithSchema(currentSources, client.Schemas[0])
	if err != nil {
		return nil, nil, err
	}
	desired, err := parser.ParseSQLSourcesWithSchema(desiredSources, client.Schemas[0])
	if err != nil {
		return nil, nil, err
	}

	return current, desired, nil
}

// gitSources reads every path at both ends of the range. A path one side does
// not hold is an empty file there, which is what a file added or removed
// between the two revisions is. A path neither side holds is an error: a
// typed path that answers to nothing would otherwise read as an empty schema
// and plan a drop of everything.
func gitSources(revs *gitfile.Range, paths []string) ([]parser.Source, []parser.Source, error) {
	currentSources := make([]parser.Source, 0, len(paths))
	desiredSources := make([]parser.Source, 0, len(paths))

	for _, path := range paths {
		currentSQL, inCurrent, err := gitfile.Read(revs.Current, path)
		if err != nil {
			return nil, nil, err
		}

		var desiredSQL string
		var inDesired bool
		if revs.Desired == "" {
			desiredSQL, inDesired, err = readWorkingTree(path)
		} else {
			desiredSQL, inDesired, err = gitfile.Read(revs.Desired, path)
		}
		if err != nil {
			return nil, nil, err
		}

		if !inCurrent && !inDesired {
			return nil, nil, fmt.Errorf("%s is in neither %s nor %s", path, revs.Current, desiredSide(revs))
		}

		currentSources = append(currentSources, parser.Source{Name: sourceName(revs.Current, path), SQL: currentSQL})
		desiredSources = append(desiredSources, parser.Source{Name: sourceName(revs.Desired, path), SQL: desiredSQL})
	}

	return currentSources, desiredSources, nil
}

// readWorkingTree reads the desired side of a range that names one revision.
// A file that is not there is not an error: the revision holds it and the
// working tree does not, which is a drop.
func readWorkingTree(path string) (string, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("failed to read SQL file: %w", err)
	}

	return string(data), true, nil
}

// sourceName is what a parse error calls the file. The working tree side
// carries the path alone, the way a file on the command line does.
func sourceName(rev, path string) string {
	if rev == "" {
		return path
	}

	return rev + ":" + path
}

func desiredSide(revs *gitfile.Range) string {
	if revs.Desired == "" {
		return "the working tree"
	}

	return revs.Desired
}
