package diff

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/winebarrel/pistachio/internal/testutil/fuzzseed"
	"github.com/winebarrel/pistachio/parser"
)

// FuzzDiffSelf checks the property the whole tool rests on: a schema diffed
// against itself needs no DDL. Both sides come from the same input parsed
// twice, so a statement here means a comparison reported two identical objects
// as different, and a panic means the diff cannot walk a model the parser
// accepts.
//
// It then diffs the same schema against an empty one, the path `pista plan`
// takes against a fresh database, which reaches the create builders the
// identity check skips.
//
// A Diff* error is not a failure. Some inputs are wrong rather than
// unsupported (a -- pista:renamed-from naming an object that is not there,
// say), and reporting them is the point.
func FuzzDiffSelf(f *testing.F) {
	seeds, err := fuzzseed.Schemas()
	if err != nil {
		f.Fatal(err)
	}
	for _, sql := range seeds {
		f.Add(sql)
	}

	dir := f.TempDir()
	path := filepath.Join(dir, "schema.sql")
	emptyPath := filepath.Join(dir, "empty.sql")
	if err := os.WriteFile(emptyPath, nil, 0o600); err != nil {
		f.Fatal(err)
	}
	empty, err := parser.ParseSQLFilesWithSchema([]string{emptyPath}, "public")
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		if err := os.WriteFile(path, []byte(sql), 0o600); err != nil {
			t.Fatal(err)
		}

		// Parsed twice rather than shared: the diff functions may rewrite the
		// models they are handed, and one pointer on both sides would hide it.
		current, err := parser.ParseSQLFilesWithSchema([]string{path}, "public")
		if err != nil {
			return
		}
		desired, err := parser.ParseSQLFilesWithSchema([]string{path}, "public")
		if err != nil {
			t.Fatalf("the same input parsed twice gave different results: %v", err)
		}

		stmts, err := diffEverything(current, desired)
		if err != nil {
			return
		}
		if len(stmts) > 0 {
			t.Fatalf("a schema diffed against itself wants %d statement(s): %v", len(stmts), stmts)
		}

		diffEverything(empty, desired)
	})
}

// diffEverything runs every Diff* entry point over the two parse results and
// returns the statements they produced. A nil DropChecker denies every drop,
// so nothing here depends on a drop policy.
func diffEverything(current, desired *parser.ParseResult) ([]string, error) {
	enums, err := DiffEnums(current.Enums, desired.Enums, nil)
	if err != nil {
		return nil, err
	}
	sequences, err := DiffSequences(current.Sequences, desired.Sequences, nil)
	if err != nil {
		return nil, err
	}
	domains, err := DiffDomains(current.Domains, desired.Domains, nil)
	if err != nil {
		return nil, err
	}
	compositeTypes, err := DiffCompositeTypes(current.CompositeTypes, desired.CompositeTypes, nil)
	if err != nil {
		return nil, err
	}
	tables, err := DiffTables(current.Tables, desired.Tables, nil)
	if err != nil {
		return nil, err
	}
	views, err := DiffViews(current.Views, desired.Views, nil)
	if err != nil {
		return nil, err
	}
	routines, err := DiffRoutines(current.Routines, desired.Routines, nil)
	if err != nil {
		return nil, err
	}

	var stmts []string
	stmts = append(stmts, enums.Stmts...)
	stmts = append(stmts, sequences.Stmts...)
	stmts = append(stmts, domains.Stmts...)
	stmts = append(stmts, compositeTypes.Stmts...)
	stmts = append(stmts, tables.Stmts...)
	stmts = append(stmts, views.DropStmts...)
	stmts = append(stmts, views.CreateStmts...)
	stmts = append(stmts, routines.Stmts...)

	return stmts, nil
}
