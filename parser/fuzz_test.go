package parser

import (
	"io"
	"testing"

	"github.com/winebarrel/pistachio/internal/testutil/fuzzseed"
	"github.com/winebarrel/pistachio/model"
)

// FuzzParseSQLWithSchema feeds arbitrary text to the parser and renders
// whatever comes back. Most inputs are rejected by pg_query and return an
// error, so the value is in the ones the mutator builds out of the seeds:
// those reach the AST walk and the SQL builders behind it, which is where an
// unexpected node shape turns into a panic rather than an error.
//
// Rendering is part of the target because `pista dump` writes the model back
// out, and a model the parser accepts but the builders cannot render is just
// as broken as a parse that crashes.
func FuzzParseSQLWithSchema(f *testing.F) {
	f.Cleanup(SetWarnWriter(io.Discard))

	seeds, err := fuzzseed.Schemas()
	if err != nil {
		f.Fatal(err)
	}
	for _, sql := range seeds {
		f.Add(sql)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		result, err := parseSQLWithSchema(sql, "public")
		if err != nil {
			return
		}

		model.TablesToSQL(result.Tables)
		model.EnumsToSQL(result.Enums)
		model.DomainsToSQL(result.Domains)
		model.CompositeTypesToSQL(result.CompositeTypes)
		model.SequencesToSQL(result.Sequences)
		model.RoutinesToSQL(result.Routines)
		model.ViewsToSQL(result.Views)
		for _, es := range result.ExecuteStmts {
			FormatExecuteStmt(es)
		}
	})
}
