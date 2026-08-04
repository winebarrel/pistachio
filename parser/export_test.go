package parser

import "io"

// ParseSQLWithSchema and ReadSQLFile are exposed only to tests; the production
// entry point is ParseSQLFilesWithSchema, which calls the unexported
// parseSQLWithSchema and readSQLFile internally.
var (
	ParseSQLWithSchema = parseSQLWithSchema
	ReadSQLFile        = readSQLFile
	IgnoredStmtSnippet = ignoredStmtSnippet
)

// SetWarnWriter swaps the destination for ignored-statement warnings and
// returns a function that restores the previous writer.
func SetWarnWriter(w io.Writer) func() {
	old := warnWriter
	warnWriter = w
	return func() { warnWriter = old }
}
