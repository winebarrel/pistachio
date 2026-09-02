package parser

import "io"

// ParseSQLWithSchema and ReadSQLFile are exposed only to tests; the production
// entry point is ParseSQLFilesWithSchema, which calls the unexported
// parseSQLWithSchema and readSQLFile internally.
var (
	ReadSQLFile        = readSQLFile
	IgnoredStmtSnippet = ignoredStmtSnippet
	AnnotateError      = annotateError
)

// ParseSQLWithSchema parses SQL that came from no file, which is how most
// parser tests call in. A warning then names no position.
func ParseSQLWithSchema(sql string, defaultSchema string) (*ParseResult, error) {
	return parseSQLWithSchema(sql, defaultSchema, nil)
}

type FileSpan = fileSpan

func NewFileSpan(path string, start int) fileSpan {
	return fileSpan{path: path, start: start}
}

func NewLocatedError(msg string, offset int) error {
	return &locatedError{msg: msg, offset: offset}
}

// SetWarnWriter swaps the destination for ignored-statement warnings and
// returns a function that restores the previous writer.
func SetWarnWriter(w io.Writer) func() {
	old := warnWriter
	warnWriter = w
	return func() { warnWriter = old }
}
