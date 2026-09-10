package parser

import "io"

// parseSQLNoFile parses SQL that came from no file, which is how most parser
// tests call in. A warning then names no position.
func parseSQLNoFile(sql string, defaultSchema string) (*ParseResult, error) {
	return parseSQLWithSchema(sql, defaultSchema, nil)
}

func parseSQLWithPublicSchema(sql string) (*ParseResult, error) {
	return parseSQLNoFile(sql, "public")
}

// setWarnWriter swaps the destination for ignored-statement warnings and
// returns a function that restores the previous writer.
func setWarnWriter(w io.Writer) func() {
	old := warnWriter
	warnWriter = w
	return func() { warnWriter = old }
}
