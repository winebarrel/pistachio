package parser

// ParseSQLWithSchema and ReadSQLFile are exposed only to tests; the production
// entry point is ParseSQLFilesWithSchema, which calls the unexported
// parseSQLWithSchema and readSQLFile internally.
var (
	ParseSQLWithSchema = parseSQLWithSchema
	ReadSQLFile        = readSQLFile
)
