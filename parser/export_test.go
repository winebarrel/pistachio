package parser

// ParseSQLWithSchema is exposed only to tests; the production entry point
// is ParseSQLFilesWithSchema, which calls the unexported parseSQLWithSchema
// internally.
var ParseSQLWithSchema = parseSQLWithSchema
