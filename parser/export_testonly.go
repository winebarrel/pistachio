//go:build testonly

package parser

import "io"

// The names the external tests use for what is unexported. The testonly tag
// keeps them out of a build without it, which is what a release is.

var (
	ParseSQLWithSchema               = parseSQLWithSchema
	ReadSQLFile                      = readSQLFile
	IgnoredStmtSnippet               = ignoredStmtSnippet
	ParenthesizeDefault              = parenthesizeDefault
	FillNumericScale                 = fillNumericScale
	NormalizeTypeName                = normalizeTypeName
	SplitTypeSuffix                  = splitTypeSuffix
	BlankBlockComments               = blankBlockComments
	ExtractBulkAlterDirectives       = extractBulkAlterDirectives
	ExtractColumnName                = extractColumnName
	ExtractConcurrentlyDirectives    = extractConcurrentlyDirectives
	ExtractConstraintName            = extractConstraintName
	ExtractExecuteDirectives         = extractExecuteDirectives
	ExtractInlineDirectives          = extractInlineDirectives
	ExtractStmtDirectives            = extractStmtDirectives
	FindLeadingCommentEnd            = findLeadingCommentEnd
	NormalizeUnqualifiedDirective    = normalizeUnqualifiedDirective
	QualifyRenameFrom                = qualifyRenameFrom
	ScanQuotedIdent                  = scanQuotedIdent
	ValidateDirectives               = validateDirectives
	CollectColumnRefsInConstraintDef = collectColumnRefsInConstraintDef
	CollectColumnRefsInIndexDef      = collectColumnRefsInIndexDef
	ConstraintKindLabel              = constraintKindLabel
	ValidateColumnRefs               = validateColumnRefs
)

// ParseSQLNoFile parses SQL that came from no file, which is how most parser
// tests call in. A warning then names no position.
func ParseSQLNoFile(sql string, defaultSchema string) (*ParseResult, error) {
	return parseSQLWithSchema(sql, defaultSchema, nil)
}

// ParseSQLWithPublicSchema parses SQL from no file with public as the default
// schema.
func ParseSQLWithPublicSchema(sql string) (*ParseResult, error) {
	return ParseSQLNoFile(sql, "public")
}

// SetWarnWriter swaps the destination for ignored-statement warnings and
// returns a function that restores the previous writer.
func SetWarnWriter(w io.Writer) func() {
	old := warnWriter
	warnWriter = w
	return func() { warnWriter = old }
}
