package parser_test

import "github.com/winebarrel/pistachio/parser"

func parseSQLWithPublicSchema(sql string) (*parser.ParseResult, error) {
	return parser.ParseSQLWithSchema(sql, "public")
}
