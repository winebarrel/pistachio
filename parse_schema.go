package pistachio

import (
	"github.com/winebarrel/pistachio/parser"
)

// ParseSchema parses the desired schema files without connecting to a
// database. Unqualified names are qualified with the first schema in
// Options.Schemas, the same way Plan and Apply read their input.
func (client *Client) ParseSchema(files []string) (*parser.ParseResult, error) {
	if err := client.validateSchemas(); err != nil {
		return nil, err
	}

	// Not wrapped: the parser's message already names the file.
	return parser.ParseSQLFilesWithSchema(files, client.Schemas[0])
}
