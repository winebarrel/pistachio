package pistachio

import (
	"fmt"
	"strings"

	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

type FmtOptions struct {
	Files []string `arg:"" help:"Path to the SQL file(s) to format."`
	Write bool     `short:"w" help:"Write result to source file(s) instead of stdout."`
}

// Format formats SQL files and returns the results. When multiple files are
// provided, each file is formatted independently and returned as a map from
// file path to formatted SQL.
func (client *Client) Format(options *FmtOptions) (map[string]string, error) {
	defaultSchema := client.Schemas[0]
	results := make(map[string]string, len(options.Files))

	for _, path := range options.Files {
		result, err := parser.ParseSQLFileWithSchema(path, defaultSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SQL file: %w", err)
		}
		results[path] = formatParseResult(result)
	}

	return results, nil
}

func formatParseResult(result *parser.ParseResult) string {
	var parts []string
	if result.Enums.Len() > 0 {
		parts = append(parts, model.EnumsToSQL(result.Enums))
	}
	if result.Tables.Len() > 0 {
		parts = append(parts, model.TablesToSQL(result.Tables))
	}
	if result.Views.Len() > 0 {
		parts = append(parts, model.ViewsToSQL(result.Views))
	}

	return strings.Join(parts, "\n\n")
}
