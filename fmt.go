package pistachio

import (
	"fmt"
	"strings"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

type FmtOptions struct {
	Files []string `arg:"" help:"Path to the SQL file(s) to format."`
	Write bool     `short:"w" help:"Write result to source file(s) instead of stdout."`
	Check bool     `help:"Check if files are formatted. Exit with non-zero status if any file needs formatting."`
}

// Format formats SQL files and returns the results. Each file is formatted
// independently and returned as a map from file path to formatted SQL.
func (client *Client) Format(options *FmtOptions) (map[string]string, error) {
	if len(client.Schemas) == 0 {
		return nil, fmt.Errorf("no schemas configured")
	}
	defaultSchema := client.Schemas[0]
	results := make(map[string]string, len(options.Files))

	for _, path := range options.Files {
		result, err := parser.ParseSQLFileWithSchema(path, defaultSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SQL file %q: %w", path, err)
		}
		results[path] = FormatSchemaSQL(result.Enums, result.Tables, result.Views)
	}

	return results, nil
}

// FormatSchemaSQL formats enums, tables, and views into canonical SQL output.
// This is the shared formatting logic used by both dump and fmt.
func FormatSchemaSQL(
	enums *orderedmap.Map[string, *model.Enum],
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
) string {
	var parts []string
	if enums.Len() > 0 {
		parts = append(parts, model.EnumsToSQL(enums))
	}
	if tables.Len() > 0 {
		parts = append(parts, model.TablesToSQL(tables))
	}
	if views.Len() > 0 {
		parts = append(parts, model.ViewsToSQL(views))
	}
	return strings.Join(parts, "\n\n")
}
