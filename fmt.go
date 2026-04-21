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

// Format formats the combined SQL from the provided files and returns the result as a string.
func (client *Client) Format(options *FmtOptions) (string, error) {
	return FmtSQL(options.Files, client.Schemas[0])
}

// FormatFile formats a single SQL file and returns the result as a string.
func (client *Client) FormatFile(path string) (string, error) {
	return FmtSQLFile(path, client.Schemas[0])
}

// FmtSQL formats the combined SQL from the provided files and returns the result as a string.
func FmtSQL(files []string, defaultSchema string) (string, error) {
	result, err := parser.ParseSQLFilesWithSchema(files, defaultSchema)
	if err != nil {
		return "", fmt.Errorf("failed to parse SQL file: %w", err)
	}

	return formatParseResult(result), nil
}

// FmtSQLFile formats a single SQL file and returns the result as a string.
func FmtSQLFile(path string, defaultSchema string) (string, error) {
	result, err := parser.ParseSQLFileWithSchema(path, defaultSchema)
	if err != nil {
		return "", fmt.Errorf("failed to parse SQL file: %w", err)
	}

	return formatParseResult(result), nil
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
