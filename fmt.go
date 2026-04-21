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

func FmtSQL(options *FmtOptions, defaultSchema string) (string, error) {
	result, err := parser.ParseSQLFilesWithSchema(options.Files, defaultSchema)
	if err != nil {
		return "", fmt.Errorf("failed to parse SQL file: %w", err)
	}

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

	return strings.Join(parts, "\n\n"), nil
}
