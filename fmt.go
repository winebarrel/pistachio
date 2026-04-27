package pistachio

import (
	"fmt"
	"strings"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

func formatEntity(result *parser.ParseResult, ref parser.EntityRef) string {
	switch ref.Kind {
	case parser.EntityKindEnum:
		if e, ok := result.Enums.GetOk(ref.FQN); ok {
			return model.EnumToSQLBare(e)
		}
	case parser.EntityKindDomain:
		if d, ok := result.Domains.GetOk(ref.FQN); ok {
			return model.DomainToSQLBare(d)
		}
	case parser.EntityKindTable:
		if t, ok := result.Tables.GetOk(ref.FQN); ok {
			return model.TableToSQLBare(t)
		}
	case parser.EntityKindView:
		if v, ok := result.Views.GetOk(ref.FQN); ok {
			return model.ViewToSQLBare(v)
		}
	}
	return ""
}

// formatWithComments emits entities in source order. Each entity gets:
//   - leading comments: those between the previous entity's ownership end and
//     this entity's CREATE keyword.
//   - the entity's bare SQL (CREATE + indexes + FKs + COMMENT ON statements).
//   - trailing comments: those between this entity's CREATE statement end and
//     its ownership end (e.g. a comment placed before a CREATE INDEX that
//     belongs to this entity).
//
// Comments after the last entity's ownership end are appended at the very end.
func formatWithComments(result *parser.ParseResult) string {
	var parts []string
	cIdx := 0
	for _, ref := range result.Order {
		var leading []string
		for cIdx < len(result.Comments) && result.Comments[cIdx].End <= ref.Location {
			leading = append(leading, result.Comments[cIdx].Text)
			cIdx++
		}
		entity := formatEntity(result, ref)
		if entity == "" {
			continue
		}
		var trailing []string
		for cIdx < len(result.Comments) && result.Comments[cIdx].End <= ref.OwnershipEnd {
			trailing = append(trailing, result.Comments[cIdx].Text)
			cIdx++
		}
		block := entity
		if len(leading) > 0 {
			block = strings.Join(leading, "\n") + "\n" + block
		}
		if len(trailing) > 0 {
			block = block + "\n" + strings.Join(trailing, "\n")
		}
		parts = append(parts, block)
	}
	if cIdx < len(result.Comments) {
		var tail []string
		for ; cIdx < len(result.Comments); cIdx++ {
			tail = append(tail, result.Comments[cIdx].Text)
		}
		parts = append(parts, strings.Join(tail, "\n"))
	}
	return strings.Join(parts, "\n\n")
}

type FmtOptions struct {
	Files []string `arg:"" help:"Path to the SQL file(s) to format."`
	Write bool     `short:"w" xor:"mode" help:"Write result to source file(s) instead of stdout."`
	Check bool     `xor:"mode" help:"Check if files are formatted. Exit with non-zero status if any file needs formatting."`
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
		formatted := formatWithComments(result)

		// Append execute statements
		if len(result.ExecuteStmts) > 0 {
			var executeParts []string
			for _, es := range result.ExecuteStmts {
				executeParts = append(executeParts, parser.FormatExecuteStmt(es))
			}
			if formatted != "" {
				formatted += "\n\n"
			}
			formatted += strings.Join(executeParts, "\n\n")
		}

		results[path] = formatted
	}

	return results, nil
}

// formatSchemaSQL formats domains, enums, tables, and views into canonical SQL
// output for dump. fmt uses formatWithComments instead, which preserves source
// order and SQL comments.
// Order: enums → domains → tables → views (enums first since domains may depend on them).
func formatSchemaSQL(
	domains *orderedmap.Map[string, *model.Domain],
	enums *orderedmap.Map[string, *model.Enum],
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
) string {
	var parts []string
	if enums != nil && enums.Len() > 0 {
		parts = append(parts, model.EnumsToSQL(enums))
	}
	if domains != nil && domains.Len() > 0 {
		parts = append(parts, model.DomainsToSQL(domains))
	}
	if tables != nil && tables.Len() > 0 {
		parts = append(parts, model.TablesToSQL(tables))
	}
	if views != nil && views.Len() > 0 {
		parts = append(parts, model.ViewsToSQL(views))
	}
	return strings.Join(parts, "\n\n")
}
