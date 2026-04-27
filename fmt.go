package pistachio

import (
	"fmt"
	"sort"
	"strings"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

func formatEntity(result *parser.ParseResult, ref parser.EntityRef) string {
	switch ref.Kind {
	case parser.EntityKindEnum:
		e, _ := result.Enums.GetOk(ref.FQN)
		return model.EnumToSQLBare(e)
	case parser.EntityKindDomain:
		d, _ := result.Domains.GetOk(ref.FQN)
		return model.DomainToSQLBare(d)
	case parser.EntityKindTable:
		t, _ := result.Tables.GetOk(ref.FQN)
		return model.TableToSQLBare(t)
	case parser.EntityKindView:
		v, _ := result.Views.GetOk(ref.FQN)
		return model.ViewToSQLBare(v)
	}
	panic(fmt.Sprintf("unknown entity kind: %v", ref.Kind))
}

// fmtItem represents a top-level statement (entity or pist:execute) for
// source-order emission with surrounding comments.
type fmtItem struct {
	location     int32
	ownershipEnd int32
	sql          string
}

// formatWithComments emits entities and pist:execute statements in source
// order. Each item gets:
//   - leading comments: those between the previous item's ownership end and
//     this item's start.
//   - the item's bare SQL.
//   - trailing comments: those between this item's stmt end and its ownership
//     end (e.g. a comment placed before a CREATE INDEX owned by an entity).
//
// Comments after the last item's ownership end are appended at the very end.
func formatWithComments(result *parser.ParseResult) string {
	items := make([]fmtItem, 0, len(result.Order)+len(result.ExecuteStmts))
	for _, ref := range result.Order {
		items = append(items, fmtItem{
			location:     ref.Location,
			ownershipEnd: ref.OwnershipEnd,
			sql:          formatEntity(result, ref),
		})
	}
	for _, es := range result.ExecuteStmts {
		items = append(items, fmtItem{
			location:     es.Location,
			ownershipEnd: es.StmtEnd,
			sql:          parser.FormatExecuteStmt(es),
		})
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].location < items[j].location })

	var parts []string
	cIdx := 0
	for _, it := range items {
		var leading []string
		for cIdx < len(result.Comments) && result.Comments[cIdx].End <= it.location {
			leading = append(leading, result.Comments[cIdx].Text)
			cIdx++
		}
		var trailing []string
		for cIdx < len(result.Comments) && result.Comments[cIdx].End <= it.ownershipEnd {
			trailing = append(trailing, result.Comments[cIdx].Text)
			cIdx++
		}
		block := it.sql
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
		if err := result.AttachComments(); err != nil {
			return nil, fmt.Errorf("failed to scan comments in %q: %w", path, err)
		}
		results[path] = formatWithComments(result)
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
