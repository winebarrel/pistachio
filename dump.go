package pistachio

import (
	"context"
	"fmt"
	"strings"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/model"
)

type DumpOptions struct {
	Split      string `help:"Output each table/view as a separate file in the specified directory."`
	OmitSchema bool   `help:"Omit schema name from the dump output."`
}

type DumpResult struct {
	Tables     *orderedmap.Map[string, *model.Table]
	Views      *orderedmap.Map[string, *model.View]
	OmitSchema bool
}

func (r *DumpResult) tables() *orderedmap.Map[string, *model.Table] {
	if !r.OmitSchema {
		return r.Tables
	}
	tables := orderedmap.New[string, *model.Table]()
	for _, t := range r.Tables.CollectValues() {
		copied := *t
		fqtn := t.FQTN()
		tableName := model.Ident(t.Name)
		copied.Schema = ""
		if copied.ForeignKeys.Len() > 0 {
			fks := orderedmap.New[string, *model.ForeignKey]()
			for _, fk := range copied.ForeignKeys.CollectValues() {
				fkCopied := *fk
				fkCopied.Schema = ""
				fks.Set(fk.Name, &fkCopied)
			}
			copied.ForeignKeys = fks
		}
		if copied.Indexes.Len() > 0 {
			idxs := orderedmap.New[string, *model.Index]()
			for _, idx := range copied.Indexes.CollectValues() {
				idxCopied := *idx
				idxCopied.Schema = ""
				idxCopied.Definition = strings.ReplaceAll(idx.Definition, " ON "+fqtn+" ", " ON "+tableName+" ")
				idxs.Set(idx.Name, &idxCopied)
			}
			copied.Indexes = idxs
		}
		tables.Set(fqtn, &copied)
	}
	return tables
}

func (r *DumpResult) views() *orderedmap.Map[string, *model.View] {
	if !r.OmitSchema {
		return r.Views
	}
	views := orderedmap.New[string, *model.View]()
	for _, v := range r.Views.CollectValues() {
		copied := *v
		copied.Schema = ""
		views.Set(v.FQVN(), &copied)
	}
	return views
}

func (r *DumpResult) String() string {
	var parts []string
	tables := r.tables()
	views := r.views()
	if tables.Len() > 0 {
		parts = append(parts, model.TablesToSQL(tables))
	}
	if views.Len() > 0 {
		parts = append(parts, model.ViewsToSQL(views))
	}
	return strings.Join(parts, "\n\n")
}

func (r *DumpResult) Files() map[string]string {
	files := make(map[string]string)
	seen := make(map[string]bool)
	for _, t := range r.tables().CollectValues() {
		name := uniqueFileName(seen, toFileName(t.Schema, t.Name))
		files[name] = model.TableToSQL(t) + "\n"
		seen[strings.ToLower(name)] = true
	}
	for _, v := range r.views().CollectValues() {
		name := uniqueFileName(seen, toFileName(v.Schema, v.Name))
		files[name] = model.ViewToSQL(v) + "\n"
		seen[strings.ToLower(name)] = true
	}
	return files
}

func uniqueFileName(seen map[string]bool, name string) string {
	if !seen[strings.ToLower(name)] {
		return name
	}
	ext := ".sql"
	base := strings.TrimSuffix(name, ext)
	for i := 2; ; i++ {
		candidate := fmt.Sprintf("%s_%d%s", base, i, ext)
		if !seen[strings.ToLower(candidate)] {
			return candidate
		}
	}
}

var fileNameReplacer = strings.NewReplacer(
	`"`, "",
	" ", "_",
)

func toFileName(schema, name string) string {
	base := name
	if schema != "" {
		base = schema + "." + name
	}
	return fileNameReplacer.Replace(base) + ".sql"
}

func (client *Client) Dump(ctx context.Context, options *DumpOptions) (*DumpResult, error) {
	if options.OmitSchema && len(client.Schemas) > 1 {
		return nil, fmt.Errorf("--omit-schema cannot be used with multiple schemas")
	}

	conn, err := client.connect()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx) //nolint:errcheck

	catalog, err := catalog.NewCatalog(conn, client.Schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	tables, err := catalog.Tables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tables: %w", err)
	}

	views, err := catalog.Views(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch views: %w", err)
	}

	return &DumpResult{
		Tables:     client.filterTables(client.remapTableSchemas(tables)),
		Views:      client.filterViews(client.remapViewSchemas(views)),
		OmitSchema: options.OmitSchema,
	}, nil
}
