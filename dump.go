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
		tables.Set(t.FQTN(), &copied)
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
	for _, t := range r.tables().CollectValues() {
		files[toFileName(t.Schema, t.Name)] = model.TableToSQL(t) + "\n"
	}
	for _, v := range r.views().CollectValues() {
		files[toFileName(v.Schema, v.Name)] = model.ViewToSQL(v) + "\n"
	}
	return files
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
		Tables:     tables,
		Views:      views,
		OmitSchema: options.OmitSchema,
	}, nil
}
