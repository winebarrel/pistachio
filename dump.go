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
	Split string `help:"Output each table/view as a separate file in the specified directory."`
}

type DumpResult struct {
	Tables *orderedmap.Map[string, *model.Table]
	Views  *orderedmap.Map[string, *model.View]
}

func (r *DumpResult) String() string {
	var parts []string
	if r.Tables.Len() > 0 {
		parts = append(parts, model.TablesToSQL(r.Tables))
	}
	if r.Views.Len() > 0 {
		parts = append(parts, model.ViewsToSQL(r.Views))
	}
	return strings.Join(parts, "\n\n")
}

func (r *DumpResult) Files() map[string]string {
	files := make(map[string]string)
	for _, t := range r.Tables.CollectValues() {
		files[toFileName(t.Schema, t.Name)] = model.TableToSQL(t) + "\n"
	}
	for _, v := range r.Views.CollectValues() {
		files[toFileName(v.Schema, v.Name)] = model.ViewToSQL(v) + "\n"
	}
	return files
}

var fileNameReplacer = strings.NewReplacer(`"`, "", " ", "_")

func toFileName(schema, name string) string {
	return fileNameReplacer.Replace(schema+"."+name) + ".sql"
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
		Tables: client.remapTableSchemas(tables),
		Views:  client.remapViewSchemas(views),
	}, nil
}
