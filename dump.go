package pistachio

import (
	"context"
	"fmt"
	"strings"

	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/model"
)

type DumpOptions struct{}

func (client *Client) Dump(ctx context.Context, options *DumpOptions) (string, error) {
	conn, err := client.connect()
	if err != nil {
		return "", err
	}
	defer conn.Close(ctx) //nolint:errcheck

	catalog, err := catalog.NewCatalog(conn, client.Schemas)
	if err != nil {
		return "", fmt.Errorf("failed to create catalog: %w", err)
	}

	tables, err := catalog.Tables(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch tables: %w", err)
	}

	views, err := catalog.Views(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch views: %w", err)
	}

	var parts []string
	if tables.Len() > 0 {
		parts = append(parts, model.TablesToSQL(tables))
	}
	if views.Len() > 0 {
		parts = append(parts, model.ViewsToSQL(views))
	}

	return strings.Join(parts, "\n\n"), nil
}
