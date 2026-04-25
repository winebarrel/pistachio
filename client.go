package pistachio

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type Client struct {
	*Options
}

func NewClient(options *Options) *Client {
	client := &Client{
		options,
	}
	return client
}

func (client *Client) connect() (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(client.ConnString)
	if err != nil {
		return nil, fmt.Errorf("pistachio: failed to parse connection string: %w", err)
	}

	if client.Password != "" {
		cfg.Password = client.Password
	}

	if len(client.Schemas) > 0 {
		if cfg.RuntimeParams == nil {
			cfg.RuntimeParams = make(map[string]string)
		}
		// Use remapped schema names for search_path so unqualified names
		// in execute statements and pg_get_viewdef resolve correctly.
		remapped := make([]string, len(client.Schemas))
		for i, s := range client.Schemas {
			remapped[i] = client.RemapSchema(s)
		}
		cfg.RuntimeParams["search_path"] = strings.Join(remapped, ", ")
	}

	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("pistachio: failed to connect database: %w", err)
	}

	return conn, err
}
