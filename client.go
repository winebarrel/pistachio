package pistachio

import (
	"context"
	"errors"
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

// validateSchemas guards every public entry point that needs to address at
// least one schema. The CLI sets Schemas to ["public"] by default via kong,
// but library callers can construct Options directly and forget it.
// catalog.NewCatalog already errors on empty Schemas, but Schemas[0] is
// also indexed in diff_all.go before that catalog call would short-circuit
// in some refactor paths — so guard explicitly with a clear message.
//
// Empty/whitespace entries are also rejected: model.Ident drops empty
// components silently, which would otherwise produce malformed DDL or
// route changes through search_path into an unintended schema.
func (client *Client) validateSchemas() error {
	if len(client.Schemas) == 0 {
		return errors.New("pistachio: at least one schema must be specified in Options.Schemas")
	}
	for _, s := range client.Schemas {
		if strings.TrimSpace(s) == "" {
			return errors.New("pistachio: Options.Schemas must not contain empty or whitespace-only entries")
		}
	}
	return nil
}

func (client *Client) buildConnConfig() (*pgx.ConnConfig, error) {
	cfg, err := pgx.ParseConfig(client.ConnString)
	if err != nil {
		return nil, fmt.Errorf("pistachio: failed to parse connection string: %w", err)
	}

	if client.Dbname != "" {
		cfg.Database = client.Dbname
	}

	if client.Password != "" {
		cfg.Password = client.Password
	}

	return cfg, nil
}

func (client *Client) connect(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := client.buildConnConfig()
	if err != nil {
		return nil, err
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pistachio: failed to connect database: %w", err)
	}

	return conn, err
}
