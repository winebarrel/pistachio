package pistachio

import (
	"context"
	"errors"
	"fmt"

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
func (client *Client) validateSchemas() error {
	if len(client.Schemas) == 0 {
		return errors.New("pistachio: at least one schema must be specified in Options.Schemas")
	}
	return nil
}

func (client *Client) connect(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(client.ConnString)
	if err != nil {
		return nil, fmt.Errorf("pistachio: failed to parse connection string: %w", err)
	}

	if client.Password != "" {
		cfg.Password = client.Password
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pistachio: failed to connect database: %w", err)
	}

	return conn, err
}
