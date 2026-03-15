package pistachio

import (
	"context"
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

func (client *Client) connect() (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(client.ConnString)
	if err != nil {
		return nil, fmt.Errorf("pistachio: failed to parse connect string: %w", err)
	}

	if client.Password != "" {
		cfg.Password = client.Password
	}

	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("pistachio: failed to connect database: %w", err)
	}

	return conn, err
}
