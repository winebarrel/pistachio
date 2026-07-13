package pistachio

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
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
// in some refactor paths; so guard explicitly with a clear message.
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

	if client.DBName != "" {
		cfg.Database = client.DBName
	}

	if client.Password != "" {
		cfg.Password = client.Password
	}

	return cfg, nil
}

// ConnInfoComment returns a SQL comment describing the connection target
// (host/port/dbname/user) for inclusion at the top of plan/apply/dump output.
// The password is intentionally never included.
//
// TCP connections render as a libpq URI (postgres://user@host:port/dbname).
// IPv6 hosts are bracketed via net.JoinHostPort; user and dbname are
// URL-escaped via net/url so identifiers with URI-meaningful characters
// (including '/' in the dbname) round-trip safely; Path holds the decoded
// form and RawPath the encoded form, so url.URL.String() uses RawPath when
// the default encoding would differ. libpq unix-socket connections (host
// starts with "/") render as a keyword/value string ("host=/path dbname=db
// user=u") instead; percent-encoding the socket path into the URI host
// component would be unreadable in a comment.
func (client *Client) ConnInfoComment() (string, error) {
	cfg, err := client.buildConnConfig()
	if err != nil {
		return "", err
	}

	if strings.HasPrefix(cfg.Host, "/") {
		return fmt.Sprintf("-- Connected to host=%s dbname=%s user=%s", cfg.Host, cfg.Database, cfg.User), nil
	}

	u := url.URL{
		Scheme:  "postgres",
		User:    url.User(cfg.User),
		Host:    net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))),
		Path:    "/" + cfg.Database,
		RawPath: "/" + url.PathEscape(cfg.Database),
	}
	return "-- Connected to " + u.String(), nil
}

// connect opens a database connection. When readOnly is true, the session
// rejects writes, so read-only operations (plan, dump) cannot modify the
// database even by accident. apply passes false because it applies DDL.
// The read-only flag is set as a startup parameter, so it is in effect for the
// whole connection with no extra round-trip.
func (client *Client) connect(ctx context.Context, readOnly bool) (*pgx.Conn, error) {
	cfg, err := client.buildConnConfig()
	if err != nil {
		return nil, err
	}

	if readOnly {
		cfg.RuntimeParams["default_transaction_read_only"] = "on"
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pistachio: failed to connect database: %w", err)
	}

	return conn, nil
}
