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
	"github.com/winebarrel/pistachio/model"
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

// searchPathSQL returns the SET that puts the target schemas, plus public, on
// the search path.
//
// public is appended so a reference to an object commonly kept there
// (extension types such as citext, functions) still resolves from a non-public
// target schema; this extends the default search_path rather than replacing it.
//
// apply issues it before the managed DDL, so an unqualified user-type reference
// in a column or attribute definition resolves. plan issues it before it
// evaluates a -- pista:execute check, so the check answers the same question in
// both commands. Neither affects where objects are created: the generated DDL
// always schema-qualifies its object names.
func (client *Client) searchPathSQL() string {
	quoted := make([]string, 0, len(client.Schemas)+1)
	hasPublic := false
	for _, s := range client.Schemas {
		quoted = append(quoted, model.Ident(s))
		if s == "public" {
			hasPublic = true
		}
	}
	if !hasPublic {
		quoted = append(quoted, "public")
	}
	return "SET search_path TO " + strings.Join(quoted, ", ")
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

	// Set search_path so the catalog output follows this setting rather than a
	// server-side one. See DefaultSearchPath for why the value matters. The
	// default is public rather than the target schemas, which keeps an object
	// outside public qualified in the dump and in the diff.
	//
	// apply issues its own SET later, so the DDL it runs still resolves an
	// unqualified reference against the target schemas. Pre-SQL runs before that
	// SET, and so under this setting.
	//
	// A startup parameter costs no extra round-trip. An invalid value fails the
	// connection with the server's own message.
	searchPath := DefaultSearchPath
	if client.SearchPath != nil {
		searchPath = *client.SearchPath
	}
	cfg.RuntimeParams["search_path"] = searchPath

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pistachio: failed to connect database: %w", err)
	}

	return conn, nil
}
