package testutil

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// ConnString returns the connection string the tests use. A test that needs its
// own connection builds it from here.
func ConnString() string {
	connString := os.Getenv("TEST_PISTA_CONN_STR")
	if connString == "" {
		// The port compose.yaml publishes PostgreSQL 15 on. `make test`
		// exports TEST_PISTA_CONN_STR built from PGPORT, so this default
		// only applies to a bare `go test`.
		connString = "postgres://postgres@localhost:5415/postgres"
	}
	return connString
}

func ConnectDB(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), ConnString())
	require.NoError(t, err)
	return conn
}

// DBPool hands databases of their own to subtests that run in parallel. The
// fixture suites reset `public` before each case, so two cases running at once
// on one database would drop each other's schema. The databases sit on the
// server ConnString names, are created the first time a pool asks for them,
// and are kept afterwards: SetupDB resets each one per case as it does the
// shared database.
type DBPool struct {
	names chan string
}

// NewDBPool returns a pool of GOMAXPROCS databases, named pista_test_1 and so
// on. go test runs that many parallel subtests at once by default, so a subtest
// rarely waits for a database.
func NewDBPool(t *testing.T) *DBPool {
	t.Helper()
	ctx := context.Background()
	conn := ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	size := runtime.GOMAXPROCS(0)
	p := &DBPool{names: make(chan string, size)}
	for i := 1; i <= size; i++ {
		name := fmt.Sprintf("pista_test_%d", i)
		var exists bool
		err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_database WHERE datname = $1)", name).Scan(&exists)
		require.NoError(t, err)
		if !exists {
			_, err = conn.Exec(ctx, "CREATE DATABASE "+name)
			require.NoError(t, err)
		}
		p.names <- name
	}
	return p
}

// Connect takes a database from the pool and connects to it. The connection is
// closed and the database goes back to the pool when t finishes.
func (p *DBPool) Connect(t *testing.T) *pgx.Conn {
	t.Helper()
	name := <-p.names
	conn, err := pgx.Connect(context.Background(), connStringFor(t, name))
	if err != nil {
		p.names <- name
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close(context.Background()) //nolint:errcheck
		p.names <- name
	})
	return conn
}

// connStringFor returns ConnString with its database replaced by db. It takes
// both forms libpq does: a URL, whose path is the database unless a dbname
// parameter overrides it, and keyword/value pairs, where a later dbname
// overrides an earlier one.
func connStringFor(t *testing.T, db string) string {
	t.Helper()
	s := ConnString()
	if strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://") {
		u, err := url.Parse(s)
		require.NoError(t, err)
		u.Path = "/" + db
		q := u.Query()
		q.Del("dbname")
		u.RawQuery = q.Encode()
		return u.String()
	}
	return s + " dbname=" + db
}

func ServerMajorVersion(t *testing.T, ctx context.Context, conn *pgx.Conn) int {
	t.Helper()
	var num int
	err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&num)
	require.NoError(t, err)
	return num / 10000
}

func SetupDB(t *testing.T, ctx context.Context, conn *pgx.Conn, initSQL string) {
	t.Helper()
	_, err := conn.Exec(ctx, "DROP SCHEMA public CASCADE")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE SCHEMA public")
	require.NoError(t, err)
	if initSQL != "" {
		_, err = conn.Exec(ctx, initSQL)
		require.NoError(t, err)
	}
}
