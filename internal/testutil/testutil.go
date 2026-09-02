package testutil

import (
	"context"
	"os"
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
