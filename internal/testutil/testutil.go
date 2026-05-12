package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func ConnectDB(t *testing.T) *pgx.Conn {
	t.Helper()
	connString := os.Getenv("TEST_PISTA_CONN_STR")
	if connString == "" {
		connString = "postgres://postgres@localhost/postgres"
	}
	conn, err := pgx.Connect(context.Background(), connString)
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
