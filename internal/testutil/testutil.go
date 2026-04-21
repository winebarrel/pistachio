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
	connString := os.Getenv("PIST_CONN_STR")
	if connString == "" {
		connString = "postgres://postgres@localhost/postgres"
	}
	conn, err := pgx.Connect(context.Background(), connString)
	require.NoError(t, err)
	return conn
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
