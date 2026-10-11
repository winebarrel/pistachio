package pistachio_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/testutil"
	"gopkg.in/yaml.v3"
)

// loadYAML rejects a key the struct has no field for, so a misspelled
// assertion key fails the case instead of being dropped silently.
func loadYAML[T any](t *testing.T, path string) *T {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var v T
	require.NoError(t, dec.Decode(&v))
	return &v
}

func setupSchemaDB(t *testing.T, ctx context.Context, schema string, initSQL string) string {
	t.Helper()
	conn := testutil.ConnectDB(t)

	_, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)

	if initSQL != "" {
		_, err = conn.Exec(ctx, initSQL)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		conn.Close(ctx)
	})

	return conn.Config().ConnString()
}

func execSQL(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(ctx, sql)
	require.NoError(t, err)
}
