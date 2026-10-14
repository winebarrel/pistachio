package testutil_test

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestConnStringFor(t *testing.T) {
	tests := map[string]string{
		"url":               "postgres://postgres@localhost:5415/postgres?sslmode=disable",
		"url with dbname":   "postgres://postgres@localhost:5415/postgres?dbname=postgres&sslmode=disable",
		"keyword/value":     "host=localhost port=5415 user=postgres dbname=postgres",
		"postgresql:// url": "postgresql://postgres@localhost:5415/postgres",
	}
	for name, connString := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TEST_PISTA_CONN_STR", connString)
			cfg, err := pgx.ParseConfig(testutil.ConnStringFor(t, "pista_test_1"))
			require.NoError(t, err)
			assert.Equal(t, "pista_test_1", cfg.Database)
			assert.Equal(t, uint16(5415), cfg.Port)
		})
	}
}
