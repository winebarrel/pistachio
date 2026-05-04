package pistachio

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect_PropagatesCancelledContext(t *testing.T) {
	connStr := os.Getenv("TEST_PIST_CONN_STR")
	if connStr == "" {
		connStr = "postgres://postgres@localhost/postgres"
	}
	client := NewClient(&Options{ConnString: connStr})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.connect(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}
