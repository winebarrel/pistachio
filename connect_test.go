package pistachio

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect_PropagatesCancelledContext(t *testing.T) {
	connStr := os.Getenv("TEST_PISTA_CONN_STR")
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

func TestBuildConnConfig_DbnameOverridesConnString(t *testing.T) {
	client := NewClient(&Options{
		ConnString: "postgres://postgres@localhost/postgres",
		Dbname:     "mydb",
	})

	cfg, err := client.buildConnConfig()
	require.NoError(t, err)
	assert.Equal(t, "mydb", cfg.Database)
}

func TestBuildConnConfig_DbnameWorksWithEmptyDbnameInConnString(t *testing.T) {
	client := NewClient(&Options{
		ConnString: "postgres://postgres@localhost:5432/",
		Dbname:     "mydb",
	})

	cfg, err := client.buildConnConfig()
	require.NoError(t, err)
	assert.Equal(t, "mydb", cfg.Database)
}

func TestBuildConnConfig_DbnameEmptyKeepsConnStringDatabase(t *testing.T) {
	client := NewClient(&Options{
		ConnString: "postgres://postgres@localhost/origdb",
	})

	cfg, err := client.buildConnConfig()
	require.NoError(t, err)
	assert.Equal(t, "origdb", cfg.Database)
}

func TestBuildConnConfig_PasswordOverride(t *testing.T) {
	client := NewClient(&Options{
		ConnString: "postgres://postgres@localhost/postgres",
		Password:   "secret",
	})

	cfg, err := client.buildConnConfig()
	require.NoError(t, err)
	assert.Equal(t, "secret", cfg.Password)
}

func TestBuildConnConfig_InvalidConnStringReturnsError(t *testing.T) {
	client := NewClient(&Options{ConnString: "::not-a-valid-conn-string::"})

	_, err := client.buildConnConfig()
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "failed to parse connection string"))
}

func TestConnect_InvalidConnStringPropagatesBuildError(t *testing.T) {
	client := NewClient(&Options{ConnString: "::not-a-valid-conn-string::"})

	_, err := client.connect(context.Background())
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "failed to parse connection string"))
}
