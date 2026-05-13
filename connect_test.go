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
		DBName:     "mydb",
	})

	cfg, err := client.buildConnConfig()
	require.NoError(t, err)
	assert.Equal(t, "mydb", cfg.Database)
}

func TestBuildConnConfig_DbnameWorksWithEmptyDbnameInConnString(t *testing.T) {
	client := NewClient(&Options{
		ConnString: "postgres://postgres@localhost:5432/",
		DBName:     "mydb",
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

func TestConnInfoComment(t *testing.T) {
	client := NewClient(&Options{
		ConnString: "postgres://myuser:secret@myhost:5433/mydb",
	})

	comment, err := client.ConnInfoComment()
	require.NoError(t, err)
	assert.Equal(t, "-- Connected to postgres://myuser@myhost:5433/mydb", comment)
	assert.NotContains(t, comment, "secret")
}

func TestConnInfoComment_WithDBNameOverride(t *testing.T) {
	client := NewClient(&Options{
		ConnString: "postgres://myuser@myhost/origdb",
		DBName:     "overridden",
	})

	comment, err := client.ConnInfoComment()
	require.NoError(t, err)
	assert.Contains(t, comment, "/overridden")
}

func TestConnInfoComment_OptionsPasswordNotIncluded(t *testing.T) {
	client := NewClient(&Options{
		ConnString: "postgres://myuser@myhost:5432/mydb",
		Password:   "topsecret",
	})

	comment, err := client.ConnInfoComment()
	require.NoError(t, err)
	assert.NotContains(t, comment, "topsecret")
}

func TestConnInfoComment_InvalidConnString(t *testing.T) {
	client := NewClient(&Options{ConnString: "::not-valid::"})

	_, err := client.ConnInfoComment()
	require.Error(t, err)
}

func TestConnect_InvalidConnStringPropagatesBuildError(t *testing.T) {
	client := NewClient(&Options{ConnString: "::not-a-valid-conn-string::"})

	_, err := client.connect(context.Background())
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "failed to parse connection string"))
}
