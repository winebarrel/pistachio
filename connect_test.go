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

func TestConnInfoComment_IPv6HostBracketed(t *testing.T) {
	// libpq URIs require IPv6 hosts to be bracketed; net.JoinHostPort handles
	// this. Without brackets the "::1:5432" string is ambiguous.
	client := NewClient(&Options{
		ConnString: "postgres://myuser@[::1]:5433/mydb",
	})

	comment, err := client.ConnInfoComment()
	require.NoError(t, err)
	assert.Equal(t, "-- Connected to postgres://myuser@[::1]:5433/mydb", comment)
}

func TestConnInfoComment_UnixSocketHost(t *testing.T) {
	// pgx accepts unix-socket hosts via the libpq URI's host= query parameter.
	// We surface them as keyword/value form because percent-encoding the
	// socket path into the URI host component is unreadable in a comment.
	client := NewClient(&Options{
		ConnString: "postgres://myuser@/mydb?host=/var/run/postgresql",
	})

	comment, err := client.ConnInfoComment()
	require.NoError(t, err)
	assert.Equal(t, "-- Connected to host=/var/run/postgresql dbname=mydb user=myuser", comment)
}

func TestConnInfoComment_URLEscapesSpecialChars(t *testing.T) {
	// User / dbname with characters that have URI meaning must be escaped so
	// the comment stays a parseable libpq URI. Round-tripping through url.URL
	// gives us this for free.
	client := NewClient(&Options{
		ConnString: "postgres://my%2Fuser@myhost:5432/my%20db",
	})

	comment, err := client.ConnInfoComment()
	require.NoError(t, err)
	// The user "my/user" and dbname "my db" must reappear escaped.
	assert.Contains(t, comment, "my%2Fuser")
	assert.Contains(t, comment, "my%20db")
}

func TestConnInfoComment_DBNameWithSlashEscaped(t *testing.T) {
	// url.URL.Path does NOT escape '/' by default — without setting RawPath a
	// dbname like "team/db" would render as multiple path segments
	// ("postgres://...:5432/team/db") and break URI round-trip. RawPath plus
	// url.PathEscape forces the '/' to be encoded as %2F.
	client := NewClient(&Options{
		ConnString: "postgres://myuser@myhost:5432/postgres",
		DBName:     "team/db",
	})

	comment, err := client.ConnInfoComment()
	require.NoError(t, err)
	assert.Equal(t, "-- Connected to postgres://myuser@myhost:5432/team%2Fdb", comment)
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
