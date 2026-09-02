package pistachio

import (
	"context"
	"os"
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

	_, err := client.connect(ctx, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestConnect_ReadOnly(t *testing.T) {
	connStr := os.Getenv("TEST_PISTA_CONN_STR")
	if connStr == "" {
		connStr = "postgres://postgres@localhost/postgres"
	}
	client := NewClient(&Options{ConnString: connStr})

	ctx := context.Background()
	conn, err := client.connect(ctx, true)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	var ro string
	require.NoError(t, conn.QueryRow(ctx, "SHOW default_transaction_read_only").Scan(&ro))
	assert.Equal(t, "on", ro)

	// A write must be rejected by the read-only transaction.
	_, err = conn.Exec(ctx, "CREATE TABLE pista_ro_probe (id integer)")
	require.Error(t, err)
}

func TestConnect_Writable(t *testing.T) {
	connStr := os.Getenv("TEST_PISTA_CONN_STR")
	if connStr == "" {
		connStr = "postgres://postgres@localhost/postgres"
	}
	client := NewClient(&Options{ConnString: connStr})

	ctx := context.Background()
	conn, err := client.connect(ctx, false)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	var ro string
	require.NoError(t, conn.QueryRow(ctx, "SHOW default_transaction_read_only").Scan(&ro))
	assert.Equal(t, "off", ro)
}

func TestConnect_SearchPath(t *testing.T) {
	connStr := os.Getenv("TEST_PISTA_CONN_STR")
	if connStr == "" {
		connStr = "postgres://postgres@localhost/postgres"
	}
	client := NewClient(&Options{ConnString: connStr})

	ctx := context.Background()
	conn, err := client.connect(ctx, true)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	var sp string
	require.NoError(t, conn.QueryRow(ctx, "SHOW search_path").Scan(&sp))
	assert.Equal(t, "public", sp)
}

// TestConnect_SearchPathIgnoresServerSide guards the reason the parameter is
// set at all: the catalog's pg_get_*def output drops the schema from every
// object on search_path, so a server-side "ALTER ROLE ... SET search_path" (or
// the database-level form) would otherwise decide what the diff compares.
// PGOPTIONS reaches the backend the same way those settings do, and pgx maps
// it to the "options" connection parameter, so it stands in for them here.
func TestConnect_SearchPathIgnoresServerSide(t *testing.T) {
	connStr := os.Getenv("TEST_PISTA_CONN_STR")
	if connStr == "" {
		connStr = "postgres://postgres@localhost/postgres"
	}
	t.Setenv("PGOPTIONS", "-c search_path=pg_catalog")
	client := NewClient(&Options{ConnString: connStr})

	ctx := context.Background()
	conn, err := client.connect(ctx, true)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	var sp string
	require.NoError(t, conn.QueryRow(ctx, "SHOW search_path").Scan(&sp))
	assert.Equal(t, "public", sp)
}

func TestConnect_SearchPathOption(t *testing.T) {
	connStr := os.Getenv("TEST_PISTA_CONN_STR")
	if connStr == "" {
		connStr = "postgres://postgres@localhost/postgres"
	}
	// Set on the server side too, to show the option wins over it.
	t.Setenv("PGOPTIONS", "-c search_path=pg_catalog")
	searchPath := "public, pg_temp"
	client := NewClient(&Options{ConnString: connStr, SearchPath: &searchPath})

	ctx := context.Background()
	conn, err := client.connect(ctx, true)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	var sp string
	require.NoError(t, conn.QueryRow(ctx, "SHOW search_path").Scan(&sp))
	assert.Equal(t, "public, pg_temp", sp)
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
	assert.Contains(t, err.Error(), "failed to parse connection string")
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
	// url.URL.Path does NOT escape '/' by default; without setting RawPath a
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

	_, err := client.connect(context.Background(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse connection string")
}
