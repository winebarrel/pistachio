package pistachio_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
)

// testCLI mirrors the global options plus the --config flag so the YAML
// configuration loader can be exercised without a running database.
type testCLI struct {
	pistachio.Options
	Config kong.ConfigFlag `name:"config" short:"C"`
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "pista.yml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func parseWithConfig(t *testing.T, args ...string) (*testCLI, error) {
	t.Helper()
	var cli testCLI
	parser, err := kong.New(&cli,
		kong.Name("pista"),
		kong.Configuration(pistachio.YAMLConfig),
		kong.Exit(func(int) {}),
	)
	require.NoError(t, err)
	_, err = parser.Parse(args)
	return &cli, err
}

func TestYAMLConfig_LoadsOptions(t *testing.T) {
	path := writeConfig(t, `
conn-string: postgres://user@db/app
dbname: app
schemas:
  - public
  - billing
schema-map:
  old: new
`)

	cli, err := parseWithConfig(t, "--config", path)
	require.NoError(t, err)

	assert.Equal(t, "postgres://user@db/app", cli.ConnString)
	assert.Equal(t, "app", cli.DBName)
	assert.Equal(t, []string{"public", "billing"}, cli.Schemas)
	assert.Equal(t, map[string]string{"old": "new"}, cli.SchemaMap)
}

func TestYAMLConfig_CLIFlagWins(t *testing.T) {
	path := writeConfig(t, "conn-string: postgres://config@db/app\n")

	cli, err := parseWithConfig(t, "--config", path, "--conn-string", "postgres://cli@db/app")
	require.NoError(t, err)

	assert.Equal(t, "postgres://cli@db/app", cli.ConnString)
}

// Pin the precedence: command-line flag > environment variable > config file.
// An environment variable overrides the config file.
func TestYAMLConfig_EnvOverridesConfig(t *testing.T) {
	t.Setenv("PISTA_CONN_STR", "postgres://env@db/app")
	path := writeConfig(t, "conn-string: postgres://config@db/app\n")

	cli, err := parseWithConfig(t, "--config", path)
	require.NoError(t, err)

	assert.Equal(t, "postgres://env@db/app", cli.ConnString)
}

// A command-line flag still wins over both the env var and the config file.
func TestYAMLConfig_FlagOverridesEnvAndConfig(t *testing.T) {
	t.Setenv("PISTA_CONN_STR", "postgres://env@db/app")
	path := writeConfig(t, "conn-string: postgres://config@db/app\n")

	cli, err := parseWithConfig(t, "--config", path, "--conn-string", "postgres://cli@db/app")
	require.NoError(t, err)

	assert.Equal(t, "postgres://cli@db/app", cli.ConnString)
}

// The config file is used when no env var or flag is set.
func TestYAMLConfig_ConfigUsedWithoutEnv(t *testing.T) {
	path := writeConfig(t, "conn-string: postgres://config@db/app\n")

	cli, err := parseWithConfig(t, "--config", path)
	require.NoError(t, err)

	assert.Equal(t, "postgres://config@db/app", cli.ConnString)
}

// Without a config file the env var still applies.
func TestYAMLConfig_EnvUsedWithoutConfig(t *testing.T) {
	t.Setenv("PISTA_CONN_STR", "postgres://env@db/app")

	cli, err := parseWithConfig(t)
	require.NoError(t, err)

	assert.Equal(t, "postgres://env@db/app", cli.ConnString)
}

func TestYAMLConfig_UnknownKeyIsRejected(t *testing.T) {
	path := writeConfig(t, "bogus: 1\n")

	_, err := parseWithConfig(t, "--config", path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bogus")
}

// Keys must match flag names exactly; the snake_case variant is not accepted
// and is reported as an unknown key.
func TestYAMLConfig_ExactKeyNameOnly(t *testing.T) {
	path := writeConfig(t, "conn_string: postgres://config@db/app\n")

	_, err := parseWithConfig(t, "--config", path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conn_string")
}

func TestYAMLConfig_EmptyFile(t *testing.T) {
	path := writeConfig(t, "")

	cli, err := parseWithConfig(t, "--config", path)
	require.NoError(t, err)

	// Falls back to the flag default.
	assert.Equal(t, "postgres://postgres@localhost/postgres", cli.ConnString)
}

func TestYAMLConfig_MissingFileIsError(t *testing.T) {
	_, err := parseWithConfig(t, "--config", filepath.Join(t.TempDir(), "nope.yml"))
	require.Error(t, err)
}

func TestYAMLConfig_InvalidYAMLIsError(t *testing.T) {
	// A top-level sequence cannot decode into a mapping.
	path := writeConfig(t, "- one\n- two\n")

	_, err := parseWithConfig(t, "--config", path)
	require.Error(t, err)
}

// commandCLI mirrors the real CLI closely enough to exercise command-specific
// flags loaded from a shared config file.
type commandCLI struct {
	pistachio.Options
	Config kong.ConfigFlag `name:"config" short:"C"`
	Plan   struct {
		pistachio.PlanOptions
	} `cmd:""`
	Dump struct {
		pistachio.DumpOptions
	} `cmd:""`
}

func parseCommandCLI(t *testing.T, args ...string) (*commandCLI, error) {
	t.Helper()
	var cli commandCLI
	parser, err := kong.New(&cli,
		kong.Name("pista"),
		kong.Configuration(pistachio.YAMLConfig),
		kong.Exit(func(int) {}),
	)
	require.NoError(t, err)
	_, err = parser.Parse(args)
	return &cli, err
}

// A command-specific flag is loaded, and a key that belongs to another command
// is accepted (a single file can be shared across commands).
func TestYAMLConfig_CommandSpecificFlag(t *testing.T) {
	path := writeConfig(t, `
omit-schema: true
bulk-alter: true
`)

	cli, err := parseCommandCLI(t, "--config", path, "dump")
	require.NoError(t, err)

	assert.True(t, cli.Dump.OmitSchema)
}
