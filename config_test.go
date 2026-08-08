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

// testCLI mirrors the global options plus the meta flags (--config, --version,
// --pager) so the YAML configuration loader can be exercised without a running
// database. --help is added by kong automatically.
type testCLI struct {
	pistachio.Options
	Config  kong.ConfigFlag `name:"config" short:"C" env:"PISTA_CONFIG"`
	Version kong.VersionFlag
	Pager   *bool `name:"pager" negatable:""`
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
		kong.Vars{"version": "test"},
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

// The --search-path default lives in a struct tag, while connect falls back to
// DefaultSearchPath for a library caller that builds Options directly. The two
// have to name the same path, or the CLI and the library would open different
// connections.
func TestOptions_SearchPathDefault(t *testing.T) {
	cli, err := parseWithConfig(t)
	require.NoError(t, err)

	require.NotNil(t, cli.SearchPath)
	assert.Equal(t, pistachio.DefaultSearchPath, *cli.SearchPath)
	assert.Equal(t, "public", *cli.SearchPath)
}

func TestOptions_SearchPathFromConfig(t *testing.T) {
	path := writeConfig(t, "search-path: myschema, public\n")

	cli, err := parseWithConfig(t, "--config", path)
	require.NoError(t, err)

	require.NotNil(t, cli.SearchPath)
	assert.Equal(t, "myschema, public", *cli.SearchPath)
}

// An empty --search-path is a path of its own, one that reaches nothing, so it
// has to arrive as a set-but-empty value rather than as a missing one.
func TestOptions_SearchPathEmpty(t *testing.T) {
	cli, err := parseWithConfig(t, "--search-path=")
	require.NoError(t, err)

	require.NotNil(t, cli.SearchPath)
	assert.Empty(t, *cli.SearchPath)
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

// The config file path can be provided via $PISTA_CONFIG.
func TestYAMLConfig_PathFromEnv(t *testing.T) {
	path := writeConfig(t, "conn-string: postgres://config@db/app\n")
	t.Setenv("PISTA_CONFIG", path)

	cli, err := parseWithConfig(t)
	require.NoError(t, err)

	assert.Equal(t, "postgres://config@db/app", cli.ConnString)
}

// --config on the command line overrides the $PISTA_CONFIG path.
func TestYAMLConfig_FlagPathOverridesEnvPath(t *testing.T) {
	envPath := writeConfig(t, "conn-string: postgres://env-file@db/app\n")
	flagPath := writeConfig(t, "conn-string: postgres://flag-file@db/app\n")
	t.Setenv("PISTA_CONFIG", envPath)

	cli, err := parseWithConfig(t, "--config", flagPath)
	require.NoError(t, err)

	assert.Equal(t, "postgres://flag-file@db/app", cli.ConnString)
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
// is accepted and ignored (a single file can be shared across commands).
func TestYAMLConfig_CommandSpecificFlag(t *testing.T) {
	// omit-schema belongs to dump; bulk-alter belongs to plan.
	content := `
omit-schema: true
bulk-alter: true
`

	t.Run("dump uses omit-schema, ignores bulk-alter", func(t *testing.T) {
		path := writeConfig(t, content)
		cli, err := parseCommandCLI(t, "--config", path, "dump")
		require.NoError(t, err)
		assert.True(t, cli.Dump.OmitSchema)
	})

	t.Run("plan uses bulk-alter, ignores omit-schema", func(t *testing.T) {
		path := writeConfig(t, content)
		cli, err := parseCommandCLI(t, "--config", path, "plan", "schema.sql")
		require.NoError(t, err)
		assert.True(t, cli.Plan.BulkAlter)
	})
}

// Built-in meta flags (help, version, and config itself) are not configurable;
// naming one is an unknown key. The values below are valid for each flag's
// type, so the only possible error is the unknown-key rejection.
func TestYAMLConfig_MetaFlagsAreNotConfigurable(t *testing.T) {
	cases := map[string]string{
		"version": "version: true\n",
		"help":    "help: true\n",
		"config":  "config: other.yml\n",
	}
	for key, content := range cases {
		t.Run(key, func(t *testing.T) {
			path := writeConfig(t, content)
			_, err := parseWithConfig(t, "--config", path)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown config key")
			assert.Contains(t, err.Error(), key)
		})
	}
}
