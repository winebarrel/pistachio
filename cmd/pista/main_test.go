package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/testutil"
)

// exitCode is what the exit function passed to run panics with, so that run
// stops where os.Exit would have.
type exitCode int

// runCLI runs the command line and returns the exit code and what was
// written to stderr. A run that returns without calling exit is 0.
func runCLI(t *testing.T, stdout io.Writer, args ...string) (code int, stderr string) {
	t.Helper()
	var errBuf bytes.Buffer
	func() {
		defer func() {
			if r := recover(); r != nil {
				c, ok := r.(exitCode)
				if !ok {
					panic(r)
				}
				code = int(c)
			}
		}()
		run(args, stdout, &errBuf, func(c int) { panic(exitCode(c)) })
	}()
	return code, errBuf.String()
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

const usersTable = `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`

func TestRun_Plan(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	desired := writeFile(t, "desired.sql", usersTable)

	var out bytes.Buffer
	code, stderr := runCLI(t, &out, "plan", "-c", testutil.ConnString(), desired)
	assert.Equal(t, 0, code)
	assert.Empty(t, stderr)
	assert.Contains(t, out.String(), "CREATE TABLE public.users")
}

func TestRun_PlanCheckDiff(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	desired := writeFile(t, "desired.sql", usersTable)

	var out bytes.Buffer
	code, stderr := runCLI(t, &out, "plan", "--check", "-c", testutil.ConnString(), desired)
	assert.Equal(t, 2, code)
	assert.Empty(t, stderr)
	assert.Contains(t, out.String(), "CREATE TABLE public.users")
}

func TestRun_FmtCheckDiff(t *testing.T) {
	path := writeFile(t, "schema.sql", "CREATE TABLE users (id integer NOT NULL, name text);\n")

	var out bytes.Buffer
	code, stderr := runCLI(t, &out, "fmt", "--check", path)
	assert.Equal(t, 2, code)
	assert.Empty(t, stderr)
	assert.Contains(t, out.String(), path)
}

func TestRun_Help(t *testing.T) {
	var out bytes.Buffer
	code, stderr := runCLI(t, &out, "--help")
	assert.Equal(t, 0, code)
	assert.Empty(t, stderr)
	assert.Contains(t, out.String(), "Usage:")
	for _, cmd := range []string{"apply", "plan", "dump", "fmt"} {
		assert.Contains(t, out.String(), cmd)
	}
}

func TestRun_Version(t *testing.T) {
	var out bytes.Buffer
	code, stderr := runCLI(t, &out, "--version")
	assert.Equal(t, 0, code)
	assert.Empty(t, stderr)
	// version is set by the linker and empty under go test.
	assert.Equal(t, "\n", out.String())
}

func TestRun_ParseError(t *testing.T) {
	var out bytes.Buffer
	code, stderr := runCLI(t, &out, "no-such-command")
	// kong exits with 80 on a usage error.
	assert.Equal(t, 80, code)
	assert.Contains(t, stderr, "error: unexpected argument no-such-command")
}

func TestRun_RunError(t *testing.T) {
	desired := writeFile(t, "desired.sql", usersTable)

	var out bytes.Buffer
	code, stderr := runCLI(t, &out, "plan", "-c", "invalid://connection", desired)
	assert.Equal(t, 1, code)
	assert.Contains(t, stderr, "error:")
	assert.Empty(t, out.String())
}

func TestRun_Pager(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	testutil.SetupDB(t, ctx, conn, "")

	desired := writeFile(t, "desired.sql", usersTable)

	// StartPager only wraps an *os.File, and --pager skips the TTY check.
	t.Setenv("PISTA_PAGER", "cat")
	f, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	defer f.Close()

	code, stderr := runCLI(t, f, "plan", "--pager", "-c", testutil.ConnString(), desired)
	assert.Equal(t, 0, code)
	assert.Empty(t, stderr)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(got), "-- Connected to "), "got: %q", got)
	assert.Contains(t, string(got), "CREATE TABLE public.users")
}

// The flags that lay SQL out have nothing to change in a JSON document, so
// kong refuses them alongside --json. The parser rejects them before a
// database is reached, so no connection is needed.
func TestRun_DumpJSONExclusiveFlags(t *testing.T) {
	for _, flag := range []string{"--split=/tmp", "--sort-by-deps", "--no-format"} {
		t.Run(flag, func(t *testing.T) {
			var stdout bytes.Buffer
			code, stderr := runCLI(t, &stdout, "dump", "--json", flag)
			// kong exits with 80 on a usage error.
			assert.Equal(t, 80, code)
			assert.Contains(t, stderr, "can't be used together")
			assert.Contains(t, stderr, "--json")
			assert.Empty(t, stdout.String())
		})
	}
}

// The connection flags belong to the commands that open a connection, so they
// follow the command instead of preceding it. fmt reads no database and takes
// none of them; parse takes --schemas alone. kong rejects the rest before a
// database is reached, so no connection is needed.
func TestRun_ConnFlagPlacement(t *testing.T) {
	for _, args := range [][]string{
		{"-c", "postgres://postgres@localhost/postgres", "plan", "x.sql"},
		{"-n", "myschema", "dump"},
		{"fmt", "-c", "postgres://postgres@localhost/postgres", "x.sql"},
		{"fmt", "-n", "myschema", "x.sql"},
		{"parse", "-c", "postgres://postgres@localhost/postgres", "x.sql"},
		{"parse", "-m", "old=new", "x.sql"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout bytes.Buffer
			code, stderr := runCLI(t, &stdout, args...)
			// kong exits with 80 on a usage error.
			assert.Equal(t, 80, code)
			assert.Contains(t, stderr, "unknown flag")
			assert.Empty(t, stdout.String())
		})
	}
}

// parse keeps --schemas: it opens no connection, but the parser qualifies an
// unqualified name with it.
func TestRun_ParseSchemasFlag(t *testing.T) {
	path := writeFile(t, "schema.sql", "CREATE TABLE users (id integer);")

	var stdout bytes.Buffer
	code, stderr := runCLI(t, &stdout, "parse", "-n", "myschema", path)
	assert.Equal(t, 0, code)
	assert.Empty(t, stderr)
	assert.Contains(t, stdout.String(), `"myschema.users"`)
}
