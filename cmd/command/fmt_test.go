package command_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/cmd/command"
)

const unformattedSQL = `create table public.items (id integer not null, name text);`

const formattedSQL = `create table public.items (
    id integer not null,
    name text
);
`

func writeSQLFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	return path
}

func TestFmt_Run(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", unformattedSQL)

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}}
	require.NoError(t, cmd.Run(&buf))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, formattedSQL, string(got))
	assert.Equal(t, path+"\n", buf.String())
}

func TestFmt_Run_AlreadyFormatted(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", formattedSQL)

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}}
	require.NoError(t, cmd.Run(&buf))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, formattedSQL, string(got))
	assert.Empty(t, buf.String())
}

func TestFmt_Run_KeepsFileMode(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", unformattedSQL)
	require.NoError(t, os.Chmod(path, 0o600))

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}}
	require.NoError(t, cmd.Run(&buf))

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestFmt_Run_Check(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", unformattedSQL)

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}, Check: true}
	err := cmd.Run(&buf)
	require.ErrorIs(t, err, command.ErrFormatDiff)

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, unformattedSQL, string(got), "--check must not write the file")
	assert.Equal(t, path+"\n", buf.String())
}

func TestFmt_Run_CheckFormatted(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", formattedSQL)

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}, Check: true}
	require.NoError(t, cmd.Run(&buf))
	assert.Empty(t, buf.String())
}

func TestFmt_Run_ParseErrorLeavesFile(t *testing.T) {
	broken := "CREATE TABLE (;\n"
	path := writeSQLFile(t, "broken.sql", broken)
	ok := writeSQLFile(t, "ok.sql", unformattedSQL)

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path, ok}}
	err := cmd.Run(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to format 1 file(s)")

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, broken, string(got), "a file that cannot be parsed is left as it was")

	// The files after it are still formatted.
	rest, err := os.ReadFile(ok)
	require.NoError(t, err)
	assert.Equal(t, formattedSQL, string(rest))
}

func TestFmt_Run_SeveralFiles(t *testing.T) {
	dir := t.TempDir()
	one := filepath.Join(dir, "one.sql")
	two := filepath.Join(dir, "two.sql")
	empty := filepath.Join(dir, "empty.sql")
	require.NoError(t, os.WriteFile(one, []byte(unformattedSQL), 0o644))
	require.NoError(t, os.WriteFile(two, []byte(formattedSQL), 0o644))
	require.NoError(t, os.WriteFile(empty, nil, 0o644))

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{one, two, empty}}
	require.NoError(t, cmd.Run(&buf))

	// Only the file that changed is named.
	assert.Equal(t, one+"\n", buf.String())

	got, err := os.ReadFile(empty)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestFmt_Run_UnwritableDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes into a read-only directory")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "schema.sql")
	require.NoError(t, os.WriteFile(path, []byte(unformattedSQL), 0o644))
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { os.Chmod(dir, 0o700) }) //nolint:errcheck

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}}
	err := cmd.Run(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to format 1 file(s)")

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, unformattedSQL, string(got), "the file is left as it was")
}

func TestFmt_Run_MissingFile(t *testing.T) {
	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{filepath.Join(t.TempDir(), "nope.sql")}}
	err := cmd.Run(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to format 1 file(s)")
}
