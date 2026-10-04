package command_test

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
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
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not carry the Unix permission bits")
	}

	path := writeSQLFile(t, "schema.sql", unformattedSQL)
	require.NoError(t, os.Chmod(path, 0o600))

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}}
	require.NoError(t, cmd.Run(&buf))

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

// A symlink is formatted through to the file it points at, and stays a
// symlink, as does a link to that link. The links and the file sit in
// different directories, so the temporary file has to go next to the file.
// The file keeps its mode.
func TestFmt_Run_Symlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating a symlink needs a privilege on Windows")
	}

	target := writeSQLFile(t, "schema.sql", unformattedSQL)
	require.NoError(t, os.Chmod(target, 0o600))
	link := filepath.Join(t.TempDir(), "link.sql")
	require.NoError(t, os.Symlink(target, link))
	chain := filepath.Join(t.TempDir(), "chain.sql")
	require.NoError(t, os.Symlink(link, chain))

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{chain}}
	require.NoError(t, cmd.Run(&buf))

	for _, l := range []string{chain, link} {
		info, err := os.Lstat(l)
		require.NoError(t, err)
		assert.NotZero(t, info.Mode()&os.ModeSymlink, "%s must stay a link", l)
	}

	got, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Equal(t, formattedSQL, string(got))
	assert.Equal(t, chain+"\n", buf.String())

	info, err := os.Stat(target)
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

const renamedSQL = `-- pista:renamed-from public.old_items
create table public.items (id integer not null,
    -- pista:renamed-from title

    name text);

-- pista:renamed-from old_idx
create index items_name_idx on public.items (name);
`

const strippedSQL = `create table public.items (
    id integer not null,

    name text
);

create index items_name_idx on public.items (name);
`

// --strip-renamed-from removes the directives and formats what is left.
func TestFmt_Run_StripRenamedFrom(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", renamedSQL)

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}, StripRenamedFrom: true}
	require.NoError(t, cmd.Run(&buf))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, strippedSQL, string(got))
	assert.Equal(t, path+"\n", buf.String())
}

// A directive between two blank lines leaves them both, and formatting folds
// them into one.
func TestFmt_Run_StripRenamedFromBlankLines(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", formattedSQL+"\n-- pista:renamed-from old_idx\n\n"+
		"create index items_name_idx on public.items (name);\n")

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}, StripRenamedFrom: true}
	require.NoError(t, cmd.Run(&buf))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, formattedSQL+"\ncreate index items_name_idx on public.items (name);\n", string(got))
}

// A file that holds nothing but directives ends up empty.
func TestFmt_Run_StripRenamedFromOnly(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", "-- pista:renamed-from old_items\n")

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}, StripRenamedFrom: true}
	require.NoError(t, cmd.Run(&buf))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Equal(t, path+"\n", buf.String())
}

// Without the flag, the directives stay.
func TestFmt_Run_KeepsRenamedFrom(t *testing.T) {
	path := writeSQLFile(t, "schema.sql", "-- pista:renamed-from old_items\n"+formattedSQL)

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}}
	require.NoError(t, cmd.Run(&buf))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "-- pista:renamed-from old_items\n"+formattedSQL, string(got))
	assert.Empty(t, buf.String())
}

// With --check, a formatted file that still has a directive is reported and
// left as it was.
func TestFmt_Run_CheckStripRenamedFrom(t *testing.T) {
	content := "-- pista:renamed-from old_items\n" + formattedSQL
	path := writeSQLFile(t, "schema.sql", content)

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}, Check: true, StripRenamedFrom: true}
	err := cmd.Run(&buf)
	require.ErrorIs(t, err, command.ErrFormatDiff)

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, content, string(got))
	assert.Equal(t, path+"\n", buf.String())
}

// A file that does not scan is reported and left as it was.
func TestFmt_Run_StripRenamedFromScanError(t *testing.T) {
	broken := "-- pista:renamed-from old_items\nSELECT 'abc\n"
	path := writeSQLFile(t, "broken.sql", broken)

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}, StripRenamedFrom: true}
	err := cmd.Run(&buf)
	require.Error(t, err)

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, broken, string(got))
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
	if runtime.GOOS == "windows" {
		t.Skip("a read-only directory does not stop a write on Windows")
	}
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

func TestFmt_Run_UnreadableFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not carry the Unix permission bits")
	}
	if os.Geteuid() == 0 {
		t.Skip("root reads a file with no permission bits")
	}

	path := writeSQLFile(t, "schema.sql", unformattedSQL)
	require.NoError(t, os.Chmod(path, 0o000))

	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{path}}
	err := cmd.Run(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to format 1 file(s)")
}

// A directory resolves like a file but cannot be read, so it fails as one
// file rather than aborting the run. Unlike an unreadable file, this holds
// under root too.
func TestFmt_Run_Directory(t *testing.T) {
	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{t.TempDir()}}
	err := cmd.Run(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to format 1 file(s)")
}

func TestFmt_Run_MissingFile(t *testing.T) {
	var buf bytes.Buffer
	cmd := &command.Fmt{Files: []string{filepath.Join(t.TempDir(), "nope.sql")}}
	err := cmd.Run(&buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to format 1 file(s)")
}
