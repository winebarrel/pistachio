package gitfile_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/gitfile"
)

// initRepo makes a repository in a temporary directory and changes into it,
// so the git commands under test run against it the way they run against the
// repository the user is in. The user's own git configuration is kept out of
// the way: a commit.gpgsign or a hook there would otherwise reach the test.
func initRepo(t *testing.T) string {
	t.Helper()

	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)

	dir := t.TempDir()
	t.Chdir(dir)

	git(t, "init", "-q", "-b", "main")
	git(t, "config", "user.email", "test@example.com")
	git(t, "config", "user.name", "test")

	return dir
}

func git(t *testing.T, args ...string) string {
	t.Helper()

	out, err := exec.Command("git", args...).CombinedOutput()
	require.NoError(t, err, "git %v: %s", args, out)

	return string(out)
}

// commit writes the files and commits them. A nil value deletes the file.
func commit(t *testing.T, msg string, files map[string]string) {
	t.Helper()

	for name, content := range files {
		path := filepath.Join(".", name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}

	git(t, "add", "-A")
	git(t, "commit", "-q", "-m", msg)
}

func TestParseRange_TwoDot(t *testing.T) {
	initRepo(t)
	commit(t, "first", map[string]string{"schema.sql": "CREATE TABLE t (id int);"})
	commit(t, "second", map[string]string{"schema.sql": "CREATE TABLE t (id int, name text);"})

	got, err := gitfile.ParseRange("HEAD^..HEAD")
	require.NoError(t, err)
	assert.Equal(t, "HEAD^", got.Current)
	assert.Equal(t, "HEAD", got.Desired)
}

// A bare revision compares it against the working tree, which Desired names
// with the empty string.
func TestParseRange_SingleRevision(t *testing.T) {
	initRepo(t)
	commit(t, "first", map[string]string{"schema.sql": "CREATE TABLE t (id int);"})

	got, err := gitfile.ParseRange("HEAD")
	require.NoError(t, err)
	assert.Equal(t, "HEAD", got.Current)
	assert.Empty(t, got.Desired)
}

// An omitted side is HEAD, as it is for git diff.
func TestParseRange_OmittedSides(t *testing.T) {
	initRepo(t)
	commit(t, "first", map[string]string{"schema.sql": "CREATE TABLE t (id int);"})
	commit(t, "second", map[string]string{"schema.sql": "CREATE TABLE t (id int, name text);"})

	got, err := gitfile.ParseRange("HEAD^..")
	require.NoError(t, err)
	assert.Equal(t, "HEAD^", got.Current)
	assert.Equal(t, "HEAD", got.Desired)

	got, err = gitfile.ParseRange("..HEAD")
	require.NoError(t, err)
	assert.Equal(t, "HEAD", got.Current)
	assert.Equal(t, "HEAD", got.Desired)
}

// A...B compares B against its merge base with A, so the current side is the
// merge base commit rather than A.
func TestParseRange_ThreeDot(t *testing.T) {
	initRepo(t)
	commit(t, "base", map[string]string{"schema.sql": "CREATE TABLE t (id int);"})
	base := git(t, "rev-parse", "HEAD")
	git(t, "checkout", "-q", "-b", "topic")
	commit(t, "topic", map[string]string{"schema.sql": "CREATE TABLE t (id int, name text);"})
	git(t, "checkout", "-q", "main")
	commit(t, "main", map[string]string{"other.sql": "CREATE TABLE u (id int);"})

	got, err := gitfile.ParseRange("main...topic")
	require.NoError(t, err)
	assert.Equal(t, base[:len(base)-1], got.Current)
	assert.Equal(t, "topic", got.Desired)
}

// Both endpoints are verified before the merge base is asked for, so every
// form of the range names the revision the same way.
func TestParseRange_UnknownRevision(t *testing.T) {
	initRepo(t)
	commit(t, "first", map[string]string{"schema.sql": "CREATE TABLE t (id int);"})

	for _, spec := range []string{
		"nosuchrev",
		"nosuchrev..HEAD",
		"HEAD..nosuchrev",
		"nosuchrev...HEAD",
		"HEAD...nosuchrev",
	} {
		_, err := gitfile.ParseRange(spec)
		require.Error(t, err, spec)
		assert.Contains(t, err.Error(), "git revision nosuchrev", spec)
	}
}

func TestParseRange_Empty(t *testing.T) {
	initRepo(t)
	commit(t, "first", map[string]string{"schema.sql": "CREATE TABLE t (id int);"})

	_, err := gitfile.ParseRange("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestParseRange_OutsideRepository(t *testing.T) {
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	t.Setenv("GIT_CEILING_DIRECTORIES", filepath.Dir(t.TempDir()))
	t.Chdir(t.TempDir())

	_, err := gitfile.ParseRange("HEAD")
	require.Error(t, err)
}

func TestRead(t *testing.T) {
	initRepo(t)
	commit(t, "first", map[string]string{"schema.sql": "CREATE TABLE t (id int);\n"})
	commit(t, "second", map[string]string{"schema.sql": "CREATE TABLE t (id int, name text);\n"})

	sql, ok, err := gitfile.Read("HEAD^", "schema.sql")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "CREATE TABLE t (id int);\n", sql)

	sql, ok, err = gitfile.Read("HEAD", "schema.sql")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "CREATE TABLE t (id int, name text);\n", sql)
}

// A path the revision does not hold is not an error: it was added or removed
// between the two revisions, and the side without it is an empty file.
func TestRead_MissingPath(t *testing.T) {
	initRepo(t)
	commit(t, "first", map[string]string{"schema.sql": "CREATE TABLE t (id int);\n"})

	sql, ok, err := gitfile.Read("HEAD", "added.sql")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Empty(t, sql)
}

// The path is read relative to the working directory, not the repository
// root, so the path the user typed is the path git looks up.
func TestRead_RelativeToWorkingDirectory(t *testing.T) {
	dir := initRepo(t)
	commit(t, "first", map[string]string{"db/schema.sql": "CREATE TABLE t (id int);\n"})

	t.Chdir(filepath.Join(dir, "db"))

	sql, ok, err := gitfile.Read("HEAD", "schema.sql")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "CREATE TABLE t (id int);\n", sql)

	// The same file by a path that climbs out of the directory first.
	sql, ok, err = gitfile.Read("HEAD", "../db/schema.sql")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "CREATE TABLE t (id int);\n", sql)
}

// An absolute path names the same file as the relative one.
func TestRead_AbsolutePath(t *testing.T) {
	dir := initRepo(t)
	commit(t, "first", map[string]string{"schema.sql": "CREATE TABLE t (id int);\n"})

	sql, ok, err := gitfile.Read("HEAD", filepath.Join(dir, "schema.sql"))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "CREATE TABLE t (id int);\n", sql)
}

// A directory is not an empty file: reporting it as one would silently drop
// every object the schema holds, so it is an error.
func TestRead_Directory(t *testing.T) {
	initRepo(t)
	commit(t, "first", map[string]string{"db/schema.sql": "CREATE TABLE t (id int);\n"})

	_, _, err := gitfile.Read("HEAD", "db")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a file")
}

// git itself has to be there. Without it the error is the one exec gives,
// which nothing else would say.
func TestParseRange_NoGitOnPath(t *testing.T) {
	initRepo(t)
	commit(t, "first", map[string]string{"schema.sql": "CREATE TABLE t (id int);"})

	t.Setenv("PATH", "")

	_, err := gitfile.ParseRange("HEAD")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "git")
}
