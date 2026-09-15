package pistachio

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// initGitRepo makes a repository in a temporary directory and changes into
// it, so diff --git runs against it the way it runs against the repository
// the user is in. The user's own git configuration is kept out of the way.
func initGitRepo(t *testing.T) string {
	t.Helper()

	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)

	dir := t.TempDir()
	t.Chdir(dir)

	runGit(t, "init", "-q", "-b", "main")
	runGit(t, "config", "user.email", "test@example.com")
	runGit(t, "config", "user.name", "test")

	return dir
}

func runGit(t *testing.T, args ...string) string {
	t.Helper()

	out, err := exec.Command("git", args...).CombinedOutput()
	require.NoError(t, err, "git %v: %s", args, out)

	return string(out)
}

// gitCommit writes the files, removes the ones the working tree holds and the
// commit does not, and commits the result.
func gitCommit(t *testing.T, msg string, files map[string]string) {
	t.Helper()

	writeWorkingTree(t, files)
	runGit(t, "add", "-A")
	runGit(t, "commit", "-q", "-m", msg)
}

func writeWorkingTree(t *testing.T, files map[string]string) {
	t.Helper()

	for name, sql := range files {
		require.NoError(t, os.MkdirAll(filepath.Dir(name), 0o755))
		require.NoError(t, os.WriteFile(name, []byte(sql), 0o644))
	}
}

func diffGit(t *testing.T, gitRange string, files ...string) (*PlanResult, error) {
	t.Helper()

	client := NewClient(&Options{Schemas: []string{"public"}})

	return client.Diff(&DiffOptions{
		AllowDrop: []string{"all"},
		Git:       gitRange,
		Files:     files,
	})
}

// The two revisions of a file are the two sides of the diff.
func TestDiff_GitTwoRevisions(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})
	gitCommit(t, "second", map[string]string{"schema.sql": "CREATE TABLE users (id int, email text);"})

	got, err := diffGit(t, "HEAD^..HEAD", "schema.sql")
	require.NoError(t, err)
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN email text;", got.SQL)
}

// A range naming one revision compares it against the working tree, so an
// uncommitted edit is part of the diff.
func TestDiff_GitWorkingTree(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})
	writeWorkingTree(t, map[string]string{"schema.sql": "CREATE TABLE users (id int, email text);"})

	got, err := diffGit(t, "HEAD", "schema.sql")
	require.NoError(t, err)
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN email text;", got.SQL)
}

// A...B is B against its merge base with A, so a change A made after the
// branch point is not part of the diff.
func TestDiff_GitMergeBase(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "base", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})
	runGit(t, "checkout", "-q", "-b", "topic")
	gitCommit(t, "topic", map[string]string{"schema.sql": "CREATE TABLE users (id int, email text);"})
	runGit(t, "checkout", "-q", "main")
	gitCommit(t, "main", map[string]string{"schema.sql": "CREATE TABLE users (id int, name text);"})

	got, err := diffGit(t, "main...topic", "schema.sql")
	require.NoError(t, err)
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN email text;", got.SQL)
}

// A file the current revision does not hold is empty there, so what it holds
// is created rather than reported as a missing file.
func TestDiff_GitFileAdded(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})
	gitCommit(t, "second", map[string]string{"added.sql": "CREATE TABLE items (id int);"})

	got, err := diffGit(t, "HEAD^..HEAD", "schema.sql", "added.sql")
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "CREATE TABLE public.items")
	assert.NotContains(t, got.SQL, "DROP")
}

// The same the other way round: a file the desired revision does not hold is
// empty there, and what the current one holds is dropped.
func TestDiff_GitFileRemoved(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);", "gone.sql": "CREATE TABLE items (id int);"})
	require.NoError(t, os.Remove("gone.sql"))
	gitCommit(t, "second", nil)

	got, err := diffGit(t, "HEAD^..HEAD", "schema.sql", "gone.sql")
	require.NoError(t, err)
	assert.Equal(t, "DROP TABLE public.items;", got.SQL)
}

// A file the working tree no longer holds is a drop too.
func TestDiff_GitWorkingTreeFileRemoved(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"gone.sql": "CREATE TABLE items (id int);"})
	require.NoError(t, os.Remove("gone.sql"))

	got, err := diffGit(t, "HEAD", "gone.sql")
	require.NoError(t, err)
	assert.Equal(t, "DROP TABLE public.items;", got.SQL)
}

// Every file of a side is one schema, the way plan reads the files of a
// desired schema, so an index can sit in a different file from its table.
func TestDiff_GitMultipleFiles(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{
		"tables.sql":  "CREATE TABLE users (id int);",
		"indexes.sql": "",
	})
	gitCommit(t, "second", map[string]string{
		"tables.sql":  "CREATE TABLE users (id int, email text);",
		"indexes.sql": "CREATE INDEX idx_users_email ON users (email);",
	})

	got, err := diffGit(t, "HEAD^..HEAD", "tables.sql", "indexes.sql")
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "ALTER TABLE public.users ADD COLUMN email text;")
	assert.Contains(t, got.SQL, "CREATE INDEX idx_users_email ON public.users USING btree (email);")
}

// A path neither side holds is a typo, not an empty schema on both sides.
func TestDiff_GitPathInNeitherRevision(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})
	gitCommit(t, "second", map[string]string{"schema.sql": "CREATE TABLE users (id int, email text);"})

	_, err := diffGit(t, "HEAD^..HEAD", "schmea.sql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schmea.sql is in neither HEAD^ nor HEAD")
}

func TestDiff_GitPathInNeitherRevisionNorWorkingTree(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})

	_, err := diffGit(t, "HEAD", "schmea.sql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schmea.sql is in neither HEAD nor the working tree")
}

func TestDiff_GitUnknownRevision(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})

	_, err := diffGit(t, "nosuchrev..HEAD", "schema.sql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nosuchrev")
}

// A parse error names the revision along with the file, so it says which
// version of the file it is in.
func TestDiff_GitParseErrorNamesRevision(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})
	gitCommit(t, "second", map[string]string{"schema.sql": "CREATE TABEL users (id int);"})

	_, err := diffGit(t, "HEAD^..HEAD", "schema.sql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HEAD:schema.sql:1:8")
}

// The working tree side of a one-revision range is a plain file, so its name
// carries no revision.
func TestDiff_GitParseErrorNamesWorkingTreeFile(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})
	writeWorkingTree(t, map[string]string{"schema.sql": "CREATE TABEL users (id int);"})

	_, err := diffGit(t, "HEAD", "schema.sql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema.sql:1:8")
	assert.NotContains(t, err.Error(), "HEAD:schema.sql")
}

// A directive in the current revision counts as its state, as it does in the
// current file.
func TestDiff_GitDirectiveInCurrentRevision(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int, email text);\n-- pista:concurrently\nCREATE INDEX idx_users_email ON users (email);"})
	gitCommit(t, "second", map[string]string{"schema.sql": "CREATE TABLE users (id int, email text);"})

	got, err := diffGit(t, "HEAD^..HEAD", "schema.sql")
	require.NoError(t, err)
	assert.Equal(t, "DROP INDEX CONCURRENTLY public.idx_users_email;", got.SQL)
}

// Without --git the command takes one file per side and nothing else.
func TestDiff_WrongFileCount(t *testing.T) {
	client := NewClient(&Options{Schemas: []string{"public"}})

	for _, files := range [][]string{nil, {"only.sql"}, {"a.sql", "b.sql", "c.sql"}} {
		_, err := client.Diff(&DiffOptions{Files: files})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "diff takes a current and a desired schema SQL file")
	}
}

// A working tree path that is not a readable file is an error, not an empty
// side: a directory holds no schema to compare.
func TestDiff_GitWorkingTreeUnreadableFile(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})
	require.NoError(t, os.Mkdir("db", 0o755))

	_, err := diffGit(t, "HEAD", "db")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read SQL file")
}

// The current side names its revision too.
func TestDiff_GitParseErrorNamesCurrentRevision(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABEL users (id int);"})
	gitCommit(t, "second", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})

	_, err := diffGit(t, "HEAD^..HEAD", "schema.sql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HEAD^:schema.sql:1:8")
}

// Neither side changed, which is what a branch that leaves the schema alone
// looks like in CI.
func TestDiff_GitNoChanges(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"schema.sql": "CREATE TABLE users (id int);"})

	got, err := diffGit(t, "HEAD", "schema.sql")
	require.NoError(t, err)
	assert.False(t, got.HasChanges)
	assert.Empty(t, got.SQL)
}

// A directory is not an empty schema on that side: reporting it as one would
// plan a drop of everything the other side holds.
func TestDiff_GitDirectoryPath(t *testing.T) {
	initGitRepo(t)
	gitCommit(t, "first", map[string]string{"db/schema.sql": "CREATE TABLE users (id int);"})
	gitCommit(t, "second", map[string]string{"db/schema.sql": "CREATE TABLE users (id int, email text);"})

	_, err := diffGit(t, "HEAD^..HEAD", "db")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db is not a file in HEAD^")
}
