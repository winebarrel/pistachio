package pistachio_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
)

// unreachableConnStr points at a port nothing listens on, so a run that
// reaches the connection fails with a connect error instead of the error under
// test. These cases need no database precisely because the files are read
// first.
const unreachableConnStr = "postgres://postgres@127.0.0.1:1/postgres?connect_timeout=1"

func unreachableClient() *pistachio.Client {
	return pistachio.NewClient(&pistachio.Options{
		ConnString: unreachableConnStr,
		Schemas:    []string{"public"},
	})
}

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func TestPlan_SyntaxErrorWithoutDatabase(t *testing.T) {
	path := writeTempFile(t, "desired.sql", "CREATE TABEL public.items (id integer);\n")

	_, err := unreachableClient().Plan(context.Background(), &pistachio.PlanOptions{Files: []string{path}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `syntax error at or near "TABEL"`)
	assert.NotContains(t, err.Error(), "failed to connect database")
}

func TestApply_SyntaxErrorWithoutDatabase(t *testing.T) {
	path := writeTempFile(t, "desired.sql", "CREATE TABEL public.items (id integer);\n")

	_, err := unreachableClient().Apply(context.Background(), &pistachio.ApplyOptions{Files: []string{path}}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `syntax error at or near "TABEL"`)
	assert.NotContains(t, err.Error(), "failed to connect database")
}

func TestPlan_MissingDesiredFileWithoutDatabase(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.sql")

	_, err := unreachableClient().Plan(context.Background(), &pistachio.PlanOptions{Files: []string{missing}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read SQL file")
	assert.NotContains(t, err.Error(), "failed to connect database")
}

func TestPlan_MissingPreSQLFileWithoutDatabase(t *testing.T) {
	desired := writeTempFile(t, "desired.sql", "CREATE TABLE public.items (id integer);\n")
	missing := filepath.Join(t.TempDir(), "pre.sql")

	_, err := unreachableClient().Plan(context.Background(), &pistachio.PlanOptions{
		Files:      []string{desired},
		PreSQLFile: missing,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read pre-SQL file")
	assert.NotContains(t, err.Error(), "failed to connect database")
}

func TestApply_MissingConcurrentlyPreSQLFileWithoutDatabase(t *testing.T) {
	desired := writeTempFile(t, "desired.sql", "CREATE TABLE public.items (id integer);\n")
	missing := filepath.Join(t.TempDir(), "pre.sql")

	_, err := unreachableClient().Apply(context.Background(), &pistachio.ApplyOptions{
		Files:                  []string{desired},
		ConcurrentlyPreSQLFile: missing,
	}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read concurrently-pre-SQL file")
	assert.NotContains(t, err.Error(), "failed to connect database")
}

// A schema file that parses still needs the database, so the run gets no
// further than the connection.
func TestPlan_ValidSchemaStillNeedsDatabase(t *testing.T) {
	path := writeTempFile(t, "desired.sql", "CREATE TABLE public.items (id integer);\n")

	_, err := unreachableClient().Plan(context.Background(), &pistachio.PlanOptions{Files: []string{path}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect database")
}
