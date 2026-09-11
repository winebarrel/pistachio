package main

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/jsonschema"
)

// schemaDir makes the directory the schema is kept in, under a temporary
// working directory, so a run writes there rather than into the repository.
func schemaDir(t *testing.T) {
	t.Helper()

	t.Chdir(t.TempDir())
	require.NoError(t, os.MkdirAll(filepath.Dir(jsonschema.Path), 0o755))
}

func TestRun(t *testing.T) {
	schemaDir(t)

	var errOut bytes.Buffer
	require.Equal(t, 0, run(&errOut))
	assert.Empty(t, errOut.String())

	got, err := os.ReadFile(jsonschema.Path)
	require.NoError(t, err)

	want, err := jsonschema.Marshal()
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))
}

func TestRun_Overwrites(t *testing.T) {
	schemaDir(t)
	require.NoError(t, os.WriteFile(jsonschema.Path, []byte("stale"), 0o644))

	var errOut bytes.Buffer
	require.Equal(t, 0, run(&errOut))

	got, err := os.ReadFile(jsonschema.Path)
	require.NoError(t, err)
	assert.NotContains(t, string(got), "stale")
}

// A run from a directory that does not hold the schema's own fails rather than
// writing somewhere else.
func TestRun_MissingDir(t *testing.T) {
	t.Chdir(t.TempDir())

	var errOut bytes.Buffer
	assert.Equal(t, 1, run(&errOut))
	assert.Contains(t, errOut.String(), "pistachio:")
}

func TestRun_UnwritableFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("a read-only directory does not stop a write on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root writes into a read-only directory")
	}

	schemaDir(t)
	dir := filepath.Dir(jsonschema.Path)
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { os.Chmod(dir, 0o700) }) //nolint:errcheck

	var errOut bytes.Buffer
	assert.Equal(t, 1, run(&errOut))
	assert.Contains(t, errOut.String(), "pistachio:")
}
