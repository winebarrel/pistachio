package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/jsonschema"
)

func TestRun_Stdout(t *testing.T) {
	var out, errOut bytes.Buffer
	require.Equal(t, 0, run(nil, &out, &errOut))
	assert.Empty(t, errOut.String())

	want, err := jsonschema.Marshal()
	require.NoError(t, err)
	assert.Equal(t, string(want), out.String())
}

func TestRun_File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema.json")

	var out, errOut bytes.Buffer
	require.Equal(t, 0, run([]string{"-o", path}, &out, &errOut))
	assert.Empty(t, out.String(), "the schema goes to the file, not stdout")
	assert.Empty(t, errOut.String())

	got, err := os.ReadFile(path)
	require.NoError(t, err)

	want, err := jsonschema.Marshal()
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))
}

func TestRun_FileOverwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema.json")
	require.NoError(t, os.WriteFile(path, []byte("stale"), 0o644))

	var out, errOut bytes.Buffer
	require.Equal(t, 0, run([]string{"-o", path}, &out, &errOut))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.NotContains(t, string(got), "stale")
}

func TestRun_UnwritableFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("a read-only directory does not stop a write on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root writes into a read-only directory")
	}

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { os.Chmod(dir, 0o700) }) //nolint:errcheck

	var out, errOut bytes.Buffer
	assert.Equal(t, 1, run([]string{"-o", filepath.Join(dir, "schema.json")}, &out, &errOut))
	assert.Contains(t, errOut.String(), "pistachio:")
}

// errWriter fails on the first write, standing in for a closed pipe.
type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errors.New("broken pipe")
}

func TestRun_WriteError(t *testing.T) {
	var errOut bytes.Buffer
	assert.Equal(t, 1, run(nil, errWriter{}, &errOut))
	assert.Contains(t, errOut.String(), "pistachio:")
}

func TestRun_BadFlag(t *testing.T) {
	var out, errOut bytes.Buffer
	assert.Equal(t, 2, run([]string{"-nope"}, &out, &errOut))
	assert.Empty(t, out.String())
	assert.Contains(t, errOut.String(), "-nope")
}
