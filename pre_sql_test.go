package pistachio_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
)

func TestResolvePreSQL_DirectString(t *testing.T) {
	got, err := pistachio.ResolvePreSQL("SELECT 1;", "")
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1;", got)
}

func TestResolvePreSQL_File(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "pre.sql")
	require.NoError(t, os.WriteFile(tmpFile, []byte("SELECT 2;"), 0o644))

	got, err := pistachio.ResolvePreSQL("", tmpFile)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 2;", got)
}

func TestResolvePreSQL_DirectTakesPrecedence(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "pre.sql")
	require.NoError(t, os.WriteFile(tmpFile, []byte("SELECT 2;"), 0o644))

	got, err := pistachio.ResolvePreSQL("SELECT 1;", tmpFile)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1;", got)
}

func TestResolvePreSQL_Empty(t *testing.T) {
	got, err := pistachio.ResolvePreSQL("", "")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestResolvePreSQL_FileNotFound(t *testing.T) {
	_, err := pistachio.ResolvePreSQL("", "/nonexistent/pre.sql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read pre-SQL file")
}

func TestResolveConcurrentlyPreSQL_DirectString(t *testing.T) {
	got, err := pistachio.ResolveConcurrentlyPreSQL("SET lock_timeout = '5s';", "")
	require.NoError(t, err)
	assert.Equal(t, "SET lock_timeout = '5s';", got)
}

func TestResolveConcurrentlyPreSQL_FileNotFound(t *testing.T) {
	_, err := pistachio.ResolveConcurrentlyPreSQL("", "/nonexistent/pre.sql")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read concurrently-pre-SQL file")
}
