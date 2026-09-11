package pistachio_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
)

func TestClient_ParseSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema.sql")
	require.NoError(t, os.WriteFile(path, []byte("create table t (id integer);"), 0o644))

	client := pistachio.NewClient(&pistachio.Options{Schemas: []string{"myschema"}})
	result, err := client.ParseSchema([]string{path})
	require.NoError(t, err)

	// An unqualified name is qualified with the first schema.
	_, ok := result.Tables.GetOk("myschema.t")
	assert.True(t, ok)
}

func TestClient_ParseSchema_NoSchemas(t *testing.T) {
	client := pistachio.NewClient(&pistachio.Options{})
	_, err := client.ParseSchema([]string{"schema.sql"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one schema")
}

func TestClient_ParseSchema_MissingFile(t *testing.T) {
	client := pistachio.NewClient(&pistachio.Options{Schemas: []string{"public"}})
	_, err := client.ParseSchema([]string{filepath.Join(t.TempDir(), "nope.sql")})
	require.Error(t, err)
}
