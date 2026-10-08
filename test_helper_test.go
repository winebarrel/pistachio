package pistachio

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// loadYAML rejects a key the struct has no field for, so a misspelled
// assertion key fails the case instead of being dropped silently.
func loadYAML[T any](t *testing.T, path string) *T {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var v T
	require.NoError(t, dec.Decode(&v))
	return &v
}
