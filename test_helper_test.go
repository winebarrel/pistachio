package pistachio_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func loadYAML[T any](t *testing.T, path string) *T {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var v T
	require.NoError(t, yaml.Unmarshal(data, &v))
	return &v
}
