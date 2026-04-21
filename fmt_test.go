package pistachio_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
)

type fmtTestCase struct {
	Input    string `yaml:"input"`
	Expected string `yaml:"expected"`
}

func TestFormat(t *testing.T) {
	files, err := filepath.Glob("testdata/fmt/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")
		t.Run(name, func(t *testing.T) {
			tc := loadYAML[fmtTestCase](t, file)

			tmpFile := filepath.Join(t.TempDir(), "input.sql")
			require.NoError(t, os.WriteFile(tmpFile, []byte(tc.Input), 0o644))

			client := pistachio.NewClient(&pistachio.Options{Schemas: []string{"public"}})
			got, err := client.Format(&pistachio.FmtOptions{Files: []string{tmpFile}})
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.Expected), strings.TrimSpace(got))
		})
	}
}

func TestFormat_InvalidFile(t *testing.T) {
	client := pistachio.NewClient(&pistachio.Options{Schemas: []string{"public"}})
	_, err := client.Format(&pistachio.FmtOptions{Files: []string{"/nonexistent/file.sql"}})
	require.Error(t, err)
}

func TestFormat_InvalidSQL(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "bad.sql")
	require.NoError(t, os.WriteFile(tmpFile, []byte("NOT VALID SQL {{{}}}"), 0o644))

	client := pistachio.NewClient(&pistachio.Options{Schemas: []string{"public"}})
	_, err := client.Format(&pistachio.FmtOptions{Files: []string{tmpFile}})
	require.Error(t, err)
}

func TestFormatFile(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "test.sql")
	require.NoError(t, os.WriteFile(tmpFile, []byte(`CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{Schemas: []string{"public"}})
	got, err := client.FormatFile(tmpFile)
	require.NoError(t, err)
	assert.Contains(t, got, "CREATE TABLE public.users")
	assert.Contains(t, got, "    id integer NOT NULL")
}

func TestFormatFile_InvalidFile(t *testing.T) {
	client := pistachio.NewClient(&pistachio.Options{Schemas: []string{"public"}})
	_, err := client.FormatFile("/nonexistent/file.sql")
	require.Error(t, err)
}
