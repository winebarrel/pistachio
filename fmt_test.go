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
			results, err := client.Format(&pistachio.FmtOptions{Files: []string{tmpFile}})
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.Expected), strings.TrimSpace(results[tmpFile]))
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

func TestFormat_MultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()
	file1 := filepath.Join(tmpDir, "a.sql")
	file2 := filepath.Join(tmpDir, "b.sql")
	require.NoError(t, os.WriteFile(file1, []byte(`CREATE TABLE public.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));`), 0o644))
	require.NoError(t, os.WriteFile(file2, []byte(`CREATE TABLE public.posts (id integer NOT NULL, CONSTRAINT posts_pkey PRIMARY KEY (id));`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{Schemas: []string{"public"}})
	results, err := client.Format(&pistachio.FmtOptions{Files: []string{file1, file2}})
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Contains(t, results[file1], "CREATE TABLE public.users")
	assert.Contains(t, results[file2], "CREATE TABLE public.posts")
}
