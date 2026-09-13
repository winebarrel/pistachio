package pistachio

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type diffTestCase struct {
	Current                  string          `yaml:"current"`
	Desired                  string          `yaml:"desired"`
	Diff                     string          `yaml:"diff"`
	Error                    string          `yaml:"error"`
	Count                    *expectedCount  `yaml:"count,omitempty"`
	DropPolicy               *planDropPolicy `yaml:"drop_policy,omitempty"`
	DisallowedDrops          string          `yaml:"disallowed_drops,omitempty"`
	Ignored                  string          `yaml:"ignored,omitempty"`
	DisableIndexConcurrently bool            `yaml:"disable_index_concurrently,omitempty"`
	ForceIndexConcurrently   bool            `yaml:"force_index_concurrently,omitempty"`
	BulkAlter                bool            `yaml:"bulk_alter,omitempty"`
	AssumeValidated          bool            `yaml:"assume_validated,omitempty"`
	Include                  []string        `yaml:"include,omitempty"`
	Exclude                  []string        `yaml:"exclude,omitempty"`
	Enable                   []string        `yaml:"enable,omitempty"`
	Disable                  []string        `yaml:"disable,omitempty"`
	ManageRoutine            bool            `yaml:"manage_routine,omitempty"`
	ManageStorageParam       bool            `yaml:"manage_storage_param,omitempty"`
	SkipPartitionChild       bool            `yaml:"skip_partition_child,omitempty"`
}

// TestDiff runs the fixture suite. Diff reads no database, so unlike the plan
// and apply suites it needs no server; both sides come from SQL files.
func TestDiff(t *testing.T) {
	files, err := filepath.Glob("testdata/diff/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")

		t.Run(name, func(t *testing.T) {
			tc := loadYAML[diffTestCase](t, file)

			tmpDir := t.TempDir()
			currentFile := filepath.Join(tmpDir, "current.sql")
			require.NoError(t, os.WriteFile(currentFile, []byte(tc.Current), 0o644))
			desiredFile := filepath.Join(tmpDir, "desired.sql")
			require.NoError(t, os.WriteFile(desiredFile, []byte(tc.Desired), 0o644))

			client := NewClient(&Options{Schemas: []string{"public"}})

			dropPolicy := DropPolicy{AllowDrop: []string{"all"}}
			if tc.DropPolicy != nil {
				dropPolicy = DropPolicy{AllowDrop: tc.DropPolicy.AllowDrop}
			}
			got, err := client.Diff(&DiffOptions{
				DropPolicy:               dropPolicy,
				Include:                  tc.Include,
				Exclude:                  tc.Exclude,
				Enable:                   tc.Enable,
				Disable:                  tc.Disable,
				ManageRoutine:            tc.ManageRoutine,
				ManageStorageParam:       tc.ManageStorageParam,
				SkipPartitionChild:       tc.SkipPartitionChild,
				Current:                  currentFile,
				Desired:                  desiredFile,
				DisableIndexConcurrently: tc.DisableIndexConcurrently,
				ForceIndexConcurrently:   tc.ForceIndexConcurrently,
				BulkAlter:                tc.BulkAlter,
				AssumeValidated:          tc.AssumeValidated,
			})
			if tc.Error != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.Error)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.Diff), strings.TrimSpace(got.SQL))
			assert.Equal(t, strings.TrimSpace(tc.DisallowedDrops), strings.TrimSpace(got.DisallowedDrops))
			assert.Equal(t, strings.TrimSpace(tc.Ignored), strings.TrimSpace(got.Ignored))
			assert.Equal(t, got.SQL != "", got.HasChanges, "HasChanges must match presence of executable SQL")
			assertExpectedCount(t, tc.Count, got.Count)
		})
	}
}

func TestDiff_MissingCurrentFile(t *testing.T) {
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE t (id int);"), 0o644))

	client := NewClient(&Options{Schemas: []string{"public"}})
	_, err := client.Diff(&DiffOptions{Current: "/nonexistent/current.sql", Desired: desiredFile})
	require.Error(t, err)
}

func TestDiff_MissingDesiredFile(t *testing.T) {
	currentFile := filepath.Join(t.TempDir(), "current.sql")
	require.NoError(t, os.WriteFile(currentFile, []byte("CREATE TABLE t (id int);"), 0o644))

	client := NewClient(&Options{Schemas: []string{"public"}})
	_, err := client.Diff(&DiffOptions{Current: currentFile, Desired: "/nonexistent/desired.sql"})
	require.Error(t, err)
}

func TestDiff_EmptySchemas(t *testing.T) {
	client := NewClient(&Options{Schemas: []string{}})
	_, err := client.Diff(&DiffOptions{Current: "current.sql", Desired: "desired.sql"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one schema must be specified")
}

// An unqualified name is qualified with the first schema, the same way plan
// and apply read their input, so the two sides meet on one key.
func TestDiff_DefaultSchemaQualification(t *testing.T) {
	tmpDir := t.TempDir()
	currentFile := filepath.Join(tmpDir, "current.sql")
	require.NoError(t, os.WriteFile(currentFile, []byte("CREATE TABLE users (id int);"), 0o644))
	desiredFile := filepath.Join(tmpDir, "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE public.users (id int);"), 0o644))

	client := NewClient(&Options{Schemas: []string{"public"}})
	got, err := client.Diff(&DiffOptions{Current: currentFile, Desired: desiredFile})
	require.NoError(t, err)
	assert.False(t, got.HasChanges)
	assert.Empty(t, got.SQL)
}

// An object outside the target schemas is out of scope on both sides: a
// current-side one is not dropped and a desired-side one is not created,
// matching what plan does when the catalog reads only the target schemas.
func TestDiff_FiltersBothSidesBySchema(t *testing.T) {
	tmpDir := t.TempDir()
	currentFile := filepath.Join(tmpDir, "current.sql")
	require.NoError(t, os.WriteFile(currentFile, []byte("CREATE TABLE other.legacy (id int);"), 0o644))
	desiredFile := filepath.Join(tmpDir, "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE other.upcoming (id int);"), 0o644))

	client := NewClient(&Options{Schemas: []string{"public"}})
	got, err := client.Diff(&DiffOptions{
		AllowDrop: []string{"all"},
		Current:   currentFile,
		Desired:   desiredFile,
	})
	require.NoError(t, err)
	assert.False(t, got.HasChanges)
	assert.Empty(t, got.SQL)
	assert.Empty(t, got.DisallowedDrops)
}
