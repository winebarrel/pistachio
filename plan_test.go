package pistachio_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
)

type planTestCase struct {
	Init    string `yaml:"init"`
	Desired string `yaml:"desired"`
	Plan    string `yaml:"plan"`
	Error   string `yaml:"error"`
	// MinPG skips the fixture on a server older than this major version, for
	// syntax that does not exist there. PostgreSQL 16 added the SQL/JSON
	// constructors, 17 the query functions.
	MinPG int `yaml:"min_pg,omitempty"`
	// MaxPG skips the fixture on a server newer than this major version, for
	// a shape a later one no longer allows. PostgreSQL 18 rejects an unlogged
	// partitioned table.
	MaxPG                    int             `yaml:"max_pg,omitempty"`
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
	SkipPartitionChild       bool            `yaml:"skip_partition_child,omitempty"`
	PreSQL                   string          `yaml:"pre_sql,omitempty"`
	// PreSQLFile holds SQL content; the runner writes it to a temp file and
	// passes the path to PlanOptions.PreSQLFile.
	PreSQLFile         string `yaml:"pre_sql_file,omitempty"`
	ConcurrentlyPreSQL string `yaml:"concurrently_pre_sql,omitempty"`
	// ConcurrentlyPreSQLFile holds SQL content; the runner writes it to a temp
	// file and passes the path to PlanOptions.ConcurrentlyPreSQLFile.
	ConcurrentlyPreSQLFile string `yaml:"concurrently_pre_sql_file,omitempty"`
}

type planDropPolicy struct {
	AllowDrop []string `yaml:"allow_drop"`
}

// expectedCount holds optional assertions for ObjectCount fields. Each pointer
// is checked only when set, so a fixture can pin individual counts without
// having to specify every field.
type expectedCount struct {
	Tables         *int `yaml:"tables,omitempty"`
	Views          *int `yaml:"views,omitempty"`
	Enums          *int `yaml:"enums,omitempty"`
	Domains        *int `yaml:"domains,omitempty"`
	CompositeTypes *int `yaml:"composite_types,omitempty"`
	Sequences      *int `yaml:"sequences,omitempty"`
	Routines       *int `yaml:"routines,omitempty"`
}

func assertExpectedCount(t *testing.T, want *expectedCount, got pistachio.ObjectCount) {
	t.Helper()
	if want == nil {
		return
	}
	if want.Tables != nil {
		assert.Equal(t, *want.Tables, got.Tables, "Count.Tables")
	}
	if want.Views != nil {
		assert.Equal(t, *want.Views, got.Views, "Count.Views")
	}
	if want.Enums != nil {
		assert.Equal(t, *want.Enums, got.Enums, "Count.Enums")
	}
	if want.Domains != nil {
		assert.Equal(t, *want.Domains, got.Domains, "Count.Domains")
	}
	if want.CompositeTypes != nil {
		assert.Equal(t, *want.CompositeTypes, got.CompositeTypes, "Count.CompositeTypes")
	}
	if want.Sequences != nil {
		assert.Equal(t, *want.Sequences, got.Sequences, "Count.Sequences")
	}
	if want.Routines != nil {
		if assert.NotNil(t, got.Routines, "Count.Routines") {
			assert.Equal(t, *want.Routines, *got.Routines, "Count.Routines")
		}
	}
}

func TestPlan_InvalidConnString(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE t (id int);"), 0o644))

	_, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.Error(t, err)
}

func TestPlan_WithPassword(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Password:   "dummy",
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "CREATE TABLE public.users")
}

func TestPlan_NoReadOnly(t *testing.T) {
	// With NoReadOnly the connection is opened read-write. Plan never writes,
	// so the output is the same; this just exercises the plumbing.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{NoReadOnly: true, Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "CREATE TABLE public.users")
}

func TestPlan_InvalidDesiredFile(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{"/nonexistent/file.sql"}})
	require.Error(t, err)
}

func TestPlan_EmptySchemas(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE t (id int);"), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{},
	})

	_, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one schema must be specified")
}

func TestPlan_BlankSchemaName(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE t (id int);"), 0o644))

	for _, name := range []string{"", "   "} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			client := pistachio.NewClient(&pistachio.Options{
				ConnString: conn.Config().ConnString(),
				Schemas:    []string{name},
			})

			_, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "must not contain empty or whitespace-only entries")
		})
	}
}

func TestPlan_InvalidConcurrentlyPreSQLFile(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:                  []string{desiredFile},
		ConcurrentlyPreSQLFile: "/nonexistent/file.sql",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read concurrently-pre-SQL file")
}

func TestPlan_WithPreSQLFile_InvalidFile(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:      []string{desiredFile},
		PreSQLFile: "/nonexistent/pre.sql",
	})
	require.Error(t, err)
}

// A check plan cannot evaluate leaves the statement in the plan with the
// reason recorded. This is a Go test rather than a fixture because the output
// carries the PostgreSQL error text, which is not stable enough to match
// exactly across server versions.
func TestPlan_ExecuteCheckUnevaluable(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM public.audit_log)
INSERT INTO public.audit_log (id, note) VALUES (1, 'seed');
CREATE TABLE public.audit_log (
    id integer NOT NULL,
    note text,
    CONSTRAINT audit_log_pkey PRIMARY KEY (id)
);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	result, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.True(t, result.HasChanges)
	assert.Contains(t, result.SQL, "CREATE TABLE public.audit_log")
	assert.Contains(t, result.SQL, "INSERT INTO public.audit_log")
	assert.Contains(t, result.SQL, "check SQL could not be evaluated at plan time")
	assert.Contains(t, result.SQL, "apply will decide")
	// The note must stay on one comment line so the plan is still valid SQL.
	for _, line := range strings.Split(result.SQL, "\n") {
		if strings.Contains(line, "could not be evaluated") {
			assert.True(t, strings.HasPrefix(line, "-- "), "note must be a comment: %q", line)
		}
	}
}

func TestPlan_RenameColumn_NonPublicSchema(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.events (
    id integer NOT NULL,
    occurred_at timestamp NOT NULL,
    CONSTRAINT events_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_events_time ON myschema.events (occurred_at);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE myschema.events (
    id integer NOT NULL,
    -- pista:renamed-from occurred_at
    event_time timestamp NOT NULL,
    CONSTRAINT events_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_events_time ON myschema.events (event_time);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Equal(t, "ALTER TABLE myschema.events RENAME COLUMN occurred_at TO event_time;", strings.TrimSpace(got.SQL))
}

func TestPlan(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	pgMajor := testutil.ServerMajorVersion(t, ctx, conn)

	files, err := filepath.Glob("testdata/plan/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")

		t.Run(name, func(t *testing.T) {
			tc := loadYAML[planTestCase](t, file)
			if tc.MinPG > 0 && pgMajor < tc.MinPG {
				t.Skipf("requires PostgreSQL %d or later", tc.MinPG)
			}
			if tc.MaxPG > 0 && pgMajor > tc.MaxPG {
				t.Skipf("requires PostgreSQL %d or earlier", tc.MaxPG)
			}
			testutil.SetupDB(t, ctx, conn, tc.Init)

			tmpDir := t.TempDir()
			desiredFile := filepath.Join(tmpDir, "desired.sql")
			require.NoError(t, os.WriteFile(desiredFile, []byte(tc.Desired), 0o644))
			var preSQLFile, concurrentlyPreSQLFile string
			if tc.PreSQLFile != "" {
				preSQLFile = filepath.Join(tmpDir, "pre.sql")
				require.NoError(t, os.WriteFile(preSQLFile, []byte(tc.PreSQLFile), 0o644))
			}
			if tc.ConcurrentlyPreSQLFile != "" {
				concurrentlyPreSQLFile = filepath.Join(tmpDir, "concurrently-pre.sql")
				require.NoError(t, os.WriteFile(concurrentlyPreSQLFile, []byte(tc.ConcurrentlyPreSQLFile), 0o644))
			}
			client := pistachio.NewClient(&pistachio.Options{
				ConnString: conn.Config().ConnString(),
				Schemas:    []string{"public"},
			})

			dropPolicy := pistachio.DropPolicy{AllowDrop: []string{"all"}}
			if tc.DropPolicy != nil {
				dropPolicy = pistachio.DropPolicy{AllowDrop: tc.DropPolicy.AllowDrop}
			}
			got, err := client.Plan(ctx, &pistachio.PlanOptions{
				DropPolicy: dropPolicy,
				FilterOptions: pistachio.FilterOptions{
					Include:            tc.Include,
					Exclude:            tc.Exclude,
					Enable:             tc.Enable,
					Disable:            tc.Disable,
					ManageRoutine:      tc.ManageRoutine,
					SkipPartitionChild: tc.SkipPartitionChild,
				},
				Files:                    []string{desiredFile},
				DisableIndexConcurrently: tc.DisableIndexConcurrently,
				ForceIndexConcurrently:   tc.ForceIndexConcurrently,
				BulkAlter:                tc.BulkAlter,
				AssumeValidated:          tc.AssumeValidated,
				PreSQL:                   tc.PreSQL,
				PreSQLFile:               preSQLFile,
				ConcurrentlyPreSQL:       tc.ConcurrentlyPreSQL,
				ConcurrentlyPreSQLFile:   concurrentlyPreSQLFile,
			})
			if tc.Error != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.Error)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.Plan), strings.TrimSpace(got.SQL))
			assert.Equal(t, strings.TrimSpace(tc.DisallowedDrops), strings.TrimSpace(got.DisallowedDrops))
			assert.Equal(t, strings.TrimSpace(tc.Ignored), strings.TrimSpace(got.Ignored))
			assert.Equal(t, got.SQL != "", got.HasChanges, "HasChanges must match presence of executable SQL")
			assertExpectedCount(t, tc.Count, got.Count)
		})
	}
}
