package pistachio_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
)

type applyTestCase struct {
	Init                     string           `yaml:"init"`
	Desired                  string           `yaml:"desired"`
	Applied                  string           `yaml:"applied"`
	AppliedPG16              string           `yaml:"applied_pg16,omitempty"`
	AppliedPG17              string           `yaml:"applied_pg17,omitempty"`
	AppliedPG18              string           `yaml:"applied_pg18,omitempty"`
	AppliedSQL               *string          `yaml:"applied_sql,omitempty"`
	MinPG                    int              `yaml:"min_pg,omitempty"`
	Count                    *expectedCount   `yaml:"count,omitempty"`
	DisallowedDrops          string           `yaml:"disallowed_drops,omitempty"`
	DropPolicy               *applyDropPolicy `yaml:"drop_policy,omitempty"`
	DisableIndexConcurrently bool             `yaml:"disable_index_concurrently,omitempty"`
	ForceIndexConcurrently   bool             `yaml:"force_index_concurrently,omitempty"`
	BulkAlter                bool             `yaml:"bulk_alter,omitempty"`
	AssumeValidated          bool             `yaml:"assume_validated,omitempty"`
	Include                  []string         `yaml:"include,omitempty"`
	Exclude                  []string         `yaml:"exclude,omitempty"`
	Enable                   []string         `yaml:"enable,omitempty"`
	Disable                  []string         `yaml:"disable,omitempty"`
	ManageRoutine            bool             `yaml:"manage_routine,omitempty"`
	PreSQL                   string           `yaml:"pre_sql,omitempty"`
	// PreSQLFile holds SQL content; the runner writes it to a temp file and
	// passes the path to ApplyOptions.PreSQLFile.
	PreSQLFile         string `yaml:"pre_sql_file,omitempty"`
	ConcurrentlyPreSQL string `yaml:"concurrently_pre_sql,omitempty"`
	// ConcurrentlyPreSQLFile holds SQL content; the runner writes it to a temp
	// file and passes the path to ApplyOptions.ConcurrentlyPreSQLFile.
	ConcurrentlyPreSQLFile string `yaml:"concurrently_pre_sql_file,omitempty"`
	// VerifyNoDrift, when true, runs Plan against the same desired SQL after
	// Apply and asserts that no further DDL is produced. This catches cases
	// where the apply succeeds but a parser/diff bug would generate a spurious
	// diff on the next run (e.g. CONCURRENTLY not stripped from index defs).
	VerifyNoDrift bool `yaml:"verify_no_drift,omitempty"`
}

type applyDropPolicy struct {
	AllowDrop []string `yaml:"allow_drop"`
}

func (tc *applyTestCase) expectedApplied(major int) string {
	switch major {
	case 16:
		if tc.AppliedPG16 != "" {
			return tc.AppliedPG16
		}
	case 17:
		if tc.AppliedPG17 != "" {
			return tc.AppliedPG17
		}
	case 18:
		if tc.AppliedPG18 != "" {
			return tc.AppliedPG18
		}
	}
	return tc.Applied
}

func TestApply(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)
	pgMajor := testutil.ServerMajorVersion(t, ctx, conn)

	files, err := filepath.Glob("testdata/apply/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")
		t.Run(name, func(t *testing.T) {
			tc := loadYAML[applyTestCase](t, file)
			if tc.MinPG > 0 && pgMajor < tc.MinPG {
				t.Skipf("requires PostgreSQL %d or later", tc.MinPG)
			}
			testutil.SetupDB(t, ctx, conn, tc.Init)

			// Apply
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
			var buf bytes.Buffer
			result, err := client.Apply(ctx, &pistachio.ApplyOptions{
				DropPolicy: dropPolicy,
				FilterOptions: pistachio.FilterOptions{
					Include:       tc.Include,
					Exclude:       tc.Exclude,
					Enable:        tc.Enable,
					Disable:       tc.Disable,
					ManageRoutine: tc.ManageRoutine,
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
			}, &buf)
			require.NoError(t, err)
			if tc.AppliedSQL != nil {
				assert.Equal(t, strings.TrimSpace(*tc.AppliedSQL), strings.TrimSpace(buf.String()))
			}
			assert.Equal(t, strings.TrimSpace(tc.DisallowedDrops), strings.TrimSpace(result.DisallowedDrops))
			assertExpectedCount(t, tc.Count, result.Count)

			// Verify
			got, err := client.Dump(ctx, &pistachio.DumpOptions{
				FilterOptions: pistachio.FilterOptions{ManageRoutine: tc.ManageRoutine},
			})
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.expectedApplied(pgMajor)), strings.TrimSpace(got.String()))

			if tc.VerifyNoDrift {
				plan, err := client.Plan(ctx, &pistachio.PlanOptions{
					DropPolicy: dropPolicy,
					FilterOptions: pistachio.FilterOptions{
						Include:       tc.Include,
						Exclude:       tc.Exclude,
						Enable:        tc.Enable,
						Disable:       tc.Disable,
						ManageRoutine: tc.ManageRoutine,
					},
					Files:                    []string{desiredFile},
					DisableIndexConcurrently: tc.DisableIndexConcurrently,
					ForceIndexConcurrently:   tc.ForceIndexConcurrently,
					AssumeValidated:          tc.AssumeValidated,
					PreSQL:                   tc.PreSQL,
					PreSQLFile:               preSQLFile,
					ConcurrentlyPreSQL:       tc.ConcurrentlyPreSQL,
					ConcurrentlyPreSQLFile:   concurrentlyPreSQLFile,
				})
				require.NoError(t, err)
				assert.Empty(t, plan.SQL, "expected no drift after apply")
			}
		})
	}
}

func TestApply_RenameColumn_NonPublicSchema(t *testing.T) {
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

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.NoError(t, err)

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	expected := `-- myschema.events
CREATE TABLE myschema.events (
    id integer NOT NULL,
    event_time timestamp without time zone NOT NULL,
    CONSTRAINT events_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_events_time ON myschema.events USING btree (event_time);`
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(got.String()))
}

// Regression: a CREATE TABLE in a non-public schema whose column type is an
// unqualified DOMAIN defined in public must come AFTER the CREATE DOMAIN in
// the apply order. Toposort previously did not model PostgreSQL's default
// search_path fallback to public, leaving the two as independent nodes; the
// table could land first and the apply failed with `type "..." does not exist`.
func TestApply_UnqualifiedDomainInPublicFromOtherSchema(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	_, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS myschema CASCADE")
	require.NoError(t, err)
	testutil.SetupDB(t, ctx, conn, "")
	_, err = conn.Exec(ctx, "CREATE SCHEMA myschema")
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Exec(ctx, "DROP SCHEMA IF EXISTS myschema CASCADE")
	})

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE DOMAIN public.short_name AS character varying(50);
CREATE TABLE myschema.department (
    id serial NOT NULL,
    name short_name NOT NULL,
    CONSTRAINT department_pkey PRIMARY KEY (id)
);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public", "myschema"},
	})

	_, err = client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.NoError(t, err)
}

func TestApply_WithTx(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	tmpDir := t.TempDir()
	desiredFile := filepath.Join(tmpDir, "desired.sql")
	preSQLFile := filepath.Join(tmpDir, "pre.sql")

	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))
	require.NoError(t, os.WriteFile(preSQLFile, []byte(`CREATE TABLE public.pre_hook (
    id integer NOT NULL
);
SELECT * FROM public.missing_table;`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:      []string{desiredFile},
		PreSQLFile: preSQLFile,
		WithTx:     true,
	}, &buf)
	require.Error(t, err)

	out := buf.String()
	assert.Contains(t, out, "-- Transaction started")
	assert.Contains(t, out, "-- Transaction rolled back")
	assert.NotContains(t, out, "-- Transaction committed")

	got, dumpErr := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, dumpErr)
	assert.NotContains(t, got.String(), "CREATE TABLE public.pre_hook")
	assert.NotContains(t, got.String(), "CREATE TABLE public.users")
}

func TestApply_WithTx_Success(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	tmpDir := t.TempDir()
	desiredFile := filepath.Join(tmpDir, "desired.sql")
	preSQLFile := filepath.Join(tmpDir, "pre.sql")

	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))
	require.NoError(t, os.WriteFile(preSQLFile, []byte(`SELECT 1;`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:      []string{desiredFile},
		PreSQLFile: preSQLFile,
		WithTx:     true,
	}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "-- Transaction started")
	assert.Contains(t, out, "-- Transaction committed")
	assert.NotContains(t, out, "-- Transaction rolled back")

	got, dumpErr := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, dumpErr)
	assert.Contains(t, got.String(), "CREATE TABLE public.users")
}

func TestApply_WithTx_NoChanges_OmitsTransactionComments(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	initSQL := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`
	testutil.SetupDB(t, ctx, conn, initSQL)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(initSQL), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}, WithTx: true}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.NotContains(t, out, "-- Transaction started")
	assert.NotContains(t, out, "-- Transaction committed")
	assert.NotContains(t, out, "-- Transaction rolled back")
}

func TestApply_NoWithTx_OmitsTransactionComments(t *testing.T) {
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

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.NotContains(t, out, "-- Transaction started")
	assert.NotContains(t, out, "-- Transaction committed")
	assert.NotContains(t, out, "-- Transaction rolled back")
}

func TestApply_ExecuteWithCheck_Executes(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)
	// Function should be executed (check returns true; function doesn't exist yet)
	assert.Contains(t, buf.String(), "CREATE OR REPLACE FUNCTION")

	// Verify function exists
	var exists bool
	err = conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')").Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestApply_ExecuteWithCheck_Skipped(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)
	// Function should be SKIPPED (check returns false; function already exists)
	assert.NotContains(t, buf.String(), "CREATE OR REPLACE FUNCTION")
}

func TestApply_ExecuteWithoutCheck(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:execute
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)
	// No check SQL -> always execute
	assert.Contains(t, buf.String(), "CREATE OR REPLACE FUNCTION")
}

func TestApply_ExecuteWithTx(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:execute SELECT true
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:  []string{desiredFile},
		WithTx: true,
	}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE OR REPLACE FUNCTION")

	// Verify function was created
	var exists bool
	err = conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')").Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestApply_ExecuteOnly_NoSchemaChange(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:execute
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)
	// Execute-only (no schema diff) should still run
	assert.Contains(t, buf.String(), "CREATE OR REPLACE FUNCTION")
}

func TestApply_ExecuteSchemalessFunction(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	// Schemaless function; should resolve to public via search_path
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:execute
CREATE OR REPLACE FUNCTION get_count() RETURNS bigint AS $$
  SELECT count(*) FROM users;
$$ LANGUAGE sql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE OR REPLACE FUNCTION")

	// Verify function was created in public schema
	var schema string
	err = conn.QueryRow(ctx, "SELECT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname = 'get_count'").Scan(&schema)
	require.NoError(t, err)
	assert.Equal(t, "public", schema)
}

func TestApply_ExecuteCheckSQLError(t *testing.T) {
	// User-supplied check SQL that itself errors (e.g., references a missing
	// table) must surface as a "failed to evaluate check SQL" error.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:execute SELECT EXISTS (SELECT 1 FROM public.does_not_exist)
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to evaluate check SQL")
}

func TestApply_ExecuteFirstCheckSQLError(t *testing.T) {
	// An execute-first check runs before the managed DDL, on its own call
	// site, so its error path needs its own test.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`-- pista:execute-first SELECT EXISTS (SELECT 1 FROM public.does_not_exist)
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to evaluate check SQL")

	// The managed DDL must not have run: execute-first failed before it.
	var n int
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_class WHERE relname = 'users'").Scan(&n))
	assert.Equal(t, 0, n)
}

func TestApply_ExecuteFirstCheckSQLFalseSkips(t *testing.T) {
	// A false execute-first check skips the statement while the managed DDL
	// still runs.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`-- pista:execute-first SELECT false
CREATE OR REPLACE FUNCTION public.skipped_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)
	assert.NotContains(t, buf.String(), "skipped_func")
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")

	var n int
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_proc WHERE proname = 'skipped_func'").Scan(&n))
	assert.Equal(t, 0, n)
}

func TestApply_ExecuteFirstRunsBeforeManagedDDL(t *testing.T) {
	// Ordering is observable through the writer: the execute-first statement
	// is emitted before the managed DDL, the plain execute after it.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`-- pista:execute
GRANT SELECT ON public.users TO PUBLIC;
-- pista:execute-first
CREATE OR REPLACE FUNCTION public.before_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)

	// strings.Index returns -1 for an absent substring, which would satisfy
	// Less and hide the very regression this guards. Assert presence first.
	out := buf.String()
	require.Contains(t, out, "before_func")
	require.Contains(t, out, "CREATE TABLE public.users")
	require.Contains(t, out, "GRANT")
	assert.Less(t, strings.Index(out, "before_func"), strings.Index(out, "CREATE TABLE public.users"))
	assert.Less(t, strings.Index(out, "CREATE TABLE public.users"), strings.Index(out, "GRANT"))
}

func TestApply_ExecuteSQLError(t *testing.T) {
	// User-supplied execute SQL that fails at execution time must surface as
	// a "failed to execute SQL" error.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:execute
INSERT INTO public.does_not_exist VALUES (1);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute SQL")
}

func TestApply_ExecError(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	// Desired has a column with a type that references a nonexistent type -> exec error on ALTER TABLE
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    data nonexistent_type,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.Error(t, err)
}

func TestApply_EmptySchemas(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE t (id int);"), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one schema must be specified")
}

func TestApply_InvalidConnString(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: "invalid://connection",
		Schemas:    []string{"public"},
	})

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("CREATE TABLE t (id int);"), 0o644))

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.Error(t, err)
}

func TestApply_InvalidDesiredFile(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{"/nonexistent/file.sql"}}, io.Discard)
	require.Error(t, err)
}

func TestApply_InvalidPreSQLFile(t *testing.T) {
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

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:      []string{desiredFile},
		PreSQLFile: "/nonexistent/pre.sql",
	}, io.Discard)
	require.Error(t, err)
}

func TestApply_ConcurrentlyDirective_WithTx_NoIndexChanges_OK(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	// Schema already has the index; no changes will be generated
	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_id ON public.users USING btree (id);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:concurrently
CREATE INDEX idx_users_id ON public.users USING btree (id);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	// Should succeed: no index DDL is generated, so --with-tx is safe
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:  []string{desiredFile},
		WithTx: true,
	}, io.Discard)
	require.NoError(t, err)
}

func TestApply_ConcurrentlyDirective_WithTx_Error(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:concurrently
CREATE INDEX idx_users_id ON public.users USING btree (id);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:  []string{desiredFile},
		WithTx: true,
	}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CONCURRENTLY")
}

func TestApply_ConcurrentlyPreSQL_ExecError(t *testing.T) {
	// concurrently-pre-SQL that fails at execution must surface as a
	// "failed to execute concurrently-pre-SQL" error.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:              []string{desiredFile},
		ConcurrentlyPreSQL: "SELECT * FROM public.does_not_exist;",
	}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute concurrently-pre-SQL")
}

func TestApply_InvalidConcurrentlyPreSQLFile(t *testing.T) {
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

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:                  []string{desiredFile},
		ConcurrentlyPreSQLFile: "/nonexistent/file.sql",
	}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read concurrently-pre-SQL file")
}

func TestApply_InlineConcurrently_WithTx_Error(t *testing.T) {
	// Inline CREATE INDEX CONCURRENTLY (without -- pista:concurrently directive)
	// must still set HasConcurrentlyIndex so --with-tx is rejected.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX CONCURRENTLY idx_users_id ON public.users USING btree (id);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:  []string{desiredFile},
		WithTx: true,
	}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CONCURRENTLY")
}

func TestApply_DisableIndexConcurrently_WithTx_OK(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:concurrently
CREATE INDEX idx_users_id ON public.users USING btree (id);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	// CONCURRENTLY is suppressed, so --with-tx is safe even though the directive is present.
	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
		WithTx:                   true,
	}, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "CREATE INDEX idx_users_id ON public.users USING btree (id);")
	assert.NotContains(t, got, "CONCURRENTLY")
}

func TestApply_TryTx_Concurrently_SkipsTransaction(t *testing.T) {
	// --try-tx runs without a transaction instead of failing when the plan
	// contains CONCURRENTLY index DDL.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files: []string{desiredFile},
		TryTx: true,
	}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "CREATE INDEX CONCURRENTLY")
	assert.Contains(t, out, "-- Transaction skipped")
	assert.NotContains(t, out, "-- Transaction started")
	assert.NotContains(t, out, "-- Transaction committed")

	got, dumpErr := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, dumpErr)
	assert.Contains(t, got.String(), "CREATE INDEX idx_users_name")
}

func TestApply_TryTx_NoConcurrently_UsesTransaction(t *testing.T) {
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

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files: []string{desiredFile},
		TryTx: true,
	}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "-- Transaction started")
	assert.Contains(t, out, "-- Transaction committed")
	assert.NotContains(t, out, "-- Transaction skipped")

	got, dumpErr := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, dumpErr)
	assert.Contains(t, got.String(), "CREATE TABLE public.users")
}

func TestApply_TryTx_NoConcurrently_RollsBackOnError(t *testing.T) {
	// The transaction --try-tx opens is a real one: a failure rolls the
	// schema changes back.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	tmpDir := t.TempDir()
	desiredFile := filepath.Join(tmpDir, "desired.sql")
	preSQLFile := filepath.Join(tmpDir, "pre.sql")

	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))
	require.NoError(t, os.WriteFile(preSQLFile, []byte(`CREATE TABLE public.pre_hook (
    id integer NOT NULL
);
SELECT * FROM public.missing_table;`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:      []string{desiredFile},
		PreSQLFile: preSQLFile,
		TryTx:      true,
	}, &buf)
	require.Error(t, err)

	out := buf.String()
	assert.Contains(t, out, "-- Transaction started")
	assert.Contains(t, out, "-- Transaction rolled back")

	got, dumpErr := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, dumpErr)
	assert.NotContains(t, got.String(), "CREATE TABLE public.pre_hook")
	assert.NotContains(t, got.String(), "CREATE TABLE public.users")
}

func TestApply_TryTx_NoChanges_OmitsTransactionComments(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	initSQL := `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_name ON public.users USING btree (name);`
	testutil.SetupDB(t, ctx, conn, initSQL)

	// The directive is present but the index already matches, so no index DDL
	// is generated and no transaction is opened either way.
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}, TryTx: true}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.NotContains(t, out, "-- Transaction started")
	assert.NotContains(t, out, "-- Transaction committed")
	assert.NotContains(t, out, "-- Transaction rolled back")
	assert.NotContains(t, out, "-- Transaction skipped")
}

func TestApply_TryTx_DirectiveIndexUnchanged_UsesTransaction(t *testing.T) {
	// The decision follows the generated diff, not the presence of the
	// directive: an opted-in index that is not being changed leaves the diff
	// free of CONCURRENTLY DDL, so the other changes run in a transaction.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_name ON public.users USING btree (name);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    age integer,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files: []string{desiredFile},
		TryTx: true,
	}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "ADD COLUMN age")
	assert.Contains(t, out, "-- Transaction started")
	assert.Contains(t, out, "-- Transaction committed")
	assert.NotContains(t, out, "-- Transaction skipped")
}

func TestApply_TryTx_ConcurrentlyPreSQL_RunsOutsideTransaction(t *testing.T) {
	// concurrently-pre-SQL keeps running outside a transaction under --try-tx.
	// The apply succeeding is the proof: CONCURRENTLY index DDL would fail if
	// a transaction had been opened.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:              []string{desiredFile},
		ConcurrentlyPreSQL: "SET lock_timeout = '5s';",
		TryTx:              true,
	}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "SET lock_timeout = '5s';")
	assert.Contains(t, out, "CREATE INDEX CONCURRENTLY")
	assert.Contains(t, out, "-- Transaction skipped")
	assert.NotContains(t, out, "-- Transaction started")
}

func TestApply_TryTx_MatviewIndex_SkipsTransaction(t *testing.T) {
	// A materialized view index carries CONCURRENTLY through the view diff, so
	// --try-tx skips the transaction for it too.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE MATERIALIZED VIEW public.mv AS SELECT 1 AS n;
-- pista:concurrently
CREATE INDEX idx_mv_n ON public.mv USING btree (n);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files: []string{desiredFile},
		TryTx: true,
	}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "CREATE INDEX CONCURRENTLY")
	assert.Contains(t, out, "-- Transaction skipped")
	assert.NotContains(t, out, "-- Transaction started")
}

func TestApply_TryTx_ForceIndexConcurrently_SkipsTransaction(t *testing.T) {
	// --force-index-concurrently is allowed with --try-tx: the forced
	// CONCURRENTLY index DDL simply means no transaction is opened.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:                  []string{desiredFile},
		ForceIndexConcurrently: true,
		TryTx:                  true,
	}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "CREATE INDEX CONCURRENTLY")
	assert.Contains(t, out, "-- Transaction skipped")
	assert.NotContains(t, out, "-- Transaction started")
}

func TestApply_TryTx_DisableIndexConcurrently_UsesTransaction(t *testing.T) {
	// CONCURRENTLY is suppressed, so --try-tx opens a transaction even though
	// the directive is present.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
		TryTx:                    true,
	}, &buf)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "-- Transaction started")
	assert.Contains(t, out, "-- Transaction committed")
	assert.NotContains(t, out, "-- Transaction skipped")
	assert.NotContains(t, out, "CONCURRENTLY")
}

func TestApplyOptions_TryTx_XorEnforcement(t *testing.T) {
	t.Run("try_tx_and_with_tx", func(t *testing.T) {
		var opts pistachio.ApplyOptions
		parser, err := kong.New(&opts)
		require.NoError(t, err)
		_, err = parser.Parse([]string{"--with-tx", "--try-tx", "schema.sql"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--try-tx")
	})

	t.Run("try_tx_and_force_index_concurrently", func(t *testing.T) {
		var opts pistachio.ApplyOptions
		parser, err := kong.New(&opts)
		require.NoError(t, err)
		_, err = parser.Parse([]string{"--force-index-concurrently", "--try-tx", "schema.sql"})
		require.NoError(t, err)
		assert.True(t, opts.TryTx)
		assert.True(t, opts.ForceIndexConcurrently)
	})
}

func TestApply_ConcurrentlyDirective_MatviewIndex_WithTx_Error(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE MATERIALIZED VIEW public.mv AS SELECT 1 AS n;
-- pista:concurrently
CREATE INDEX idx_mv_n ON public.mv USING btree (n);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:  []string{desiredFile},
		WithTx: true,
	}, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CONCURRENTLY")
}

func TestApplyOptions_ForceIndexConcurrently_XorEnforcement(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"force_and_disable", []string{"--force-index-concurrently", "--disable-index-concurrently", "schema.sql"}},
		{"force_and_with_tx", []string{"--force-index-concurrently", "--with-tx", "schema.sql"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var opts pistachio.ApplyOptions
			parser, err := kong.New(&opts)
			require.NoError(t, err)
			_, err = parser.Parse(tc.args)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "--force-index-concurrently")
		})
	}
}

func TestPlanOptions_ForceIndexConcurrently_XorEnforcement(t *testing.T) {
	var opts pistachio.PlanOptions
	parser, err := kong.New(&opts)
	require.NoError(t, err)
	_, err = parser.Parse([]string{"--force-index-concurrently", "--disable-index-concurrently", "schema.sql"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--force-index-concurrently")
}
