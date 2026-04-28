package pistachio_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
)

type applyTestCase struct {
	Init    string `yaml:"init"`
	Desired string `yaml:"desired"`
	Applied string `yaml:"applied"`
}

func TestApply(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	files, err := filepath.Glob("testdata/apply/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")
		t.Run(name, func(t *testing.T) {
			tc := loadYAML[applyTestCase](t, file)
			testutil.SetupDB(t, ctx, conn, tc.Init)

			// Apply
			desiredFile := filepath.Join(t.TempDir(), "desired.sql")
			require.NoError(t, os.WriteFile(desiredFile, []byte(tc.Desired), 0o644))
			client := pistachio.NewClient(&pistachio.Options{
				ConnString: conn.Config().ConnString(),
				Schemas:    []string{"public"},
			})
			_, err = client.Apply(ctx, &pistachio.ApplyOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}}, io.Discard)
			require.NoError(t, err)

			// Verify
			got, err := client.Dump(ctx, &pistachio.DumpOptions{})
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.Applied), strings.TrimSpace(got.String()))
		})
	}
}

func TestApply_WithPreSQL(t *testing.T) {
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
		Files:  []string{desiredFile},
		PreSQL: "SELECT 1;",
	}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "SELECT 1;")
	assert.Contains(t, buf.String(), "CREATE TABLE public.users")
}

func TestApply_WithPreSQLFile(t *testing.T) {
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
    id integer NOT NULL,
    CONSTRAINT pre_hook_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, applyErr := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:      []string{desiredFile},
		PreSQLFile: preSQLFile,
	}, io.Discard)
	require.NoError(t, applyErr)

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	assert.Contains(t, got.String(), "CREATE TABLE public.pre_hook")
	assert.Contains(t, got.String(), "CREATE TABLE public.users")
}

func TestApply_WithPreSQLFile_Output(t *testing.T) {
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
    id integer NOT NULL,
    CONSTRAINT pre_hook_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:      []string{desiredFile},
		PreSQLFile: preSQLFile,
	}, &buf)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "CREATE TABLE public.pre_hook")
	assert.Contains(t, output, "CREATE TABLE public.users")

	// pre-SQL should appear before diff statements
	preSQLPos := strings.Index(output, "CREATE TABLE public.pre_hook")
	diffPos := strings.Index(output, "CREATE TABLE public.users")
	assert.Less(t, preSQLPos, diffPos)
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

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:      []string{desiredFile},
		PreSQLFile: preSQLFile,
		WithTx:     true,
	}, io.Discard)
	require.Error(t, err)

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

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:      []string{desiredFile},
		PreSQLFile: preSQLFile,
		WithTx:     true,
	}, io.Discard)
	require.NoError(t, err)

	got, dumpErr := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, dumpErr)
	assert.Contains(t, got.String(), "CREATE TABLE public.users")
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
-- pist:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)
	// Function should be executed (check returns true — function doesn't exist yet)
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
-- pist:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'test_func')
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)
	// Function should be SKIPPED (check returns false — function already exists)
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
-- pist:execute
CREATE OR REPLACE FUNCTION public.test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)
	// No check SQL → always execute
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
-- pist:execute SELECT true
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
-- pist:execute
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
	// Schemaless function — should resolve to public via search_path
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:execute
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

func TestApply_NoDiff(t *testing.T) {
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

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.NoError(t, err)
}

func TestApply_SchemalessDesired(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.NoError(t, err)

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	assert.Contains(t, got.String(), "name text")
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
-- pist:execute SELECT EXISTS (SELECT 1 FROM public.does_not_exist)
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
-- pist:execute
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

	// Desired has a column with a type that references a nonexistent type → exec error on ALTER TABLE
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

	// Schema already has the index — no changes will be generated
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
-- pist:concurrently
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
-- pist:concurrently
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

func TestApply_ConcurrentlyDirective(t *testing.T) {
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
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files: []string{desiredFile},
	}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE INDEX CONCURRENTLY idx_users_name")
}

func TestApply_WithConcurrentlyPreSQL(t *testing.T) {
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
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:              []string{desiredFile},
		ConcurrentlyPreSQL: "SET lock_timeout = '5s';",
	}, &buf)
	require.NoError(t, err)
	output := buf.String()
	assert.Contains(t, output, "SET lock_timeout = '5s';")
	assert.Contains(t, output, "CREATE INDEX CONCURRENTLY idx_users_name")
	// concurrently-pre-SQL must be emitted before the CONCURRENTLY DDL.
	setPos := strings.Index(output, "SET lock_timeout")
	idxPos := strings.Index(output, "CREATE INDEX CONCURRENTLY")
	assert.Less(t, setPos, idxPos)
}

func TestApply_ConcurrentlyPreSQL_SkippedWhenNoConcurrentlyDDL(t *testing.T) {
	// concurrently-pre-SQL is gated on the diff producing CONCURRENTLY index DDL.
	// A plain CREATE INDEX must not trigger it.
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
		Files:              []string{desiredFile},
		ConcurrentlyPreSQL: "SET lock_timeout = '5s';",
	}, &buf)
	require.NoError(t, err)
	assert.NotContains(t, buf.String(), "SET lock_timeout")
	assert.Contains(t, buf.String(), "CREATE INDEX idx_users_name")
}

func TestApply_WithConcurrentlyPreSQLFile(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	tmpDir := t.TempDir()
	desiredFile := filepath.Join(tmpDir, "desired.sql")
	preSQLFile := filepath.Join(tmpDir, "concurrently-pre.sql")

	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))
	require.NoError(t, os.WriteFile(preSQLFile, []byte(`SET lock_timeout = '5s';`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:                  []string{desiredFile},
		ConcurrentlyPreSQLFile: preSQLFile,
	}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "SET lock_timeout = '5s';")
	assert.Contains(t, buf.String(), "CREATE INDEX CONCURRENTLY idx_users_name")
}

func TestApply_ConcurrentlyPreSQL_SkippedWhenDisableIndexConcurrently(t *testing.T) {
	// --disable-index-concurrently strips CONCURRENTLY from the diff, so
	// HasConcurrentlyIndex becomes false and concurrently-pre-SQL must not run.
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
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:                    []string{desiredFile},
		ConcurrentlyPreSQL:       "SET lock_timeout = '5s';",
		DisableIndexConcurrently: true,
	}, &buf)
	require.NoError(t, err)
	output := buf.String()
	assert.NotContains(t, output, "SET lock_timeout")
	assert.NotContains(t, output, "CONCURRENTLY")
	assert.Contains(t, output, "CREATE INDEX idx_users_name")
}

func TestApply_ConcurrentlyPreSQL_TriggersOnInlineConcurrently(t *testing.T) {
	// Inline CREATE INDEX CONCURRENTLY (no directive) must also trigger
	// concurrently-pre-SQL via HasConcurrentlyIndex.
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
CREATE INDEX CONCURRENTLY idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:              []string{desiredFile},
		ConcurrentlyPreSQL: "SET lock_timeout = '5s';",
	}, &buf)
	require.NoError(t, err)
	output := buf.String()
	assert.Contains(t, output, "SET lock_timeout = '5s';")
	assert.Contains(t, output, "CREATE INDEX CONCURRENTLY idx_users_name")
	setPos := strings.Index(output, "SET lock_timeout")
	idxPos := strings.Index(output, "CREATE INDEX CONCURRENTLY")
	assert.Less(t, setPos, idxPos)
}

func TestApply_ConcurrentlyPreSQL_TriggersOnDropConcurrently(t *testing.T) {
	// DROP INDEX CONCURRENTLY (driven by --disable-index-concurrently being
	// off and the desired removing an existing concurrently-flagged index)
	// must also set HasConcurrentlyIndex.
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
	// Definition change forces DROP+CREATE; both go through CONCURRENTLY because
	// the desired index carries the directive.
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING hash (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		DropPolicy:         pistachio.DropPolicy{AllowDrop: []string{"all"}},
		Files:              []string{desiredFile},
		ConcurrentlyPreSQL: "SET lock_timeout = '5s';",
	}, &buf)
	require.NoError(t, err)
	output := buf.String()
	assert.Contains(t, output, "SET lock_timeout = '5s';")
	assert.Contains(t, output, "DROP INDEX CONCURRENTLY public.idx_users_name;")
	setPos := strings.Index(output, "SET lock_timeout")
	dropPos := strings.Index(output, "DROP INDEX CONCURRENTLY")
	assert.Less(t, setPos, dropPos)
}

func TestApply_PreSQL_And_ConcurrentlyPreSQL_Order(t *testing.T) {
	// When both --pre-sql and --concurrently-pre-sql are set, order must be:
	// pre-SQL → concurrently-pre-SQL → schema DDL.
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
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:              []string{desiredFile},
		PreSQL:             "SET statement_timeout = '10s';",
		ConcurrentlyPreSQL: "SET lock_timeout = '5s';",
	}, &buf)
	require.NoError(t, err)
	output := buf.String()
	preSQLPos := strings.Index(output, "SET statement_timeout")
	concPos := strings.Index(output, "SET lock_timeout")
	idxPos := strings.Index(output, "CREATE INDEX CONCURRENTLY")
	require.GreaterOrEqual(t, preSQLPos, 0)
	require.GreaterOrEqual(t, concPos, 0)
	require.GreaterOrEqual(t, idxPos, 0)
	assert.Less(t, preSQLPos, concPos)
	assert.Less(t, concPos, idxPos)
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
-- pist:concurrently
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
}

func TestApply_DisallowedDrops_MultipleTypes(t *testing.T) {
	// ApplyResult.DisallowedDrops aggregates skipped DROPs across object types
	// when --allow-drop is empty. The buffer must stay empty (no executable DDL).
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    legacy text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TYPE public.color AS ENUM ('red', 'blue');`)

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
	result, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files: []string{desiredFile},
	}, &buf)
	require.NoError(t, err)
	assert.Empty(t, buf.String(), "no executable DDL should be written")
	assert.Contains(t, result.DisallowedDrops, "-- skipped: ALTER TABLE public.users DROP COLUMN legacy;")
	assert.Contains(t, result.DisallowedDrops, "-- skipped: DROP TYPE public.color;")

	// Confirm DB state was not mutated by the comments.
	var n int
	require.NoError(t, conn.QueryRow(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='legacy'").Scan(&n))
	assert.Equal(t, 1, n, "skipped column drop must not actually drop the column")
}

func TestApply_InlineConcurrently_NoDriftAfterApply(t *testing.T) {
	// Regression: after applying inline CREATE INDEX CONCURRENTLY, a subsequent
	// plan against the same SQL must produce no diff. Validates that the parser
	// strips CONCURRENTLY from Index.Definition so it matches the catalog-derived
	// definition (which never contains CONCURRENTLY).
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
CREATE INDEX CONCURRENTLY idx_users_name ON public.users USING btree (name);
CREATE UNIQUE INDEX CONCURRENTLY idx_users_id ON public.users USING btree (id);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files: []string{desiredFile},
	}, io.Discard)
	require.NoError(t, err)

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files: []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Empty(t, got.SQL, "expected no drift after applying inline CONCURRENTLY indexes")
}

func TestApply_InlineConcurrently_ChangeIndex(t *testing.T) {
	// Changing an inline-CONCURRENTLY index emits DROP + CREATE, both of which
	// must use CONCURRENTLY (since the desired carries Concurrently=true).
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
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX CONCURRENTLY idx_users_name ON public.users USING hash (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}},
		Files:      []string{desiredFile},
	}, &buf)
	require.NoError(t, err)
	got := buf.String()
	assert.Contains(t, got, "DROP INDEX CONCURRENTLY public.idx_users_name;")
	assert.Contains(t, got, "CREATE INDEX CONCURRENTLY idx_users_name ON public.users USING hash (name);")
}

func TestApply_InlineConcurrently_MatviewIndex(t *testing.T) {
	// Inline CREATE INDEX CONCURRENTLY on a materialized view index must emit
	// CONCURRENTLY in the apply output and leave no drift.
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
CREATE MATERIALIZED VIEW public.user_names AS SELECT id, name FROM public.users;
CREATE INDEX CONCURRENTLY idx_user_names ON public.user_names USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files: []string{desiredFile},
	}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE INDEX CONCURRENTLY idx_user_names")

	// No drift after apply.
	plan, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files: []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Empty(t, plan.SQL)
}

func TestApply_InlineConcurrently_Preserved(t *testing.T) {
	// Inline CREATE INDEX CONCURRENTLY (without -- pist:concurrently and
	// without --disable-index-concurrently) must round-trip to CONCURRENTLY
	// in the emitted DDL.
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
CREATE INDEX CONCURRENTLY idx_users_name ON public.users USING btree (name);
CREATE UNIQUE INDEX CONCURRENTLY idx_users_id ON public.users USING btree (id);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files: []string{desiredFile},
	}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE INDEX CONCURRENTLY idx_users_name")
	assert.Contains(t, buf.String(), "CREATE UNIQUE INDEX CONCURRENTLY idx_users_id")
}

func TestApply_InlineConcurrently_WithTx_Error(t *testing.T) {
	// Inline CREATE INDEX CONCURRENTLY (without -- pist:concurrently directive)
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

func TestApply_InlineConcurrently_DisableSuppresses(t *testing.T) {
	// --disable-index-concurrently must strip CONCURRENTLY from inline
	// CREATE INDEX CONCURRENTLY in the input SQL, not just the directive.
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
CREATE INDEX CONCURRENTLY idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
		WithTx:                   true,
	}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE INDEX idx_users_name")
	assert.NotContains(t, buf.String(), "CONCURRENTLY")
}

func TestApply_DisableIndexConcurrently(t *testing.T) {
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
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "CREATE INDEX idx_users_name")
	assert.NotContains(t, buf.String(), "CONCURRENTLY")
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
-- pist:concurrently
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

func TestApply_DisableIndexConcurrently_DropIndex(t *testing.T) {
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
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING hash (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		DropPolicy:               pistachio.DropPolicy{AllowDrop: []string{"all"}},
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	}, &buf)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "DROP INDEX public.idx_users_name;")
	assert.NotContains(t, buf.String(), "CONCURRENTLY")
}

func TestApply_ConcurrentlyDirective_MatviewIndex_WithTx_Error(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE MATERIALIZED VIEW public.mv AS SELECT 1 AS n;
-- pist:concurrently
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
