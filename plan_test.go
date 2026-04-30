package pistachio_test

import (
	"context"
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
}

func TestPlan_WithPreSQLFile(t *testing.T) {
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:      []string{desiredFile},
		PreSQLFile: preSQLFile,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "SELECT 1;")
	assert.Contains(t, got.SQL, "CREATE TABLE public.users")

	// pre-SQL should appear before diff statements
	preSQLPos := strings.Index(got.SQL, "SELECT 1;")
	diffPos := strings.Index(got.SQL, "CREATE TABLE public.users")
	assert.Less(t, preSQLPos, diffPos)
}

func TestPlan_WithPreSQL(t *testing.T) {
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:  []string{desiredFile},
		PreSQL: "SET search_path TO public;",
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "SET search_path TO public;")
	assert.Contains(t, got.SQL, "CREATE TABLE public.users")

	preSQLPos := strings.Index(got.SQL, "SET search_path")
	diffPos := strings.Index(got.SQL, "CREATE TABLE")
	assert.Less(t, preSQLPos, diffPos)
}

func TestPlan_WithConcurrentlyPreSQL(t *testing.T) {
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:              []string{desiredFile},
		ConcurrentlyPreSQL: "SET lock_timeout = '5s';",
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "SET lock_timeout = '5s';")
	assert.Contains(t, got.SQL, "CREATE INDEX CONCURRENTLY idx_users_name")

	setPos := strings.Index(got.SQL, "SET lock_timeout")
	idxPos := strings.Index(got.SQL, "CREATE INDEX CONCURRENTLY")
	assert.Less(t, setPos, idxPos)
}

func TestPlan_PreSQL_And_ConcurrentlyPreSQL_Order(t *testing.T) {
	// When both pre-SQL and concurrently-pre-SQL are set, plan output order
	// must be: pre-SQL → concurrently-pre-SQL → schema DDL.
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:              []string{desiredFile},
		PreSQL:             "SET statement_timeout = '10s';",
		ConcurrentlyPreSQL: "SET lock_timeout = '5s';",
	})
	require.NoError(t, err)
	preSQLPos := strings.Index(got.SQL, "SET statement_timeout")
	concPos := strings.Index(got.SQL, "SET lock_timeout")
	idxPos := strings.Index(got.SQL, "CREATE INDEX CONCURRENTLY")
	require.GreaterOrEqual(t, preSQLPos, 0)
	require.GreaterOrEqual(t, concPos, 0)
	require.GreaterOrEqual(t, idxPos, 0)
	assert.Less(t, preSQLPos, concPos)
	assert.Less(t, concPos, idxPos)
}

func TestPlan_WithConcurrentlyPreSQLFile(t *testing.T) {
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:                  []string{desiredFile},
		ConcurrentlyPreSQLFile: preSQLFile,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "SET lock_timeout = '5s';")
	assert.Contains(t, got.SQL, "CREATE INDEX CONCURRENTLY idx_users_name")
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

func TestPlan_ConcurrentlyPreSQL_SkippedWhenNoConcurrentlyDDL(t *testing.T) {
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:              []string{desiredFile},
		ConcurrentlyPreSQL: "SET lock_timeout = '5s';",
	})
	require.NoError(t, err)
	assert.NotContains(t, got.SQL, "SET lock_timeout")
	assert.Contains(t, got.SQL, "CREATE INDEX idx_users_name")
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

func TestPlan_WithPreSQLFile_NoDiff(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:      []string{desiredFile},
		PreSQLFile: preSQLFile,
	})
	require.NoError(t, err)
	assert.Empty(t, got.SQL)
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
    -- pist:renamed-from occurred_at
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

	files, err := filepath.Glob("testdata/plan/*.yml")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".yml")

		t.Run(name, func(t *testing.T) {
			tc := loadYAML[planTestCase](t, file)
			testutil.SetupDB(t, ctx, conn, tc.Init)

			desiredFile := filepath.Join(t.TempDir(), "desired.sql")
			require.NoError(t, os.WriteFile(desiredFile, []byte(tc.Desired), 0o644))
			client := pistachio.NewClient(&pistachio.Options{
				ConnString: conn.Config().ConnString(),
				Schemas:    []string{"public"},
			})

			got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
			if tc.Error != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.Error)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.Plan), strings.TrimSpace(got.SQL))
		})
	}
}

func TestPlan_DisallowedDrops_MultipleTypes(t *testing.T) {
	// PlanResult.DisallowedDrops aggregates skipped DROPs across object types
	// (table, view, matview, column, enum, domain) when --allow-drop is empty.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    legacy text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.user_v AS SELECT id FROM public.users;
CREATE MATERIALIZED VIEW public.user_mv AS SELECT id FROM public.users;
CREATE TYPE public.color AS ENUM ('red', 'blue');
CREATE DOMAIN public.pos_int AS integer CHECK (VALUE > 0);
CREATE TABLE public.legacy_users (
    id integer NOT NULL,
    CONSTRAINT legacy_users_pkey PRIMARY KEY (id)
);`)

	// Desired removes everything except public.users (and even the legacy
	// column on public.users).
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files: []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Empty(t, got.SQL, "no executable DDL should be generated when all drops are denied")
	dd := got.DisallowedDrops
	assert.Contains(t, dd, "-- skipped: DROP VIEW public.user_v;")
	assert.Contains(t, dd, "-- skipped: DROP MATERIALIZED VIEW public.user_mv;")
	assert.Contains(t, dd, "-- skipped: DROP TABLE public.legacy_users;")
	assert.Contains(t, dd, "-- skipped: ALTER TABLE public.users DROP COLUMN legacy;")
	assert.Contains(t, dd, "-- skipped: DROP TYPE public.color;")
	assert.Contains(t, dd, "-- skipped: DROP DOMAIN public.pos_int;")
}

func TestPlan_DisallowedDrops_PartiallyAllowed(t *testing.T) {
	// When only --allow-drop=table is set, table DROPs execute while other
	// type DROPs are surfaced as skipped comments.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    legacy text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.legacy_users (
    id integer NOT NULL,
    CONSTRAINT legacy_users_pkey PRIMARY KEY (id)
);
CREATE TYPE public.color AS ENUM ('red', 'blue');`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    legacy text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"table"}},
		Files:      []string{desiredFile},
	})
	require.NoError(t, err)
	// Table drop is executable.
	assert.Contains(t, got.SQL, "DROP TABLE public.legacy_users;")
	// Enum drop is skipped.
	assert.Contains(t, got.DisallowedDrops, "-- skipped: DROP TYPE public.color;")
	assert.NotContains(t, got.SQL, "DROP TYPE")
}

func TestPlan_DisallowedDrops_Constraint(t *testing.T) {
	// Pure removals of constraints honor --allow-drop=constraint.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_email_key UNIQUE (email)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.SQL)
	assert.Contains(t, got.DisallowedDrops, "-- skipped: ALTER TABLE public.users DROP CONSTRAINT users_email_key;")

	got, err = client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"constraint"}},
		Files:      []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "ALTER TABLE public.users DROP CONSTRAINT users_email_key;")
	assert.Empty(t, got.DisallowedDrops)
}

func TestPlan_DisallowedDrops_Index(t *testing.T) {
	// Pure removals of indexes honor --allow-drop=index.
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
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.SQL)
	assert.Contains(t, got.DisallowedDrops, "-- skipped: DROP INDEX public.idx_users_name;")

	got, err = client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"index"}},
		Files:      []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "DROP INDEX public.idx_users_name;")
	assert.Empty(t, got.DisallowedDrops)
}

func TestPlan_DisallowedDrops_IndexChange_AlwaysExecutes(t *testing.T) {
	// Definition changes still produce DROP+CREATE because PostgreSQL has no
	// ALTER INDEX for definitions, even when --allow-drop is empty.
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
CREATE INDEX idx_users_name ON public.users USING hash (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "DROP INDEX public.idx_users_name;")
	assert.Contains(t, got.SQL, "CREATE INDEX idx_users_name")
	assert.Empty(t, got.DisallowedDrops)
}

func TestPlan_DisallowedDrops_ConstraintAllowed_IndexDenied(t *testing.T) {
	// --allow-drop=constraint allows constraint drops but does NOT allow index drops.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    email text,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_email_key UNIQUE (email)
);
CREATE INDEX idx_users_name ON public.users USING btree (name);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    email text,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"constraint"}},
		Files:      []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "ALTER TABLE public.users DROP CONSTRAINT users_email_key;")
	assert.NotContains(t, got.SQL, "DROP INDEX")
	assert.Contains(t, got.DisallowedDrops, "-- skipped: DROP INDEX public.idx_users_name;")
}

func TestPlan_DisallowedDrops_FK_byForeignKeyPolicy(t *testing.T) {
	// FK pure removals follow --allow-drop=foreign_key, NOT --allow-drop=constraint.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id),
    CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	// Default (no --allow-drop): FK drop is suppressed.
	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.SQL)
	assert.Contains(t, got.DisallowedDrops, "-- skipped: ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey;")

	// --allow-drop=constraint alone does NOT allow FK drop.
	got, err = client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"constraint"}},
		Files:      []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Empty(t, got.SQL)
	assert.Contains(t, got.DisallowedDrops, "-- skipped: ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey;")

	// --allow-drop=foreign_key allows FK drop.
	got, err = client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"foreign_key"}},
		Files:      []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey;")
	assert.Empty(t, got.DisallowedDrops)
}

func TestPlan_DisallowedDrops_ConstraintAndForeignKey_Orthogonal(t *testing.T) {
	// constraint and foreign_key are independent --allow-drop keys.
	// constraint covers CHECK/UNIQUE/PK/EXCLUSION; FKs require foreign_key.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    qty integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id),
    CONSTRAINT orders_qty_check CHECK (qty > 0),
    CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id)
);`)

	// Desired removes both the CHECK and the FK.
	desired := `CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    qty integer NOT NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);`
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(desired), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	// constraint only → CHECK drops, FK suppressed.
	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"constraint"}},
		Files:      []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "DROP CONSTRAINT orders_qty_check;")
	assert.NotContains(t, got.SQL, "orders_user_id_fkey")
	assert.Contains(t, got.DisallowedDrops, "-- skipped: ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey;")

	// foreign_key only → FK drops, CHECK suppressed.
	got, err = client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"foreign_key"}},
		Files:      []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "DROP CONSTRAINT orders_user_id_fkey;")
	assert.NotContains(t, got.SQL, "orders_qty_check")
	assert.Contains(t, got.DisallowedDrops, "-- skipped: ALTER TABLE public.orders DROP CONSTRAINT orders_qty_check;")

	// Both → both drop.
	got, err = client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"constraint", "foreign_key"}},
		Files:      []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "DROP CONSTRAINT orders_qty_check;")
	assert.Contains(t, got.SQL, "DROP CONSTRAINT orders_user_id_fkey;")
	assert.Empty(t, got.DisallowedDrops)
}

func TestPlan_ConcurrentlyDirective(t *testing.T) {
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
CREATE INDEX idx_users_name ON public.users USING btree (name);
CREATE INDEX idx_users_id ON public.users USING btree (id);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files: []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "CREATE INDEX CONCURRENTLY idx_users_name")
	assert.Contains(t, got.SQL, "CREATE INDEX idx_users_id")
	assert.NotContains(t, got.SQL, "CREATE INDEX CONCURRENTLY idx_users_id")
}

func TestPlan_DisableIndexConcurrently_AddIndex(t *testing.T) {
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "CREATE INDEX idx_users_name")
	assert.NotContains(t, got.SQL, "CONCURRENTLY")
}

func TestPlan_DisableIndexConcurrently_ChangeIndex(t *testing.T) {
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy:               pistachio.DropPolicy{AllowDrop: []string{"all"}},
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "DROP INDEX public.idx_users_name;")
	assert.Contains(t, got.SQL, "CREATE INDEX idx_users_name")
	assert.NotContains(t, got.SQL, "CONCURRENTLY")
}

func TestPlan_DisableIndexConcurrently_NewTable(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, "")

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);
-- pist:concurrently
CREATE UNIQUE INDEX idx_users_id ON public.users USING btree (id);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "CREATE TABLE public.users")
	assert.Contains(t, got.SQL, "CREATE INDEX idx_users_name")
	assert.Contains(t, got.SQL, "CREATE UNIQUE INDEX idx_users_id")
	assert.NotContains(t, got.SQL, "CONCURRENTLY")
}

func TestPlan_DisableIndexConcurrently_ExistingMatview_AddIndex(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.user_names AS
SELECT id, name FROM public.users;`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.user_names AS
SELECT id, name FROM public.users;
-- pist:concurrently
CREATE INDEX idx_user_names_name ON public.user_names USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "CREATE INDEX idx_user_names_name")
	assert.NotContains(t, got.SQL, "CREATE MATERIALIZED VIEW")
	assert.NotContains(t, got.SQL, "CONCURRENTLY")
}

func TestPlan_DisableIndexConcurrently_ExistingMatview_ChangeIndex(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.user_names AS
SELECT id, name FROM public.users;
CREATE INDEX idx_user_names_name ON public.user_names USING btree (name);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.user_names AS
SELECT id, name FROM public.users;
-- pist:concurrently
CREATE INDEX idx_user_names_name ON public.user_names USING hash (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy:               pistachio.DropPolicy{AllowDrop: []string{"all"}},
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "DROP INDEX public.idx_user_names_name;")
	assert.Contains(t, got.SQL, "CREATE INDEX idx_user_names_name")
	assert.NotContains(t, got.SQL, "CREATE MATERIALIZED VIEW")
	assert.NotContains(t, got.SQL, "CONCURRENTLY")
}

func TestPlan_DisableIndexConcurrently_RecreatedMatview(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	// Matview definition change forces DROP MATERIALIZED VIEW + CREATE,
	// which exercises the second matview index loop in DiffViews.
	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.user_names AS
SELECT id, name FROM public.users;
CREATE INDEX idx_user_names_name ON public.user_names USING btree (name);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE MATERIALIZED VIEW public.user_names AS
SELECT id, name, name AS display_name FROM public.users;
-- pist:concurrently
CREATE INDEX idx_user_names_name ON public.user_names USING btree (name);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy:               pistachio.DropPolicy{AllowDrop: []string{"all"}},
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "DROP MATERIALIZED VIEW public.user_names;")
	assert.Contains(t, got.SQL, "CREATE MATERIALIZED VIEW public.user_names")
	assert.Contains(t, got.SQL, "CREATE INDEX idx_user_names_name")
	assert.NotContains(t, got.SQL, "CONCURRENTLY")
}

func TestPlan_DisableIndexConcurrently_NoOpWithoutDirectives(t *testing.T) {
	// Regression: --disable-index-concurrently is a no-op when no index has
	// the directive, and must not mangle plain CREATE INDEX output.
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "CREATE INDEX idx_users_name")
	assert.NotContains(t, got.SQL, "CONCURRENTLY")
}

func TestPlan_DisableIndexConcurrently_MixedDirectives(t *testing.T) {
	// Some indexes have the directive, some don't. With the flag, both should
	// produce plain CREATE INDEX.
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);
CREATE INDEX idx_users_email ON public.users USING btree (email);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "CREATE INDEX idx_users_name")
	assert.Contains(t, got.SQL, "CREATE INDEX idx_users_email")
	assert.NotContains(t, got.SQL, "CONCURRENTLY")
}

func TestPlan_DisableIndexConcurrently_MatviewIndex(t *testing.T) {
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
-- pist:concurrently
CREATE INDEX idx_user_names ON public.user_names USING btree (name);

-- A regular view (parsed with an empty Indexes map) coexists with the matview
-- to exercise the regular-view path in clearConcurrentlyDirectives.
CREATE VIEW public.user_view AS SELECT id FROM public.users;`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		Files:                    []string{desiredFile},
		DisableIndexConcurrently: true,
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "CREATE INDEX idx_user_names")
	assert.Contains(t, got.SQL, "VIEW public.user_view")
	assert.NotContains(t, got.SQL, "CONCURRENTLY")
}
