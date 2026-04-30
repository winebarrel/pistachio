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
	Init                     string          `yaml:"init"`
	Desired                  string          `yaml:"desired"`
	Plan                     string          `yaml:"plan"`
	Error                    string          `yaml:"error"`
	DropPolicy               *planDropPolicy `yaml:"drop_policy,omitempty"`
	DisallowedDrops          string          `yaml:"disallowed_drops,omitempty"`
	DisableIndexConcurrently bool            `yaml:"disable_index_concurrently,omitempty"`
	Include                  []string        `yaml:"include,omitempty"`
	Exclude                  []string        `yaml:"exclude,omitempty"`
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
				DropPolicy:               dropPolicy,
				FilterOptions:            pistachio.FilterOptions{Include: tc.Include, Exclude: tc.Exclude},
				Files:                    []string{desiredFile},
				DisableIndexConcurrently: tc.DisableIndexConcurrently,
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
		})
	}
}
