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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Contains(t, got, "CREATE TABLE public.users")
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

			got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tc.Plan), strings.TrimSpace(got))
		})
	}
}
