package pistachio_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestPlan_DesiredSchemaFiltered(t *testing.T) {
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
CREATE TABLE other.stuff (
    id integer NOT NULL
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}},
		Files:      []string{desiredFile},
	})
	require.NoError(t, err)
	// other.stuff should be ignored
	assert.Empty(t, got.SQL)
	assert.Equal(t, 1, got.Count.Tables)
}

func TestApply_DesiredSchemaFiltered(t *testing.T) {
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
CREATE TABLE other.stuff (
    id integer NOT NULL
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	count, err := client.Apply(ctx, &pistachio.ApplyOptions{
		DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}},
		Files:      []string{desiredFile},
	}, &buf)
	require.NoError(t, err)
	// No changes (other.stuff ignored)
	assert.Empty(t, buf.String())
	assert.Equal(t, 1, count.Tables)
}

func TestObjectCount_SchemaNames(t *testing.T) {
	c := pistachio.ObjectCount{Schemas: []string{"public"}}
	assert.Equal(t, "public", c.SchemaNames())

	c2 := pistachio.ObjectCount{Schemas: []string{"public", "myschema"}}
	assert.Equal(t, "public, myschema", c2.SchemaNames())
}

func TestObjectCount_Summary(t *testing.T) {
	c := pistachio.ObjectCount{Tables: 3, Views: 1, Enums: 2, Domains: 0}
	assert.Equal(t, "3 tables, 1 view, 2 enums, 0 domains", c.Summary())
}

func TestObjectCount_Summary_Singular(t *testing.T) {
	c := pistachio.ObjectCount{Tables: 1, Views: 1, Enums: 1, Domains: 1}
	assert.Equal(t, "1 table, 1 view, 1 enum, 1 domain", c.Summary())
}
