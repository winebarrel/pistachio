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

func TestValidatePatterns(t *testing.T) {
	t.Run("valid patterns", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"user*", "post?"}, Exclude: []string{"tmp_*"}}
		assert.NoError(t, o.ValidatePatterns())
	})

	t.Run("invalid include pattern", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"[invalid"}}
		err := o.ValidatePatterns()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--include")
	})

	t.Run("invalid exclude pattern", func(t *testing.T) {
		o := &pistachio.FilterOptions{Exclude: []string{"[invalid"}}
		err := o.ValidatePatterns()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--exclude")
	})

	t.Run("empty", func(t *testing.T) {
		o := &pistachio.FilterOptions{}
		assert.NoError(t, o.ValidatePatterns())
	})
}

func TestFilterOptions_AfterApply_Valid(t *testing.T) {
	o := &pistachio.FilterOptions{Include: []string{"user*"}}
	assert.NoError(t, o.AfterApply())
}

func TestFilterOptions_AfterApply_Invalid(t *testing.T) {
	o := &pistachio.FilterOptions{Include: []string{"[bad"}}
	err := o.AfterApply()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--include")
}

func TestMatchName(t *testing.T) {
	t.Run("no filters", func(t *testing.T) {
		o := &pistachio.FilterOptions{}
		assert.True(t, o.MatchName("users"))
	})

	t.Run("include match", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"users"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("include wildcard", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"user*"}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("user_roles"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("exclude match", func(t *testing.T) {
		o := &pistachio.FilterOptions{Exclude: []string{"posts"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("exclude wildcard", func(t *testing.T) {
		o := &pistachio.FilterOptions{Exclude: []string{"tmp_*"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("tmp_backup"))
	})

	t.Run("include and exclude", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"user*"}, Exclude: []string{"user_tmp"}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("user_roles"))
		assert.False(t, o.MatchName("user_tmp"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("multiple include patterns", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"users", "posts"}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("posts"))
		assert.False(t, o.MatchName("orders"))
	})

	t.Run("question mark wildcard", func(t *testing.T) {
		o := &pistachio.FilterOptions{Include: []string{"user?"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("user_roles"))
	})
}

func TestIsTypeEnabled_Disable(t *testing.T) {
	t.Run("disable table", func(t *testing.T) {
		f := &pistachio.FilterOptions{Disable: []string{"table"}}
		assert.False(t, f.IsTypeEnabled("table"))
		assert.True(t, f.IsTypeEnabled("view"))
		assert.True(t, f.IsTypeEnabled("enum"))
		assert.True(t, f.IsTypeEnabled("domain"))
	})

	t.Run("disable multiple", func(t *testing.T) {
		f := &pistachio.FilterOptions{Disable: []string{"table", "view"}}
		assert.False(t, f.IsTypeEnabled("table"))
		assert.False(t, f.IsTypeEnabled("view"))
		assert.True(t, f.IsTypeEnabled("enum"))
		assert.True(t, f.IsTypeEnabled("domain"))
	})

	t.Run("enable takes precedence over disable", func(t *testing.T) {
		f := &pistachio.FilterOptions{Enable: []string{"enum"}, Disable: []string{"table"}}
		assert.True(t, f.IsTypeEnabled("enum"))
		assert.False(t, f.IsTypeEnabled("table"))
		assert.False(t, f.IsTypeEnabled("view"))
	})
}

func TestDump_Disable_Table(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{
		FilterOptions: pistachio.FilterOptions{Disable: []string{"table"}},
	})
	require.NoError(t, err)
	output := got.String()
	assert.Contains(t, output, "CREATE TYPE public.status")
	assert.NotContains(t, output, "CREATE TABLE")
}

func TestPlan_Disable_View_IncludesMatview(t *testing.T) {
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
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.v AS SELECT id FROM public.users;
CREATE MATERIALIZED VIEW public.mv AS SELECT count(*) AS cnt FROM public.users;`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		FilterOptions: pistachio.FilterOptions{Disable: []string{"view"}},
		Files:         []string{desiredFile},
	})
	require.NoError(t, err)
	// --disable view should suppress both regular views AND materialized views
	assert.NotContains(t, got.SQL, "CREATE OR REPLACE VIEW")
	assert.NotContains(t, got.SQL, "CREATE MATERIALIZED VIEW")
	assert.Contains(t, got.SQL, "ADD COLUMN name")
}

func TestPlan_Disable_Table(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TYPE public.status AS ENUM ('active', 'inactive', 'pending');
CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		FilterOptions: pistachio.FilterOptions{Disable: []string{"table"}},
		Files:         []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "ALTER TYPE")
	assert.NotContains(t, got.SQL, "ALTER TABLE")
}

func TestIsTypeEnabled_Enable(t *testing.T) {
	t.Run("empty (all enabled)", func(t *testing.T) {
		f := &pistachio.FilterOptions{}
		assert.True(t, f.IsTypeEnabled("table"))
		assert.True(t, f.IsTypeEnabled("view"))
		assert.True(t, f.IsTypeEnabled("enum"))
		assert.True(t, f.IsTypeEnabled("domain"))
	})

	t.Run("only table", func(t *testing.T) {
		f := &pistachio.FilterOptions{Enable: []string{"table"}}
		assert.True(t, f.IsTypeEnabled("table"))
		assert.False(t, f.IsTypeEnabled("view"))
		assert.False(t, f.IsTypeEnabled("enum"))
		assert.False(t, f.IsTypeEnabled("domain"))
	})

	t.Run("multiple types", func(t *testing.T) {
		f := &pistachio.FilterOptions{Enable: []string{"table", "enum"}}
		assert.True(t, f.IsTypeEnabled("table"))
		assert.False(t, f.IsTypeEnabled("view"))
		assert.True(t, f.IsTypeEnabled("enum"))
		assert.False(t, f.IsTypeEnabled("domain"))
	})
}

func TestDump_Enable_Enum(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{
		FilterOptions: pistachio.FilterOptions{Enable: []string{"enum"}},
	})
	require.NoError(t, err)
	output := got.String()
	assert.Contains(t, output, "CREATE TYPE public.status")
	assert.NotContains(t, output, "CREATE TABLE")
}

func TestDump_Enable_Table(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{
		FilterOptions: pistachio.FilterOptions{Enable: []string{"table"}},
	})
	require.NoError(t, err)
	output := got.String()
	assert.Contains(t, output, "CREATE TABLE public.users")
	assert.NotContains(t, output, "CREATE TYPE")
}

func TestDump_Enable_View(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{
		FilterOptions: pistachio.FilterOptions{Enable: []string{"view"}},
	})
	require.NoError(t, err)
	output := got.String()
	assert.Contains(t, output, "CREATE OR REPLACE VIEW")
	assert.NotContains(t, output, "CREATE TABLE")
}

func TestDump_Enable_Domain(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE DOMAIN public.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{
		FilterOptions: pistachio.FilterOptions{Enable: []string{"domain"}},
	})
	require.NoError(t, err)
	output := got.String()
	assert.Contains(t, output, "CREATE DOMAIN")
	assert.NotContains(t, output, "CREATE TABLE")
}

func TestPlan_Enable_Enum(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TYPE public.status AS ENUM ('active', 'inactive', 'pending');
CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{
		FilterOptions: pistachio.FilterOptions{Enable: []string{"enum"}},
		Files:         []string{desiredFile},
	})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "ALTER TYPE")
	assert.NotContains(t, got.SQL, "ALTER TABLE")
}

func TestApply_Enable_Table(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.status AS ENUM ('active', 'inactive');
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TYPE public.status AS ENUM ('active', 'inactive', 'pending');
CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	var buf bytes.Buffer
	_, err := client.Apply(ctx, &pistachio.ApplyOptions{
		FilterOptions: pistachio.FilterOptions{Enable: []string{"table"}},
		Files:         []string{desiredFile},
	}, &buf)
	require.NoError(t, err)
	output := buf.String()
	assert.Contains(t, output, "ALTER TABLE")
	assert.NotContains(t, output, "ALTER TYPE")
}
