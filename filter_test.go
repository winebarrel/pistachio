package pistachio_test

import (
	"context"
	"io"
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
		o := &pistachio.Options{Include: []string{"user*", "post?"}, Exclude: []string{"tmp_*"}}
		assert.NoError(t, o.ValidatePatterns())
	})

	t.Run("invalid include pattern", func(t *testing.T) {
		o := &pistachio.Options{Include: []string{"[invalid"}}
		err := o.ValidatePatterns()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--include")
	})

	t.Run("invalid exclude pattern", func(t *testing.T) {
		o := &pistachio.Options{Exclude: []string{"[invalid"}}
		err := o.ValidatePatterns()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--exclude")
	})

	t.Run("empty", func(t *testing.T) {
		o := &pistachio.Options{}
		assert.NoError(t, o.ValidatePatterns())
	})
}

func TestAfterApply_InvalidPattern(t *testing.T) {
	o := &pistachio.Options{Include: []string{"[bad"}}
	err := o.AfterApply()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--include")
}

func TestMatchName(t *testing.T) {
	t.Run("no filters", func(t *testing.T) {
		o := &pistachio.Options{}
		assert.True(t, o.MatchName("users"))
	})

	t.Run("include match", func(t *testing.T) {
		o := &pistachio.Options{Include: []string{"users"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("include wildcard", func(t *testing.T) {
		o := &pistachio.Options{Include: []string{"user*"}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("user_roles"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("exclude match", func(t *testing.T) {
		o := &pistachio.Options{Exclude: []string{"posts"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("exclude wildcard", func(t *testing.T) {
		o := &pistachio.Options{Exclude: []string{"tmp_*"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("tmp_backup"))
	})

	t.Run("include and exclude", func(t *testing.T) {
		o := &pistachio.Options{Include: []string{"user*"}, Exclude: []string{"user_tmp"}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("user_roles"))
		assert.False(t, o.MatchName("user_tmp"))
		assert.False(t, o.MatchName("posts"))
	})

	t.Run("multiple include patterns", func(t *testing.T) {
		o := &pistachio.Options{Include: []string{"users", "posts"}}
		assert.True(t, o.MatchName("users"))
		assert.True(t, o.MatchName("posts"))
		assert.False(t, o.MatchName("orders"))
	})

	t.Run("question mark wildcard", func(t *testing.T) {
		o := &pistachio.Options{Include: []string{"user?"}}
		assert.True(t, o.MatchName("users"))
		assert.False(t, o.MatchName("user_roles"))
	})
}

func TestDump_Include(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
		Include:    []string{"users"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	assert.Contains(t, output, "CREATE TABLE public.users")
	assert.NotContains(t, output, "public.posts")
	assert.NotContains(t, output, "active_users")
}

func TestDump_Exclude(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
		Exclude:    []string{"posts"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	assert.Contains(t, output, "CREATE TABLE public.users")
	assert.NotContains(t, output, "public.posts")
}

func TestDump_IncludeWildcard(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.user_roles (
    id integer NOT NULL,
    CONSTRAINT user_roles_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
		Include:    []string{"user*"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	assert.Contains(t, output, "CREATE TABLE public.users")
	assert.Contains(t, output, "CREATE TABLE public.user_roles")
	assert.NotContains(t, output, "public.posts")
}

func TestDump_NoFilters(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	assert.Contains(t, output, "public.users")
	assert.Contains(t, output, "public.posts")
	assert.Contains(t, output, "public.active_users")
}

func TestDump_ExcludeView(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;
CREATE VIEW public.tmp_view AS SELECT 1;`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
		Exclude:    []string{"tmp_*"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	assert.Contains(t, output, "active_users")
	assert.NotContains(t, output, "tmp_view")
}

func TestPlan_Include(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    title text,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
		Include:    []string{"users"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)

	assert.Contains(t, got, "ALTER TABLE public.users ADD COLUMN name text;")
	assert.NotContains(t, got, "posts")
}

func TestPlan_Exclude(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    title text,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
		Exclude:    []string{"posts"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)

	assert.Contains(t, got, "ALTER TABLE public.users ADD COLUMN name text;")
	assert.NotContains(t, got, "posts")
}

func TestApply_Include(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    title text,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
		Include:    []string{"users"},
	})

	err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.NoError(t, err)

	// Verify: only users should have the new column
	verifyClient := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := verifyClient.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	assert.Contains(t, output, "name text")
	assert.NotContains(t, output, "title text")
}
