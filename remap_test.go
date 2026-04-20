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

func setupSchemaDB(t *testing.T, ctx context.Context, schema string, initSQL string) string {
	t.Helper()
	conn := testutil.ConnectDB(t)

	_, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)

	if initSQL != "" {
		_, err = conn.Exec(ctx, initSQL)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		_, _ = conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		conn.Close(ctx)
	})

	return conn.Config().ConnString()
}

func TestRemapSchema(t *testing.T) {
	t.Run("nil map", func(t *testing.T) {
		o := &pistachio.Options{}
		assert.Equal(t, "public", o.RemapSchema("public"))
	})

	t.Run("mapped", func(t *testing.T) {
		o := &pistachio.Options{SchemaMap: map[string]string{"myschema": "public"}}
		assert.Equal(t, "public", o.RemapSchema("myschema"))
	})

	t.Run("unmapped", func(t *testing.T) {
		o := &pistachio.Options{SchemaMap: map[string]string{"myschema": "public"}}
		assert.Equal(t, "other", o.RemapSchema("other"))
	})
}

func TestValidateSchemaMap(t *testing.T) {
	t.Run("nil map", func(t *testing.T) {
		o := &pistachio.Options{}
		assert.NoError(t, o.ValidateSchemaMap())
	})

	t.Run("single entry", func(t *testing.T) {
		o := &pistachio.Options{SchemaMap: map[string]string{"staging": "public"}}
		assert.NoError(t, o.ValidateSchemaMap())
	})

	t.Run("distinct destinations", func(t *testing.T) {
		o := &pistachio.Options{SchemaMap: map[string]string{"a": "x", "b": "y"}}
		assert.NoError(t, o.ValidateSchemaMap())
	})

	t.Run("duplicate destinations", func(t *testing.T) {
		o := &pistachio.Options{SchemaMap: map[string]string{"a": "public", "b": "public"}}
		err := o.ValidateSchemaMap()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate schema-map destination")
	})
}

func TestReverseRemapSchema(t *testing.T) {
	t.Run("nil map", func(t *testing.T) {
		o := &pistachio.Options{}
		assert.Equal(t, "public", o.ReverseRemapSchema("public"))
	})

	t.Run("mapped", func(t *testing.T) {
		o := &pistachio.Options{SchemaMap: map[string]string{"myschema": "public"}}
		assert.Equal(t, "myschema", o.ReverseRemapSchema("public"))
	})

	t.Run("unmapped", func(t *testing.T) {
		o := &pistachio.Options{SchemaMap: map[string]string{"myschema": "public"}}
		assert.Equal(t, "other", o.ReverseRemapSchema("other"))
	})
}

func TestDump_WithSchemaMap_QuotedSchema(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, `"My Schema"`, `
CREATE TABLE "My Schema".users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX users_id_idx ON "My Schema".users (id);
`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"My Schema"},
		SchemaMap:  map[string]string{"My Schema": "public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	t.Log(output)

	assert.Contains(t, output, "CREATE TABLE public.users")
	assert.Contains(t, output, "ON public.users")
	assert.NotContains(t, output, `"My Schema"`)
}

func TestDump_WithSchemaMap(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW myschema.active_users AS SELECT id, name FROM myschema.users;
`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	t.Log(output)

	// Schema references should be remapped to "public", including inside view definitions
	assert.Contains(t, output, "CREATE TABLE public.users")
	assert.Contains(t, output, "CREATE OR REPLACE VIEW public.active_users")
	assert.Contains(t, output, "FROM public.users")
}

func TestDump_WithSchemaMap_Files(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	files := got.Files()
	assert.Contains(t, files, "public.users.sql")
	assert.NotContains(t, files, "myschema.users.sql")
}

func TestPlan_WithSchemaMap(t *testing.T) {
	ctx := context.Background()

	// DB has myschema.users with (id), desired has public.users with (id, name)
	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	t.Log(got)

	// Should detect the diff and produce ALTER TABLE with the real DB schema
	assert.Contains(t, got, "ALTER TABLE myschema.users ADD COLUMN name text;")
}

func TestPlan_WithSchemaMap_NoDiff(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)

	// No diff expected since schemas are remapped
	assert.Empty(t, got)
}

func TestDump_WithoutSchemaMap(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	assert.Contains(t, output, "CREATE TABLE myschema.users")
}

func TestPlan_WithoutSchemaMap(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestDump_WithSchemaMap_ForeignKey(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE myschema.posts (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id),
    CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES myschema.users(id)
);
`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	t.Log(output)

	assert.Contains(t, output, "CREATE TABLE public.users")
	assert.Contains(t, output, "CREATE TABLE public.posts")
	assert.Contains(t, output, "ALTER TABLE ONLY public.posts")
}

func TestDump_WithSchemaMap_Index(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX users_name_idx ON myschema.users (name);
`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	t.Log(output)

	assert.Contains(t, output, "CREATE TABLE public.users")
	assert.Contains(t, output, "users_name_idx")
}

func TestDump_WithSchemaMap_UnmappedSchema(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	// Map a different schema, myschema should remain unchanged
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"other": "public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	assert.Contains(t, output, "CREATE TABLE myschema.users")
}

func TestPlan_WithSchemaMap_UnmappedDesiredSchema(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	// Desired uses "other" schema which is not in the reverse map
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE other.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)

	// "other" schema is not reverse-mapped, so it won't match myschema
	// This results in creating the "other" table and dropping "myschema" table
	assert.Contains(t, got, "DROP TABLE myschema.users;")
}

func TestPlan_WithSchemaMap_ForeignKey(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE myschema.posts (
    id integer NOT NULL,
    user_id integer NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id),
    CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES myschema.users(id)
);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    user_id integer NOT NULL,
    title text,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);
ALTER TABLE ONLY public.posts ADD CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	t.Log(got)

	assert.Contains(t, got, "ALTER TABLE myschema.posts ADD COLUMN title text;")
}

func TestPlan_WithSchemaMap_Index(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX users_name_idx ON public.users (name);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	t.Log(got)

	assert.Contains(t, got, "users_name_idx")
}

func TestPlan_WithSchemaMap_View(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	t.Log(got)

	assert.Contains(t, got, "CREATE OR REPLACE VIEW myschema.active_users")
}

func TestApply_WithSchemaMap(t *testing.T) {
	ctx := context.Background()

	// DB has myschema.users with (id), desired has public.users with (id, name)
	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, io.Discard)
	require.NoError(t, err)

	// Verify: dump without schema map should show myschema with new column
	verifyClient := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
	})

	got, err := verifyClient.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	t.Log(output)
	assert.Contains(t, output, "name text")
}
