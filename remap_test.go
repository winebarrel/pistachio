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

func TestAfterApply(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		o := &pistachio.Options{SchemaMap: map[string]string{"staging": "public"}}
		assert.NoError(t, o.AfterApply())
	})

	t.Run("duplicate destinations", func(t *testing.T) {
		o := &pistachio.Options{SchemaMap: map[string]string{"a": "public", "b": "public"}}
		err := o.AfterApply()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate schema-map destination")
	})
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	t.Log(got)

	// Should detect the diff and produce ALTER TABLE with the real DB schema
	assert.Contains(t, got.SQL, "ALTER TABLE myschema.users ADD COLUMN name text;")
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)

	// No diff expected since schemas are remapped
	assert.Empty(t, got.SQL)
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.SQL)
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)

	// "other" schema is not reverse-mapped, so it won't match myschema
	// This results in creating the "other" table and dropping "myschema" table
	assert.Contains(t, got.SQL, "DROP TABLE myschema.users;")
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	t.Log(got)

	assert.Contains(t, got.SQL, "ALTER TABLE myschema.posts ADD COLUMN title text;")
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	t.Log(got)

	assert.Contains(t, got.SQL, "users_name_idx")
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

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	t.Log(got)

	assert.Contains(t, got.SQL, "CREATE OR REPLACE VIEW myschema.active_users")
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

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}}, io.Discard)
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

func TestPlan_SchemalessDesired_CustomSchema(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	// Desired SQL without schema - should use -n schema ("myschema")
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	t.Log(got)

	assert.Contains(t, got.SQL, "ALTER TABLE myschema.users ADD COLUMN name text;")
}

func TestPlan_SchemalessDesired_CustomSchema_NoDiff(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.SQL)
}

func TestDump_WithSchemaMap_Domain(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE DOMAIN myschema.pos_int AS integer CONSTRAINT pos_check CHECK (VALUE > 0);
`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	assert.Contains(t, output, "CREATE DOMAIN public.pos_int")
	assert.NotContains(t, output, "myschema.pos_int")
}

func TestPlan_WithSchemaMap_Domain(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE DOMAIN myschema.pos_int AS integer;
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE DOMAIN public.pos_int AS integer NOT NULL;`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "ALTER DOMAIN myschema.pos_int SET NOT NULL;")
}

func TestDump_WithSchemaMap_Enum(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TYPE myschema.status AS ENUM ('active', 'inactive');
`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	assert.Contains(t, output, "CREATE TYPE public.status AS ENUM")
	assert.NotContains(t, output, "myschema.status")
}

func TestPlan_WithSchemaMap_Enum(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TYPE myschema.status AS ENUM ('active', 'inactive');
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TYPE public.status AS ENUM ('active', 'inactive', 'pending');`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)

	assert.Contains(t, got.SQL, "ALTER TYPE myschema.status ADD VALUE 'pending' AFTER 'inactive';")
}

func TestPlan_WithSchemaMap_Enum_NoDiff(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TYPE myschema.status AS ENUM ('active', 'inactive');
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TYPE public.status AS ENUM ('active', 'inactive');`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.SQL)
}

func TestApply_WithSchemaMap_Enum(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TYPE myschema.status AS ENUM ('active', 'inactive');
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TYPE public.status AS ENUM ('active', 'inactive', 'pending');`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}}, io.Discard)
	require.NoError(t, err)

	verifyClient := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
	})

	got, err := verifyClient.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	assert.Contains(t, got.String(), "'pending'")
}

func TestApply_SchemalessDesired_CustomSchema(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE users (
    id integer NOT NULL,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
	})

	_, err := client.Apply(ctx, &pistachio.ApplyOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}}, io.Discard)
	require.NoError(t, err)

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	assert.Contains(t, got.String(), "name text")
}

// Verifies that --schema-map remaps the table schema in CREATE POLICY / ALTER
// POLICY / DROP POLICY targets, and that USING / WITH CHECK expressions get
// the same schema substitution applied so cross-schema references stay
// consistent on the desired side.
func TestPlan_WithSchemaMap_Policy_NoDiff(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.documents (
    id bigint NOT NULL,
    owner text NOT NULL,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
ALTER TABLE myschema.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY owner_select ON myschema.documents FOR SELECT USING (owner = current_user);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.documents (
    id bigint NOT NULL,
    owner text NOT NULL,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY owner_select ON public.documents FOR SELECT USING (owner = current_user);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.SQL)
}

// Verifies that the schema replacer rewrites schema-qualified references
// inside USING expressions. Uses a schema-qualified function call so the
// schema prefix survives both pg_get_expr rendering on the catalog side and
// pg_query deparsing on the desired side, isolating the replacer's behaviour
// from unrelated normalization concerns (e.g. subquery column qualification).
func TestPlan_WithSchemaMap_Policy_USING_SchemaRef(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE FUNCTION myschema.is_admin() RETURNS boolean AS $$ SELECT true $$ LANGUAGE SQL IMMUTABLE;
CREATE TABLE myschema.documents (
    id bigint NOT NULL,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
ALTER TABLE myschema.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY visible ON myschema.documents FOR SELECT USING (myschema.is_admin());
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.documents (
    id bigint NOT NULL,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY visible ON public.documents FOR SELECT USING (public.is_admin());`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.SQL, "schema replacement on policy USING should yield no diff")
}

// Verifies that schema replacement also walks WITH CHECK expressions.
// Together with TestPlan_WithSchemaMap_Policy_USING_SchemaRef this ensures
// both clause replacers have coverage.
func TestPlan_WithSchemaMap_Policy_WithCheck_SchemaRef(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE FUNCTION myschema.is_admin() RETURNS boolean AS $$ SELECT true $$ LANGUAGE SQL IMMUTABLE;
CREATE TABLE myschema.documents (
    id bigint NOT NULL,
    owner text NOT NULL,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
ALTER TABLE myschema.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY mod ON myschema.documents FOR ALL USING (myschema.is_admin()) WITH CHECK (myschema.is_admin());
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.documents (
    id bigint NOT NULL,
    owner text NOT NULL,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY mod ON public.documents FOR ALL USING (public.is_admin()) WITH CHECK (public.is_admin());`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.SQL, "schema replacement on policy WITH CHECK should yield no diff")
}

// When the desired schema differs in policy attributes, the diff must target
// the real database schema name (not the desired-side mapped name).
func TestPlan_WithSchemaMap_Policy_AlterUsing(t *testing.T) {
	ctx := context.Background()

	connString := setupSchemaDB(t, ctx, "myschema", `
CREATE TABLE myschema.documents (
    id bigint NOT NULL,
    owner text NOT NULL,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
ALTER TABLE myschema.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY owner_select ON myschema.documents FOR SELECT USING (owner = current_user);
`)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.documents (
    id bigint NOT NULL,
    owner text NOT NULL,
    CONSTRAINT documents_pkey PRIMARY KEY (id)
);
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY owner_select ON public.documents FOR SELECT USING (owner = session_user);`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SchemaMap:  map[string]string{"myschema": "public"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{DropPolicy: pistachio.DropPolicy{AllowDrop: []string{"all"}}, Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Contains(t, got.SQL, "ALTER POLICY owner_select ON myschema.documents")
	assert.NotContains(t, got.SQL, "ALTER POLICY owner_select ON public.documents")
}
