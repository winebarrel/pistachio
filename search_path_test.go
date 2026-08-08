package pistachio_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
	"github.com/winebarrel/pistachio/model"
)

// searchPathDesiredSchema declares one object per catalog function that drops
// the schema from anything the session reaches unqualified: pg_get_viewdef for
// the view, format_type for the enum column, pg_get_expr for the default, and
// pg_get_constraintdef for the CHECK that calls a function.
const searchPathDesiredSchema = `
CREATE TYPE myschema.mood AS ENUM ('ok', 'ng');
CREATE SEQUENCE myschema.counter;
CREATE TABLE myschema.users (
    id integer NOT NULL,
    m myschema.mood,
    n integer DEFAULT nextval('myschema.counter'::regclass),
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_name_check CHECK (myschema.non_empty(name))
);
CREATE VIEW myschema.active_users AS SELECT id FROM myschema.users;
`

// The function the CHECK calls. pistachio does not manage functions, so it
// stays out of the desired schema.
const searchPathInitSchema = `
CREATE FUNCTION myschema.non_empty(v text) RETURNS boolean AS $$ SELECT v <> '' $$ LANGUAGE sql IMMUTABLE;
` + searchPathDesiredSchema

// PGOPTIONS reaches the backend the way "ALTER ROLE ... SET search_path" does,
// so it stands in for one without altering a role the whole test run shares.
const searchPathServerSide = "-c search_path=myschema"

// A dump taken under a server-side search_path used to come out unqualified,
// and reloading it put the objects wherever the next session resolved the bare
// names.
func TestDump_ServerSideSearchPath(t *testing.T) {
	ctx := context.Background()
	connString := setupSchemaDB(t, ctx, "myschema", searchPathInitSchema)

	t.Setenv("PGOPTIONS", searchPathServerSide)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	t.Log(output)

	assert.Contains(t, output, "FROM myschema.users")
	assert.Contains(t, output, "m myschema.mood")
	assert.Contains(t, output, "nextval('myschema.counter'::regclass)")
	assert.Contains(t, output, "CHECK (myschema.non_empty(name))")
}

// The other half: a desired schema names its own objects, so a catalog reading
// them unqualified diffs against it on every plan. The DEFAULT and the CHECK
// are the two that show it, since neither equalDefault nor normalizeCheckExpr
// touches a schema qualification.
func TestPlan_ServerSideSearchPath(t *testing.T) {
	ctx := context.Background()
	connString := setupSchemaDB(t, ctx, "myschema", searchPathInitSchema)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(searchPathDesiredSchema), 0o600))

	t.Setenv("PGOPTIONS", searchPathServerSide)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
	})

	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)

	assert.Empty(t, got.SQL)
}

// The same dump with the path asked for rather than inherited, which is what
// --search-path is for. Only what the catalog spells out loses the schema; the
// object declarations keep it.
func TestDump_SearchPathOption(t *testing.T) {
	ctx := context.Background()
	connString := setupSchemaDB(t, ctx, "myschema", searchPathInitSchema)

	searchPath := "myschema"
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SearchPath: &searchPath,
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	t.Log(output)

	assert.Contains(t, output, "FROM users")
	assert.Contains(t, output, "m mood")
	assert.Contains(t, output, "nextval('counter'::regclass)")
	assert.Contains(t, output, "CHECK (non_empty(name))")
	assert.Contains(t, output, "CREATE TABLE myschema.users")
}

// An empty --search-path reaches nothing, so the catalog qualifies everything,
// including what sits in public and comes out bare under any other path. That
// is the one value whose dump reloads the same way wherever it runs.
func TestDump_SearchPathEmpty(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	testutil.SetupDB(t, ctx, conn, `
CREATE TYPE public.mood AS ENUM ('ok', 'ng');
CREATE TABLE public.users (
    id integer NOT NULL,
    m public.mood,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW public.active_users AS SELECT id FROM public.users;`)

	searchPath := ""
	client := pistachio.NewClient(&pistachio.Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
		SearchPath: &searchPath,
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	output := got.String()
	t.Log(output)

	assert.Contains(t, output, "m public.mood")
	assert.Contains(t, output, "FROM public.users")
}

// A schema named after the connecting role is reached through the "$user"
// entry of the default path, so the catalog reports its objects bare and the
// dump is only reloadable by a role of that name. The default keeps
// PostgreSQL's own value, which is what pre-SQL and anything else on the
// connection expect, so this is pinned as it stands rather than fixed here.
// --search-path=public is the way out, and the second half checks it.
func TestDump_SchemaNamedAfterRole(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	var role string
	require.NoError(t, conn.QueryRow(ctx, "SELECT current_user").Scan(&role))

	connString := setupSchemaDB(t, ctx, model.Ident(role), `
CREATE TABLE `+model.Ident(role)+`.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE VIEW `+model.Ident(role)+`.active_users AS SELECT id FROM `+model.Ident(role)+`.users;`)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{role},
	})

	got, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	assert.Contains(t, got.String(), "FROM users")

	searchPath := "public"
	client = pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{role},
		SearchPath: &searchPath,
	})

	got, err = client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)
	assert.Contains(t, got.String(), "FROM "+model.Ident(role)+".users")
}
