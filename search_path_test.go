package pistachio_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
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

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"myschema"},
		SearchPath: "myschema",
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
