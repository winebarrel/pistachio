package pistachio

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/testutil"
)

// setupCrossSchemaFK creates public.base, <schema>.base and <schema>.item,
// whose foreign key references ref, and returns the connection string. schema
// is written as SQL, so a name that needs quoting comes quoted.
func setupCrossSchemaFK(t *testing.T, schema, ref string) string {
	t.Helper()
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.base (id integer PRIMARY KEY);`)
	return setupSchemaDB(t, ctx, schema, `
CREATE TABLE `+schema+`.base (id integer PRIMARY KEY);
CREATE TABLE `+schema+`.item (id integer PRIMARY KEY, base_id integer REFERENCES `+ref+` (id));`)
}

// crossSchemaFKDesired is the desired schema setupCrossSchemaFK loads, with
// the foreign key written as ref.
func crossSchemaFKDesired(schema, ref string) string {
	return `
CREATE TABLE public.base (id integer PRIMARY KEY);
CREATE TABLE ` + schema + `.base (id integer PRIMARY KEY);
CREATE TABLE ` + schema + `.item (id integer PRIMARY KEY, base_id integer REFERENCES ` + ref + ` (id));`
}

func planCrossSchemaFK(t *testing.T, opts *Options, desired string) string {
	t.Helper()
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(desired), 0o600))

	got, err := NewClient(opts).Plan(context.Background(), &PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	return got.SQL
}

// The catalog prints a reference bare when the search_path reaches its
// schema, and qualified otherwise. A desired reference written either way
// must match it. A key in app naming public.base explicitly used to drop and
// add itself on every plan, since the bare catalog reference was taken to be
// in app.
func TestPlan_CrossSchemaFK_NoChange(t *testing.T) {
	empty := ""
	appPath := "app"
	tests := []struct {
		name       string
		dbSchema   string
		dbRef      string
		desired    string
		schemas    []string
		searchPath *string
		schemaMap  map[string]string
	}{
		// public.base, which the catalog prints bare.
		{name: "qualified public, public first", dbSchema: "app", dbRef: "public.base", desired: crossSchemaFKDesired("app", "public.base"), schemas: []string{"public", "app"}},
		{name: "qualified public, app first", dbSchema: "app", dbRef: "public.base", desired: crossSchemaFKDesired("app", "public.base"), schemas: []string{"app", "public"}},
		{name: "qualified public, empty search path", dbSchema: "app", dbRef: "public.base", desired: crossSchemaFKDesired("app", "public.base"), schemas: []string{"public", "app"}, searchPath: &empty},
		{name: "qualified public, search path app", dbSchema: "app", dbRef: "public.base", desired: crossSchemaFKDesired("app", "public.base"), schemas: []string{"public", "app"}, searchPath: &appPath},
		{name: "qualified public, schema map", dbSchema: "app_stg", dbRef: "public.base", desired: crossSchemaFKDesired("app", "public.base"), schemas: []string{"public", "app_stg"}, schemaMap: map[string]string{"app_stg": "app"}},
		{name: "bare public, public first", dbSchema: "app", dbRef: "public.base", desired: crossSchemaFKDesired("app", "base"), schemas: []string{"public", "app"}},
		{name: "bare public, app first", dbSchema: "app", dbRef: "public.base", desired: crossSchemaFKDesired("app", "base"), schemas: []string{"app", "public"}},
		// app.base, which the catalog qualifies unless app is on the
		// search_path. A hand-written file often names it bare.
		{name: "bare own schema, public first", dbSchema: "app", dbRef: "app.base", desired: crossSchemaFKDesired("app", "base"), schemas: []string{"public", "app"}},
		{name: "bare own schema, app first", dbSchema: "app", dbRef: "app.base", desired: crossSchemaFKDesired("app", "base"), schemas: []string{"app", "public"}},
		{name: "bare own schema, empty search path", dbSchema: "app", dbRef: "app.base", desired: crossSchemaFKDesired("app", "base"), schemas: []string{"public", "app"}, searchPath: &empty},
		{name: "bare own schema, search path app", dbSchema: "app", dbRef: "app.base", desired: crossSchemaFKDesired("app", "base"), schemas: []string{"public", "app"}, searchPath: &appPath},
		{name: "bare own schema, schema map", dbSchema: "app_stg", dbRef: "app_stg.base", desired: crossSchemaFKDesired("app", "base"), schemas: []string{"public", "app_stg"}, schemaMap: map[string]string{"app_stg": "app"}},
		{name: "bare own schema, quoted", dbSchema: `"App"`, dbRef: `"App".base`, desired: crossSchemaFKDesired(`"App"`, "base"), schemas: []string{"public", "App"}},
		{name: "qualified own schema, search path app", dbSchema: "app", dbRef: "app.base", desired: crossSchemaFKDesired("app", "app.base"), schemas: []string{"public", "app"}, searchPath: &appPath},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connString := setupCrossSchemaFK(t, tt.dbSchema, tt.dbRef)
			sql := planCrossSchemaFK(t, &Options{
				ConnString: connString,
				Schemas:    tt.schemas,
				SearchPath: tt.searchPath,
				SchemaMap:  tt.schemaMap,
			}, tt.desired)
			assert.Empty(t, sql)
		})
	}
}

// A real change to a key whose catalog reference is bare still plans, and
// the ADD names public.base.
func TestPlan_CrossSchemaFK_Changed(t *testing.T) {
	connString := setupCrossSchemaFK(t, "app", "public.base")
	sql := planCrossSchemaFK(t, &Options{
		ConnString: connString,
		Schemas:    []string{"public", "app"},
	}, `
CREATE TABLE public.base (id integer PRIMARY KEY);
CREATE TABLE app.base (id integer PRIMARY KEY);
CREATE TABLE app.item (id integer PRIMARY KEY, base_id integer REFERENCES public.base (id) ON DELETE CASCADE);`)

	assert.Contains(t, sql, "ALTER TABLE app.item DROP CONSTRAINT item_base_id_fkey;")
	assert.Contains(t, sql, "REFERENCES public.base (id) ON DELETE CASCADE")
}

// dump output plans clean under each option that changes how it writes the
// reference.
func TestPlan_CrossSchemaFK_DumpRoundTrip(t *testing.T) {
	empty := ""
	appPath := "app"
	tests := []struct {
		name       string
		schemas    []string
		searchPath *string
		schemaMap  map[string]string
	}{
		{name: "public first", schemas: []string{"public", "app"}},
		{name: "app first", schemas: []string{"app", "public"}},
		{name: "empty search path", schemas: []string{"public", "app"}, searchPath: &empty},
		{name: "search path app", schemas: []string{"public", "app"}, searchPath: &appPath},
		{name: "schema map", schemas: []string{"public", "app"}, schemaMap: map[string]string{"app": "app_prod"}},
	}
	for _, ref := range []string{"public.base", "app.base"} {
		for _, tt := range tests {
			t.Run(ref+", "+tt.name, func(t *testing.T) {
				connString := setupCrossSchemaFK(t, "app", ref)
				opts := &Options{
					ConnString: connString,
					Schemas:    tt.schemas,
					SearchPath: tt.searchPath,
					SchemaMap:  tt.schemaMap,
				}

				dumped, err := NewClient(opts).Dump(context.Background(), &DumpOptions{})
				require.NoError(t, err)
				t.Log(dumped.String())

				assert.Empty(t, planCrossSchemaFK(t, opts, dumped.String()))
			})
		}
	}
}
