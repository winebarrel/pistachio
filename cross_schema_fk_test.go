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

// setupCrossSchemaFK creates public.base and <schema>.item, whose foreign key
// references public.base, and returns the connection string.
func setupCrossSchemaFK(t *testing.T, schema string) string {
	t.Helper()
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.base (id integer PRIMARY KEY);`)
	return setupSchemaDB(t, ctx, schema, `
CREATE TABLE `+schema+`.base (id integer PRIMARY KEY);
CREATE TABLE `+schema+`.item (id integer PRIMARY KEY, base_id integer REFERENCES public.base (id));`)
}

func planCrossSchemaFK(t *testing.T, opts *Options, desired string) string {
	t.Helper()
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(desired), 0o600))

	got, err := NewClient(opts).Plan(context.Background(), &PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	return got.SQL
}

// The catalog prints a reference into public bare while public is on the
// search_path, and a key in another schema that names public.base explicitly
// used to drop and add itself on every plan.
func TestPlan_CrossSchemaFK_Qualified(t *testing.T) {
	const desired = `
CREATE TABLE public.base (id integer PRIMARY KEY);
CREATE TABLE app.base (id integer PRIMARY KEY);
CREATE TABLE app.item (id integer PRIMARY KEY, base_id integer REFERENCES public.base (id));`

	empty := ""
	appPath := "app"
	tests := []struct {
		name       string
		schemas    []string
		searchPath *string
	}{
		{"public first", []string{"public", "app"}, nil},
		{"app first", []string{"app", "public"}, nil},
		{"empty search path", []string{"public", "app"}, &empty},
		{"search path app", []string{"public", "app"}, &appPath},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connString := setupCrossSchemaFK(t, "app")
			sql := planCrossSchemaFK(t, &Options{
				ConnString: connString,
				Schemas:    tt.schemas,
				SearchPath: tt.searchPath,
			}, desired)
			assert.Empty(t, sql)
		})
	}
}

// The reference moved to a table of the same name in the owning table's
// schema is a change. Filling the bare catalog reference with the owning
// table's schema used to hide it.
func TestPlan_CrossSchemaFK_MovedToOwnSchema(t *testing.T) {
	connString := setupCrossSchemaFK(t, "app")
	sql := planCrossSchemaFK(t, &Options{
		ConnString: connString,
		Schemas:    []string{"public", "app"},
	}, `
CREATE TABLE public.base (id integer PRIMARY KEY);
CREATE TABLE app.base (id integer PRIMARY KEY);
CREATE TABLE app.item (id integer PRIMARY KEY, base_id integer REFERENCES app.base (id));`)

	assert.Contains(t, sql, "ALTER TABLE app.item DROP CONSTRAINT item_base_id_fkey;")
	assert.Contains(t, sql, "REFERENCES app.base (id)")
}

// dump writes the reference bare, which plans clean whichever schema comes
// first in -n.
func TestPlan_CrossSchemaFK_DumpRoundTrip(t *testing.T) {
	for _, schemas := range [][]string{{"public", "app"}, {"app", "public"}} {
		t.Run(schemas[0]+" first", func(t *testing.T) {
			connString := setupCrossSchemaFK(t, "app")
			opts := &Options{ConnString: connString, Schemas: schemas}

			dumped, err := NewClient(opts).Dump(context.Background(), &DumpOptions{})
			require.NoError(t, err)
			require.Contains(t, dumped.String(), "REFERENCES base(id)")

			assert.Empty(t, planCrossSchemaFK(t, opts, dumped.String()))
		})
	}
}

// With --schema-map the owning schema has another name in the desired
// schema, and the reference into public still plans clean.
func TestPlan_CrossSchemaFK_SchemaMap(t *testing.T) {
	connString := setupCrossSchemaFK(t, "app_stg")
	sql := planCrossSchemaFK(t, &Options{
		ConnString: connString,
		Schemas:    []string{"public", "app_stg"},
		SchemaMap:  map[string]string{"app_stg": "app"},
	}, `
CREATE TABLE public.base (id integer PRIMARY KEY);
CREATE TABLE app.base (id integer PRIMARY KEY);
CREATE TABLE app.item (id integer PRIMARY KEY, base_id integer REFERENCES public.base (id));`)

	assert.Empty(t, sql)
}
