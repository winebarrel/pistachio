package pistachio

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/internal/testutil"
	"github.com/winebarrel/pistachio/model"
)

// --split renders each file through the same model, so every file carries the
// size comments.
func TestDumpResult_Files_Explain(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx)

	testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (id integer NOT NULL);
CREATE INDEX users_id_idx ON public.users USING btree (id);
INSERT INTO public.users SELECT g FROM generate_series(1, 3) g;
ANALYZE public.users;`)

	client := NewClient(&Options{
		ConnString: conn.Config().ConnString(),
		Schemas:    []string{"public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{Explain: true})
	require.NoError(t, err)

	file := got.Files()["public.users.sql"]
	assert.Contains(t, file, "-- public.users (~3 rows, 8192 bytes, as of ")
	assert.Contains(t, file, "-- 16 kB\nCREATE INDEX users_id_idx ON public.users")
}

func TestExplainDump_CatalogError(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	cat, err := catalog.NewCatalog(conn, []string{"public"})
	require.NoError(t, err)
	require.NoError(t, conn.Close(ctx))

	err = explainDump(ctx, cat, orderedmap.New[string, *model.Table](), orderedmap.New[string, *model.View]())
	require.Error(t, err)
}

// dump --explain reads the estimates under the schema the catalog names, so a
// --schema-map that renames the schema still finds them.
func TestDump_ExplainSchemaMap(t *testing.T) {
	ctx := context.Background()
	connStr := setupSchemaDB(t, ctx, "staging", `
CREATE TABLE staging.users (id integer NOT NULL, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE INDEX users_id_idx ON staging.users USING btree (id);
INSERT INTO staging.users SELECT g FROM generate_series(1, 3) g;
ANALYZE staging.users;`)

	client := NewClient(&Options{
		ConnString: connStr,
		Schemas:    []string{"staging"},
		SchemaMap:  map[string]string{"staging": "public"},
	})

	got, err := client.Dump(ctx, &DumpOptions{Explain: true})
	require.NoError(t, err)
	out := got.String()
	assert.Contains(t, out, "-- public.users (~3 rows, 8192 bytes, as of ")
	assert.Contains(t, out, "-- 16 kB\nCREATE INDEX users_id_idx ON public.users")
}
