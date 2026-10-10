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

const invalidIndexSchema = `CREATE TABLE public.users (
    id integer NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX users_email_key ON public.users USING btree (email);
`

// setupInvalidIndex leaves public.users_email_key invalid: a unique index
// built concurrently over duplicates fails and stays behind. A Go test rather
// than a fixture, since CONCURRENTLY cannot run in the transaction the
// fixture harness loads its init SQL in.
func setupInvalidIndex(t *testing.T, ctx context.Context) (string, string) {
	t.Helper()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
INSERT INTO public.users VALUES (1, 'a'), (2, 'a');`)
	_, err := conn.Exec(ctx, "CREATE UNIQUE INDEX CONCURRENTLY users_email_key ON public.users (email)")
	require.Error(t, err)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(invalidIndexSchema), 0o644))
	return conn.Config().ConnString(), desiredFile
}

func TestPlan_InvalidIndexWarning(t *testing.T) {
	ctx := context.Background()
	connString, desiredFile := setupInvalidIndex(t, ctx)

	client := pistachio.NewClient(&pistachio.Options{ConnString: connString, Schemas: []string{"public"}})
	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)

	// The index matches the desired schema, so there is nothing to apply.
	assert.False(t, got.HasChanges)
	assert.Equal(t, "-- Warning: index public.users_email_key is invalid", got.InvalidIndexes)
}

func TestApply_InvalidIndexWarning(t *testing.T) {
	ctx := context.Background()
	connString, desiredFile := setupInvalidIndex(t, ctx)

	client := pistachio.NewClient(&pistachio.Options{ConnString: connString, Schemas: []string{"public"}})
	var buf bytes.Buffer
	got, err := client.Apply(ctx, &pistachio.ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)

	assert.False(t, got.Applied)
	assert.Equal(t, "-- Warning: index public.users_email_key is invalid", got.InvalidIndexes)
}

// An index the filters leave out is not reported.
func TestPlan_InvalidIndexWarning_Excluded(t *testing.T) {
	ctx := context.Background()
	connString, _ := setupInvalidIndex(t, ctx)

	client := pistachio.NewClient(&pistachio.Options{
		ConnString: connString,
		Schemas:    []string{"public"}, Exclude: []string{"users"},
	})
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, nil, 0o644))
	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.InvalidIndexes)
}

// An index on a materialized view is reported the same way.
func TestPlan_InvalidIndexWarning_MaterializedView(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	testutil.SetupDB(t, ctx, conn, `CREATE MATERIALIZED VIEW public.emails AS SELECT 'a'::text AS email UNION ALL SELECT 'a'::text;`)
	_, err := conn.Exec(ctx, "CREATE UNIQUE INDEX CONCURRENTLY emails_email_key ON public.emails (email)")
	require.Error(t, err)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE MATERIALIZED VIEW public.emails AS
SELECT 'a'::text AS email
UNION ALL
SELECT 'a'::text AS email;
CREATE UNIQUE INDEX emails_email_key ON public.emails USING btree (email);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{ConnString: conn.Config().ConnString(), Schemas: []string{"public"}})
	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Equal(t, "-- Warning: index public.emails_email_key is invalid", got.InvalidIndexes)
}

// Each invalid index gets a line of its own, and a valid one gets none.
func TestPlan_InvalidIndexWarning_Several(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (
    id integer NOT NULL,
    email text,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX users_id_idx ON public.users USING btree (id);
INSERT INTO public.users VALUES (1, 'a', 'b'), (2, 'a', 'b');`)
	for _, sql := range []string{
		"CREATE UNIQUE INDEX CONCURRENTLY users_email_key ON public.users (email)",
		"CREATE UNIQUE INDEX CONCURRENTLY users_name_key ON public.users (name)",
	} {
		_, err := conn.Exec(ctx, sql)
		require.Error(t, err)
	}

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (
    id integer NOT NULL,
    email text,
    name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX users_id_idx ON public.users USING btree (id);
CREATE UNIQUE INDEX users_email_key ON public.users USING btree (email);
CREATE UNIQUE INDEX users_name_key ON public.users USING btree (name);
`), 0o644))

	client := pistachio.NewClient(&pistachio.Options{ConnString: conn.Config().ConnString(), Schemas: []string{"public"}})
	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Equal(t, "-- Warning: index public.users_email_key is invalid\n-- Warning: index public.users_name_key is invalid", got.InvalidIndexes)
}

// An index on a table the desired schema ignores is not managed, so it is not
// reported.
func TestPlan_InvalidIndexWarning_Ignored(t *testing.T) {
	ctx := context.Background()
	connString, _ := setupInvalidIndex(t, ctx)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte("-- pista:ignore\n"+invalidIndexSchema), 0o644))

	client := pistachio.NewClient(&pistachio.Options{ConnString: connString, Schemas: []string{"public"}})
	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.InvalidIndexes)
}

// An index created ON ONLY a partitioned table is invalid until an index of
// every partition is attached. It is reported too, with no change planned:
// the dump writes it without ONLY, and that is what the catalog read reports.
func TestPlan_InvalidIndexWarning_PartitionedOnOnly(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.p (id integer, k integer) PARTITION BY RANGE (id);
CREATE TABLE public.p1 PARTITION OF public.p FOR VALUES FROM (0) TO (10);
CREATE INDEX p_k_idx ON ONLY public.p (k);`)

	client := pistachio.NewClient(&pistachio.Options{ConnString: conn.Config().ConnString(), Schemas: []string{"public"}})
	dumped, err := client.Dump(ctx, &pistachio.DumpOptions{})
	require.NoError(t, err)

	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(dumped.String()), 0o644))
	got, err := client.Plan(ctx, &pistachio.PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.False(t, got.HasChanges, got.SQL)
	assert.Equal(t, "-- Warning: index public.p_k_idx is invalid", got.InvalidIndexes)
}
