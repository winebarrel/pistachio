package pistachio

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	client := NewClient(&Options{ConnString: connString, Schemas: []string{"public"}})
	got, err := client.Plan(ctx, &PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)

	// The index matches the desired schema, so there is nothing to apply.
	assert.False(t, got.HasChanges)
	assert.Equal(t, "-- Warning: index public.users_email_key is invalid", got.InvalidIndexes)
}

func TestApply_InvalidIndexWarning(t *testing.T) {
	ctx := context.Background()
	connString, desiredFile := setupInvalidIndex(t, ctx)

	client := NewClient(&Options{ConnString: connString, Schemas: []string{"public"}})
	var buf bytes.Buffer
	got, err := client.Apply(ctx, &ApplyOptions{Files: []string{desiredFile}}, &buf)
	require.NoError(t, err)

	assert.False(t, got.Applied)
	assert.Equal(t, "-- Warning: index public.users_email_key is invalid", got.InvalidIndexes)
}

// An index the filters leave out is not reported.
func TestPlan_InvalidIndexWarning_Excluded(t *testing.T) {
	ctx := context.Background()
	connString, _ := setupInvalidIndex(t, ctx)

	client := NewClient(&Options{
		ConnString: connString,
		Schemas:    []string{"public"}, Exclude: []string{"users"},
	})
	desiredFile := filepath.Join(t.TempDir(), "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, nil, 0o644))
	got, err := client.Plan(ctx, &PlanOptions{Files: []string{desiredFile}})
	require.NoError(t, err)
	assert.Empty(t, got.InvalidIndexes)
}
