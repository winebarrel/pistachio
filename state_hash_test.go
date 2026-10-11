package pistachio

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/testutil"
)

// hashNow reads the current schema through the client's own connection and
// returns its state hash.
func hashNow(t *testing.T, ctx context.Context, client *Client) string {
	t.Helper()
	conn, err := client.connect(ctx, true)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck
	current, err := client.currentState(ctx, conn)
	require.NoError(t, err)
	hash, err := current.stateHash()
	require.NoError(t, err)
	assert.Len(t, hash, 64)
	return hash
}

func execSQL(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(ctx, sql)
	require.NoError(t, err)
}

func TestStateHash(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	newClient := func(opts ...func(*Options)) *Client {
		options := &Options{
			ConnString: testutil.ConnString(),
			Schemas:    []string{"public"},
		}
		for _, opt := range opts {
			opt(options)
		}
		return NewClient(options)
	}

	t.Run("a schema read twice hashes the same", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (id integer NOT NULL, name text, CONSTRAINT users_pkey PRIMARY KEY (id));`)
		client := newClient()
		first := hashNow(t, ctx, client)
		assert.Equal(t, first, hashNow(t, ctx, client))
	})

	t.Run("a schema change is detected", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		client := newClient()
		before := hashNow(t, ctx, client)
		execSQL(t, ctx, conn, `ALTER TABLE public.users ADD COLUMN name text`)
		assert.NotEqual(t, before, hashNow(t, ctx, client))
	})

	// A table dropped and created again with the same definition takes a new
	// OID, which the model carries, so the plan computed against the old one
	// is no longer the plan the database would get.
	t.Run("a recreated table is detected", func(t *testing.T) {
		const ddl = `CREATE TABLE public.users (id integer NOT NULL, name text);`
		testutil.SetupDB(t, ctx, conn, ddl)
		client := newClient()
		before := hashNow(t, ctx, client)
		execSQL(t, ctx, conn, `DROP TABLE public.users`)
		execSQL(t, ctx, conn, ddl)
		assert.NotEqual(t, before, hashNow(t, ctx, client))
	})

	// The storage parameters are read whether or not they are managed, but the
	// diff only sees them with --manage-storage-param, so only then may they
	// move the hash. An autovacuum setting changed on the database is not
	// drift for a plan that ignores it.
	t.Run("a storage parameter follows --manage-storage-param", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		unmanaged := newClient()
		managed := newClient(func(o *Options) { o.ManageStorageParam = true })
		unmanagedBefore := hashNow(t, ctx, unmanaged)
		managedBefore := hashNow(t, ctx, managed)

		execSQL(t, ctx, conn, `ALTER TABLE public.users SET (autovacuum_vacuum_scale_factor = 0.05)`)

		assert.Equal(t, unmanagedBefore, hashNow(t, ctx, unmanaged))
		assert.NotEqual(t, managedBefore, hashNow(t, ctx, managed))
	})

	// What a filter leaves out cannot change the plan, so it cannot change the
	// hash either.
	t.Run("an excluded table is left out", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (id integer NOT NULL);
CREATE TABLE public.posts (id integer NOT NULL);`)
		client := newClient(func(o *Options) { o.Exclude = []string{"posts"} })
		before := hashNow(t, ctx, client)
		execSQL(t, ctx, conn, `ALTER TABLE public.posts ADD COLUMN title text`)
		assert.Equal(t, before, hashNow(t, ctx, client))
		execSQL(t, ctx, conn, `ALTER TABLE public.users ADD COLUMN name text`)
		assert.NotEqual(t, before, hashNow(t, ctx, client))
	})

	// The routines are only read with --manage-routine, so the flag decides
	// whether one of them can move the hash.
	t.Run("a routine follows --manage-routine", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		unmanaged := newClient()
		managed := newClient(func(o *Options) { o.ManageRoutine = true })
		unmanagedBefore := hashNow(t, ctx, unmanaged)
		managedBefore := hashNow(t, ctx, managed)

		execSQL(t, ctx, conn, `CREATE FUNCTION public.f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$`)

		assert.Equal(t, unmanagedBefore, hashNow(t, ctx, unmanaged))
		assert.NotEqual(t, managedBefore, hashNow(t, ctx, managed))
	})

	// The hash a plan records and the one apply-from recomputes have to come
	// out of the same read, or every plan file would look like drift.
	t.Run("the plan and the standalone read agree", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (id integer NOT NULL, name text, CONSTRAINT users_pkey PRIMARY KEY (id));
CREATE TYPE public.status AS ENUM ('active');
CREATE VIEW public.v AS SELECT id FROM public.users;`)
		client := newClient()

		desiredFile := filepath.Join(t.TempDir(), "desired.sql")
		require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (id integer NOT NULL);`), 0o644))
		desired, err := client.loadDesiredInput([]string{desiredFile}, "", "", "", "")
		require.NoError(t, err)

		planConn, err := client.connect(ctx, true)
		require.NoError(t, err)
		defer planConn.Close(ctx) //nolint:errcheck
		result, err := client.diffAll(ctx, planConn, &diffAllOptions{
			FilterOptions: client.FilterOptions,
			Desired:       desired,
			StateHash:     true,
		})
		require.NoError(t, err)

		assert.Equal(t, hashNow(t, ctx, client), result.StateHash)
	})
}
