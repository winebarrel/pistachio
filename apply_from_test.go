package pistachio

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/testutil"
)

// writePlan runs plan --out under the given scope and returns the path it
// wrote to.
func writePlan(t *testing.T, ctx context.Context, desiredSQL string, scope ScopeOptions) string {
	t.Helper()
	dir := t.TempDir()
	desiredFile := filepath.Join(dir, "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(desiredSQL), 0o644))
	out := filepath.Join(dir, "plan.json")

	client := NewClient(&Options{
		ConnString:   testutil.ConnString(),
		ScopeOptions: scope,
	})
	_, err := client.Plan(ctx, &PlanOptions{
		AllowDrop: []string{"all"},
		Files:     []string{desiredFile},
		Out:       out,
	})
	require.NoError(t, err)
	return out
}

// applyFrom runs apply-from with only the connection given, the way the CLI
// does: the scope is the plan file's.
func applyFrom(t *testing.T, ctx context.Context, path string, force bool) (*ApplyResult, string, error) {
	t.Helper()
	client := NewClient(&Options{ConnString: testutil.ConnString()})
	var buf bytes.Buffer
	result, err := client.ApplyFrom(ctx, &ApplyFromOptions{PlanFile: path, Force: force}, &buf)
	return result, buf.String(), err
}

func TestApplyFrom(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck

	publicScope := ScopeOptions{Schemas: []string{"public"}}

	t.Run("it runs the statements the plan file holds", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`, publicScope)

		result, out, err := applyFrom(t, ctx, path, false)
		require.NoError(t, err)
		assert.True(t, result.Applied)
		assert.Equal(t, "ALTER TABLE public.users ADD COLUMN name text;", strings.TrimSpace(out))
		assert.Equal(t, 1, result.Count.Tables)

		// The column is there, and a plan of the same schema is now empty.
		dumped := dumpPublic(t, ctx)
		assert.Contains(t, dumped, "name text")
	})

	t.Run("a plan with nothing to do applies nothing", func(t *testing.T) {
		const schema = `CREATE TABLE public.users (id integer NOT NULL);`
		testutil.SetupDB(t, ctx, conn, schema)
		path := writePlan(t, ctx, schema, publicScope)

		result, out, err := applyFrom(t, ctx, path, false)
		require.NoError(t, err)
		assert.False(t, result.Applied)
		assert.Empty(t, strings.TrimSpace(out))
	})

	// The point of the file: what it was computed against has to still be
	// there.
	t.Run("drift stops the apply", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`, publicScope)

		execSQL(t, ctx, conn, `ALTER TABLE public.users ADD COLUMN other text`)

		_, _, err := applyFrom(t, ctx, path, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "drift")
		assert.NotContains(t, dumpPublic(t, ctx), "name text", "nothing ran")
	})

	t.Run("--force applies over drift with a warning", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`, publicScope)

		execSQL(t, ctx, conn, `ALTER TABLE public.users ADD COLUMN other text`)

		result, out, err := applyFrom(t, ctx, path, true)
		require.NoError(t, err)
		assert.True(t, result.Applied)
		assert.Contains(t, out, "-- Warning:")
		assert.Contains(t, out, "drift")
		assert.Contains(t, dumpPublic(t, ctx), "name text")
	})

	// The scope comes out of the file, so the hash is computed over the same
	// read the plan made. A filter the plan ran under is not drift.
	t.Run("the scope is the plan file's", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (id integer NOT NULL);
CREATE TABLE public.posts (id integer NOT NULL);`)
		scope := ScopeOptions{
			Schemas: []string{"public"},
			Exclude: []string{"posts"},
		}
		path := writePlan(t, ctx, `
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`, scope)

		// Outside the plan's scope, so not drift.
		execSQL(t, ctx, conn, `ALTER TABLE public.posts ADD COLUMN title text`)

		result, _, err := applyFrom(t, ctx, path, false)
		require.NoError(t, err)
		assert.True(t, result.Applied)
		assert.Equal(t, 1, result.Count.Tables, "the count follows the plan file's filters")
	})

	// An execute statement carries no check into the file, so it runs.
	t.Run("an execute statement runs as decided", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `
CREATE TABLE public.users (id integer NOT NULL);

-- pista:execute SELECT true
CREATE TABLE public.made_by_execute (id integer NOT NULL);`, publicScope)

		result, out, err := applyFrom(t, ctx, path, false)
		require.NoError(t, err)
		assert.True(t, result.Applied)
		assert.Contains(t, out, "-- pista:execute\n")
		assert.Contains(t, dumpPublic(t, ctx), "made_by_execute")
	})

	t.Run("a plan file written for another server version is refused", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `CREATE TABLE public.users (id integer NOT NULL);`, publicScope)

		plan, err := readPlanFile(path)
		require.NoError(t, err)
		plan.ServerVersion++
		require.NoError(t, writePlanFile(path, plan))

		_, _, err = applyFrom(t, ctx, path, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "server")

		// --force is about drift, not about reading a file this pista cannot
		// run.
		_, _, err = applyFrom(t, ctx, path, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "server")
	})

	t.Run("a missing plan file is reported", func(t *testing.T) {
		_, _, err := applyFrom(t, ctx, filepath.Join(t.TempDir(), "nope.json"), false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "plan file")
	})
}

func dumpPublic(t *testing.T, ctx context.Context) string {
	t.Helper()
	client := NewClient(&Options{
		ConnString: testutil.ConnString(),
		Schemas:    []string{"public"},
	})
	dumped, err := client.Dump(ctx, &DumpOptions{})
	require.NoError(t, err)
	return dumped.String()
}
