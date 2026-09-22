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

	// --force is about drift alone; with none there is nothing to warn about.
	t.Run("--force says nothing where there is no drift", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`, publicScope)

		_, out, err := applyFrom(t, ctx, path, true)
		require.NoError(t, err)
		assert.NotContains(t, out, "Warning")
	})

	// Both were decided when the plan was written, so they come out of the
	// file rather than out of a diff apply-from does not make.
	t.Run("the skipped drops and the ignored statements come from the file", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (id integer NOT NULL);
CREATE TABLE public.legacy (id integer NOT NULL);`)

		dir := t.TempDir()
		desiredFile := filepath.Join(dir, "desired.sql")
		require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);

CREATE INDEX CONCURRENTLY users_name_idx ON public.users (name);`), 0o644))
		out := filepath.Join(dir, "plan.json")

		// No --allow-drop, so the table left out of the desired schema is a
		// skipped drop rather than a DROP.
		client := NewClient(&Options{
			ConnString:   testutil.ConnString(),
			ScopeOptions: publicScope,
		})
		_, err := client.Plan(ctx, &PlanOptions{Files: []string{desiredFile}, Out: out})
		require.NoError(t, err)

		result, _, err := applyFrom(t, ctx, out, false)
		require.NoError(t, err)
		assert.Contains(t, result.DisallowedDrops, "-- skipped: DROP TABLE public.legacy;")
	})

	// The flag that refuses a transaction is the plan file's, not a fresh
	// reading of the statements.
	t.Run("--with-tx is refused where the plan holds CONCURRENTLY index DDL", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);

CREATE INDEX CONCURRENTLY users_name_idx ON public.users (name);`, publicScope)

		plan, err := readPlanFile(path)
		require.NoError(t, err)
		require.True(t, plan.HasConcurrentlyIndex)

		client := NewClient(&Options{ConnString: testutil.ConnString()})
		var buf bytes.Buffer
		_, err = client.ApplyFrom(ctx, &ApplyFromOptions{
			PlanFile: path,
			WithTx:   true,
		}, &buf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--with-tx")
	})

	t.Run("--with-tx wraps the plan in a transaction", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`, publicScope)

		client := NewClient(&Options{ConnString: testutil.ConnString()})
		var buf bytes.Buffer
		result, err := client.ApplyFrom(ctx, &ApplyFromOptions{
			PlanFile: path,
			WithTx:   true,
		}, &buf)
		require.NoError(t, err)
		assert.True(t, result.Applied)
		assert.Contains(t, buf.String(), "-- Transaction started")
		assert.Contains(t, buf.String(), "-- Transaction committed")
	})

	// The pre-SQL belongs to the plan, so apply-from runs the one the file
	// holds.
	t.Run("the pre-SQL from the file is executed", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		dir := t.TempDir()
		desiredFile := filepath.Join(dir, "desired.sql")
		require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`), 0o644))
		out := filepath.Join(dir, "plan.json")

		client := NewClient(&Options{
			ConnString:   testutil.ConnString(),
			ScopeOptions: publicScope,
		})
		_, err := client.Plan(ctx, &PlanOptions{
			AllowDrop: []string{"all"},
			Files:     []string{desiredFile},
			PreSQL:    "SET lock_timeout = '5s';",
			Out:       out,
		})
		require.NoError(t, err)

		_, output, err := applyFrom(t, ctx, out, false)
		require.NoError(t, err)
		assert.Contains(t, output, "SET lock_timeout = '5s';")
	})

	// execute-first runs before the managed DDL, as it does in apply.
	t.Run("execute-first runs before the DDL", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);

-- pista:execute-first SELECT true
CREATE TABLE public.first (id integer NOT NULL);

-- pista:execute SELECT true
CREATE TABLE public.last (id integer NOT NULL);`, publicScope)

		_, out, err := applyFrom(t, ctx, path, false)
		require.NoError(t, err)
		first := strings.Index(out, "public.first")
		alter := strings.Index(out, "ALTER TABLE public.users")
		last := strings.Index(out, "public.last")
		require.Positive(t, first)
		assert.Less(t, first, alter)
		assert.Less(t, alter, last)
	})

	t.Run("a plan file that is not JSON is reported", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "plan.json")
		require.NoError(t, os.WriteFile(path, []byte("not json"), 0o644))

		_, _, err := applyFrom(t, ctx, path, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse the plan file")
	})

	// The scope is read back from the file and has to hold up: a file naming
	// no schema cannot be applied.
	t.Run("a plan file naming no schema is refused", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `CREATE TABLE public.users (id integer NOT NULL);`, publicScope)

		plan, err := readPlanFile(path)
		require.NoError(t, err)
		plan.Scope.Schemas = nil
		require.NoError(t, writePlanFile(path, plan))

		_, _, err = applyFrom(t, ctx, path, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema")
	})

	// The exclusion guards the database, so a plan file waits for another
	// apply the same way a fresh one does.
	t.Run("--exclusive fails while another apply holds the exclusion", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`, publicScope)

		holder := testutil.ConnectDB(t)
		defer holder.Close(ctx) //nolint:errcheck
		holdExclusive(t, ctx, holder)
		defer releaseExclusive(ctx, holder) //nolint:errcheck

		client := NewClient(&Options{ConnString: testutil.ConnString()})
		var buf bytes.Buffer
		_, err := client.ApplyFrom(ctx, &ApplyFromOptions{
			PlanFile:  path,
			Exclusive: true,
		}, &buf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "another exclusive apply")
		assert.NotContains(t, dumpPublic(t, ctx), "name text", "nothing ran")
	})

	t.Run("a connection that cannot be opened is reported", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		path := writePlan(t, ctx, `CREATE TABLE public.users (id integer NOT NULL);`, publicScope)

		client := NewClient(&Options{ConnString: "postgres://postgres@localhost:1/postgres"})
		var buf bytes.Buffer
		_, err := client.ApplyFrom(ctx, &ApplyFromOptions{PlanFile: path}, &buf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to connect database")
	})

	// The schemas, the schema map and the search path are restored from the
	// file as well, so apply-from reads and writes where the plan did even
	// though it was given neither.
	t.Run("a plan under another schema is applied there", func(t *testing.T) {
		connString := setupSchemaDB(t, ctx, "myschema", `CREATE TABLE myschema.users (id integer NOT NULL);`)

		dir := t.TempDir()
		desiredFile := filepath.Join(dir, "desired.sql")
		// Written against public, and mapped onto myschema by the plan.
		require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`), 0o644))
		out := filepath.Join(dir, "plan.json")

		planClient := NewClient(&Options{
			ConnString: connString,
			Schemas:    []string{"myschema"},
			SchemaMap:  map[string]string{"myschema": "public"},
		})
		_, err := planClient.Plan(ctx, &PlanOptions{
			AllowDrop: []string{"all"},
			Files:     []string{desiredFile},
			Out:       out,
		})
		require.NoError(t, err)

		plan, err := readPlanFile(out)
		require.NoError(t, err)
		assert.Equal(t, []string{"myschema"}, plan.Scope.Schemas)
		assert.Equal(t, map[string]string{"myschema": "public"}, plan.Scope.SchemaMap)

		client := NewClient(&Options{ConnString: connString})
		var buf bytes.Buffer
		result, err := client.ApplyFrom(ctx, &ApplyFromOptions{PlanFile: out}, &buf)
		require.NoError(t, err)
		assert.True(t, result.Applied)
		assert.Contains(t, buf.String(), "ALTER TABLE myschema.users ADD COLUMN name text;")
		assert.Equal(t, "schema myschema", result.Count.SchemaLabel())
	})

	// An ignored object is hashed on both sides, since apply-from reads no
	// desired schema and cannot tell which objects the plan ignored. A change
	// to one is drift, although the statements do not touch it.
	t.Run("a change to an ignored object is drift", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (id integer NOT NULL);
CREATE TABLE public.owned_elsewhere (id integer NOT NULL);`)
		path := writePlan(t, ctx, `
-- pista:ignore
CREATE TABLE public.owned_elsewhere (id integer NOT NULL);

CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`, publicScope)

		execSQL(t, ctx, conn, `ALTER TABLE public.owned_elsewhere ADD COLUMN note text`)

		_, _, err := applyFrom(t, ctx, path, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "drift")
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
