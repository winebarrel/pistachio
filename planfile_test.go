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

// planOut runs plan with --out and returns what it wrote.
func planOut(t *testing.T, ctx context.Context, client *Client, desiredSQL string) (*PlanResult, *planFile) {
	t.Helper()
	dir := t.TempDir()
	desiredFile := filepath.Join(dir, "desired.sql")
	require.NoError(t, os.WriteFile(desiredFile, []byte(desiredSQL), 0o644))
	out := filepath.Join(dir, "plan.json")

	result, err := client.Plan(ctx, &PlanOptions{
		AllowDrop: []string{"all"},
		Files:     []string{desiredFile},
		Out:       out,
	})
	require.NoError(t, err)

	planFile, err := readPlanFile(out)
	require.NoError(t, err)
	return result, planFile
}

func TestPlanOut(t *testing.T) {
	ctx := context.Background()
	conn := testutil.ConnectDB(t)
	defer conn.Close(ctx) //nolint:errcheck
	pgMajor := testutil.ServerMajorVersion(t, ctx, conn)

	client := NewClient(&Options{
		ConnString: testutil.ConnString(),
		Schemas:    []string{"public"},
	})

	t.Run("it writes the statements, the scope and the state hash", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)

		_, planFile := planOut(t, ctx, client, `
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`)

		assert.Equal(t, planFileVersion, planFile.Version)
		assert.Equal(t, pgMajor, planFile.ServerVersion)
		assert.Equal(t, []string{"ALTER TABLE public.users ADD COLUMN name text;"}, planFile.Stmts)
		assert.Equal(t, []string{"public"}, planFile.Scope.Schemas)
		require.NotNil(t, planFile.Scope.SearchPath)
		assert.Equal(t, DefaultSearchPath, *planFile.Scope.SearchPath)
		assert.Equal(t, hashNow(t, ctx, client), planFile.StateHash)
	})

	// A plan with nothing to do is still a plan: apply-from runs it and
	// reports no changes, the same way apply does.
	t.Run("a plan with no changes is written too", func(t *testing.T) {
		const schema = `CREATE TABLE public.users (id integer NOT NULL);`
		testutil.SetupDB(t, ctx, conn, schema)

		result, planFile := planOut(t, ctx, client, schema)

		assert.False(t, result.HasChanges)
		assert.Empty(t, planFile.Stmts)
		assert.Empty(t, planFile.ExecuteStmts)
		assert.NotEmpty(t, planFile.StateHash)
	})

	// The pre-SQL is a statement of its own in the file: apply runs it outside
	// the managed DDL, before the search_path is set, and reports its own
	// failure for it.
	t.Run("the pre-SQL is kept apart from the DDL", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		dir := t.TempDir()
		desiredFile := filepath.Join(dir, "desired.sql")
		require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`), 0o644))
		out := filepath.Join(dir, "plan.json")

		_, err := client.Plan(ctx, &PlanOptions{
			AllowDrop: []string{"all"},
			Files:     []string{desiredFile},
			PreSQL:    "SET lock_timeout = '5s';",
			Out:       out,
		})
		require.NoError(t, err)

		planFile, err := readPlanFile(out)
		require.NoError(t, err)
		assert.Equal(t, "SET lock_timeout = '5s';", planFile.PreSQL)
		assert.Equal(t, []string{"ALTER TABLE public.users ADD COLUMN name text;"}, planFile.Stmts)
	})

	// The check SQL is evaluated once, when the plan is written. What it
	// decided is what the file holds, so the statement carries no condition
	// into apply-from.
	t.Run("an execute statement is decided at plan time", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)

		_, planFile := planOut(t, ctx, client, `
CREATE TABLE public.users (id integer NOT NULL);

-- pista:execute-first SELECT true
SELECT 1;

-- pista:execute SELECT false
SELECT 2;

-- pista:execute
SELECT 3;`)

		require.Len(t, planFile.ExecuteStmts, 2)
		assert.Equal(t, "SELECT 1", planFile.ExecuteStmts[0].SQL)
		assert.True(t, planFile.ExecuteStmts[0].First)
		assert.Equal(t, "SELECT 3", planFile.ExecuteStmts[1].SQL)
		assert.False(t, planFile.ExecuteStmts[1].First)
		for _, es := range planFile.ExecuteStmts {
			assert.Empty(t, es.CheckSQL, "the decision is the plan's, not apply-from's")
		}
	})

	// Without --out an unevaluable check is noted and apply decides. With
	// --out there is no apply to decide, so the plan fails rather than write a
	// guess as a promise.
	t.Run("an unevaluable check fails the plan file", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		dir := t.TempDir()
		desiredFile := filepath.Join(dir, "desired.sql")
		require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (id integer NOT NULL);

-- pista:execute SELECT count(*) = 0 FROM public.no_such_table
SELECT 1;`), 0o644))

		result, err := client.Plan(ctx, &PlanOptions{
			AllowDrop: []string{"all"},
			Files:     []string{desiredFile},
		})
		require.NoError(t, err)
		assert.Contains(t, result.SQL, "check SQL could not be evaluated at plan time")

		out := filepath.Join(dir, "plan.json")
		_, err = client.Plan(ctx, &PlanOptions{
			AllowDrop: []string{"all"},
			Files:     []string{desiredFile},
			Out:       out,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "check SQL")
		assert.NoFileExists(t, out, "a plan file is not left behind by a failed plan")
	})

	// The CONCURRENTLY pre-SQL and the flag that gates it are the plan's, so
	// apply-from does not have to read the statements again to decide.
	t.Run("the CONCURRENTLY pre-SQL is recorded with the flag that gates it", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL, name text);`)
		dir := t.TempDir()
		desiredFile := filepath.Join(dir, "desired.sql")
		require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);

CREATE INDEX CONCURRENTLY users_name_idx ON public.users (name);`), 0o644))
		out := filepath.Join(dir, "plan.json")

		_, err := client.Plan(ctx, &PlanOptions{
			AllowDrop:          []string{"all"},
			Files:              []string{desiredFile},
			ConcurrentlyPreSQL: "SET lock_timeout = '5s';",
			Out:                out,
		})
		require.NoError(t, err)

		planFile, err := readPlanFile(out)
		require.NoError(t, err)
		assert.True(t, planFile.HasConcurrentlyIndex)
		assert.Equal(t, "SET lock_timeout = '5s';", planFile.ConcurrentlyPreSQL)
	})

	// --explain writes a comment above the statements it has something to say
	// about. That belongs in the output a person reads; the file holds the
	// statements apply-from executes.
	t.Run("the explain comments stay out of the plan file", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (id integer NOT NULL, n integer);
INSERT INTO public.users SELECT g, g FROM generate_series(1, 3) g;
ANALYZE public.users;`)
		dir := t.TempDir()
		desiredFile := filepath.Join(dir, "desired.sql")
		require.NoError(t, os.WriteFile(desiredFile, []byte(`
CREATE TABLE public.users (
    id integer NOT NULL,
    n integer,
    CONSTRAINT users_n_positive CHECK (n > 0)
);`), 0o644))
		out := filepath.Join(dir, "plan.json")

		result, err := client.Plan(ctx, &PlanOptions{
			AllowDrop: []string{"all"},
			Files:     []string{desiredFile},
			Explain:   true,
			Out:       out,
		})
		require.NoError(t, err)
		require.Contains(t, result.SQL, "-- scan, blocks reads and writes")

		planFile, err := readPlanFile(out)
		require.NoError(t, err)
		assert.Equal(t, []string{
			"ALTER TABLE public.users ADD CONSTRAINT users_n_positive CHECK (n > 0);",
		}, planFile.Stmts)
	})

	// The count is the plan's. An object the desired schema ignores is left
	// out of it, and apply-from, which reads no desired schema, could not tell
	// which those are.
	t.Run("the count is recorded and leaves an ignored object out", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `
CREATE TABLE public.users (id integer NOT NULL);
CREATE TABLE public.legacy (id integer NOT NULL, name text);`)

		result, planFile := planOut(t, ctx, client, `
-- pista:ignore
CREATE TABLE public.legacy (id integer NOT NULL);

CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);`)

		assert.Equal(t, 1, result.Count.Tables)
		assert.Equal(t, result.Count, planFile.Count)

		// The names are recorded so apply-from can drop them from its read.
		// The hash is of what the plan compared, which is not them.
		assert.Equal(t, []string{"public.legacy"}, planFile.IgnoredObjects)
		assert.Equal(t, "-- ignored: public.legacy", result.Ignored)
	})

	t.Run("a plan file that cannot be written is reported", func(t *testing.T) {
		testutil.SetupDB(t, ctx, conn, `CREATE TABLE public.users (id integer NOT NULL);`)
		dir := t.TempDir()
		desiredFile := filepath.Join(dir, "desired.sql")
		require.NoError(t, os.WriteFile(desiredFile, []byte(`CREATE TABLE public.users (id integer NOT NULL);`), 0o644))

		_, err := client.Plan(ctx, &PlanOptions{
			AllowDrop: []string{"all"},
			Files:     []string{desiredFile},
			Out:       filepath.Join(dir, "no-such-dir", "plan.json"),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write the plan file")
	})

	t.Run("a plan file of another format version is refused", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "plan.json")
		require.NoError(t, os.WriteFile(path, []byte(`{"version":0}`), 0o644))

		_, err := readPlanFile(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "version")
	})
}
