package pistachio_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/internal/testutil"
)

func TestEngineIsDSQL(t *testing.T) {
	assert.True(t, pistachio.EngineIsDSQL(pistachio.EngineDSQL))
	assert.False(t, pistachio.EngineIsDSQL(pistachio.EnginePostgres))
	assert.False(t, pistachio.EngineIsDSQL(pistachio.Engine("")))
}

func TestEngineValidate(t *testing.T) {
	for _, e := range []pistachio.Engine{"", pistachio.EnginePostgres, pistachio.EngineDSQL} {
		require.NoError(t, pistachio.EngineValidate(e))
	}
	require.EqualError(t, pistachio.EngineValidate(pistachio.Engine("mysql")), `unknown engine "mysql"`)
}

func TestValidatePlanEngine(t *testing.T) {
	opts := &pistachio.PlanOptions{Explain: true, BulkAlter: true}
	require.NoError(t, pistachio.ValidatePlanEngine(pistachio.EnginePostgres, opts))
	require.NoError(t, pistachio.ValidatePlanEngine("", opts))
	require.EqualError(t, pistachio.ValidatePlanEngine(pistachio.EngineDSQL, opts), "--explain cannot be used with --engine dsql")
	require.NoError(t, pistachio.ValidatePlanEngine(pistachio.EngineDSQL, &pistachio.PlanOptions{}))
}

func TestValidateApplyEngine(t *testing.T) {
	opts := &pistachio.ApplyOptions{BulkAlter: true, WithTx: true}
	require.NoError(t, pistachio.ValidateApplyEngine(pistachio.EnginePostgres, opts))
	require.EqualError(t, pistachio.ValidateApplyEngine(pistachio.EngineDSQL, opts), "--bulk-alter cannot be used with --engine dsql")
	require.EqualError(t, pistachio.ValidateApplyEngine(pistachio.EngineDSQL, &pistachio.ApplyOptions{WithTx: true}), "--with-tx cannot be used with --engine dsql")

	noWait := &pistachio.ApplyOptions{DSQLNoWaitJob: true}
	require.NoError(t, pistachio.ValidateApplyEngine(pistachio.EngineDSQL, noWait))
	require.EqualError(t, pistachio.ValidateApplyEngine(pistachio.EnginePostgres, noWait), "--dsql-no-wait-job requires --engine dsql")
}

func TestValidateApplyFromEngine(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "plan.json")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
		return path
	}

	dsqlPlan := write(t, `{"version":5,"scope":{"engine":"dsql"}}`)
	require.NoError(t, pistachio.ValidateApplyFromEngine(&pistachio.ApplyFromOptions{PlanFile: dsqlPlan}))
	require.NoError(t, pistachio.ValidateApplyFromEngine(&pistachio.ApplyFromOptions{PlanFile: dsqlPlan, DSQLNoWaitJob: true}))
	require.EqualError(t, pistachio.ValidateApplyFromEngine(&pistachio.ApplyFromOptions{PlanFile: dsqlPlan, TryTx: true}), "--try-tx cannot be used with --engine dsql")

	// A plan file written before the engine was recorded is postgres.
	oldPlan := write(t, `{"version":5,"scope":{}}`)
	require.NoError(t, pistachio.ValidateApplyFromEngine(&pistachio.ApplyFromOptions{PlanFile: oldPlan, WithTx: true}))
	require.EqualError(t, pistachio.ValidateApplyFromEngine(&pistachio.ApplyFromOptions{PlanFile: oldPlan, DSQLNoWaitJob: true}), "--dsql-no-wait-job requires --engine dsql")

	unknownPlan := write(t, `{"version":5,"scope":{"engine":"mysql"}}`)
	require.EqualError(t, pistachio.ValidateApplyFromEngine(&pistachio.ApplyFromOptions{PlanFile: unknownPlan}), "plan file "+unknownPlan+`: unknown engine "mysql"`)

	require.ErrorContains(t, pistachio.ValidateApplyFromEngine(&pistachio.ApplyFromOptions{PlanFile: filepath.Join(t.TempDir(), "missing.json")}), "failed to read the plan file")
}

func TestValidateDumpEngine(t *testing.T) {
	require.NoError(t, pistachio.ValidateDumpEngine(pistachio.EnginePostgres, &pistachio.DumpOptions{Explain: true}))
	require.EqualError(t, pistachio.ValidateDumpEngine(pistachio.EngineDSQL, &pistachio.DumpOptions{Explain: true}), "--explain cannot be used with --engine dsql")
	require.NoError(t, pistachio.ValidateDumpEngine(pistachio.EngineDSQL, &pistachio.DumpOptions{}))
}

// DSQL rejects default_transaction_read_only, so a read-only connection sets
// the session's transactions read-only once it is open. Local PostgreSQL
// takes the same statement.
func TestConnect_DSQLReadOnly(t *testing.T) {
	ctx := context.Background()
	client := pistachio.NewClient(&pistachio.Options{ConnString: testutil.ConnString(), Schemas: []string{"public"}, Engine: pistachio.EngineDSQL})

	for readOnly, want := range map[bool]string{true: "on", false: "off"} {
		conn, err := client.Connect(ctx, readOnly)
		require.NoError(t, err)
		var got string
		require.NoError(t, conn.QueryRow(ctx, "SHOW transaction_read_only").Scan(&got))
		assert.Equal(t, want, got)
		require.NoError(t, conn.Close(ctx))
	}
}
