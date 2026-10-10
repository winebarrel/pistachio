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

func TestEngineIsDSQL(t *testing.T) {
	assert.True(t, EngineDSQL.isDSQL())
	assert.False(t, EnginePostgres.isDSQL())
	assert.False(t, Engine("").isDSQL())
}

func TestEngineValidate(t *testing.T) {
	for _, e := range []Engine{"", EnginePostgres, EngineDSQL} {
		require.NoError(t, e.validate())
	}
	require.EqualError(t, Engine("mysql").validate(), `unknown engine "mysql"`)
}

func TestValidatePlanEngine(t *testing.T) {
	opts := &PlanOptions{Explain: true, BulkAlter: true}
	require.NoError(t, ValidatePlanEngine(EnginePostgres, opts))
	require.NoError(t, ValidatePlanEngine("", opts))
	require.EqualError(t, ValidatePlanEngine(EngineDSQL, opts), "--explain cannot be used with --engine dsql")
	require.NoError(t, ValidatePlanEngine(EngineDSQL, &PlanOptions{}))

	require.NoError(t, ValidatePlanEngine(EngineDSQL, &PlanOptions{DSQLIgnoreAsync: true}))
	require.EqualError(t, ValidatePlanEngine(EnginePostgres, &PlanOptions{DSQLIgnoreAsync: true}), "--dsql-ignore-async requires --engine dsql")
}

func TestValidateApplyEngine(t *testing.T) {
	opts := &ApplyOptions{BulkAlter: true, WithTx: true}
	require.NoError(t, ValidateApplyEngine(EnginePostgres, opts))
	require.EqualError(t, ValidateApplyEngine(EngineDSQL, opts), "--bulk-alter cannot be used with --engine dsql")
	require.EqualError(t, ValidateApplyEngine(EngineDSQL, &ApplyOptions{WithTx: true}), "--with-tx cannot be used with --engine dsql")

	noWait := &ApplyOptions{DSQLNoWaitIndexBuild: true}
	require.NoError(t, ValidateApplyEngine(EngineDSQL, noWait))
	require.EqualError(t, ValidateApplyEngine(EnginePostgres, noWait), "--dsql-no-wait-index-build requires --engine dsql")

	require.NoError(t, ValidateApplyEngine(EngineDSQL, &ApplyOptions{DSQLIgnoreAsync: true}))
	require.EqualError(t, ValidateApplyEngine(EnginePostgres, &ApplyOptions{DSQLIgnoreAsync: true}), "--dsql-ignore-async requires --engine dsql")
}

func TestValidateApplyFromEngine(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "plan.json")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
		return path
	}

	dsqlPlan := write(t, `{"version":5,"scope":{"engine":"dsql"}}`)
	require.NoError(t, ValidateApplyFromEngine(&ApplyFromOptions{PlanFile: dsqlPlan}))
	require.NoError(t, ValidateApplyFromEngine(&ApplyFromOptions{PlanFile: dsqlPlan, DSQLNoWaitIndexBuild: true}))
	require.EqualError(t, ValidateApplyFromEngine(&ApplyFromOptions{PlanFile: dsqlPlan, TryTx: true}), "--try-tx cannot be used with --engine dsql")

	// A plan file written before the engine was recorded is postgres.
	oldPlan := write(t, `{"version":5,"scope":{}}`)
	require.NoError(t, ValidateApplyFromEngine(&ApplyFromOptions{PlanFile: oldPlan, WithTx: true}))
	require.EqualError(t, ValidateApplyFromEngine(&ApplyFromOptions{PlanFile: oldPlan, DSQLNoWaitIndexBuild: true}), "--dsql-no-wait-index-build requires --engine dsql")

	unknownPlan := write(t, `{"version":5,"scope":{"engine":"mysql"}}`)
	require.EqualError(t, ValidateApplyFromEngine(&ApplyFromOptions{PlanFile: unknownPlan}), "plan file "+unknownPlan+`: unknown engine "mysql"`)

	require.ErrorContains(t, ValidateApplyFromEngine(&ApplyFromOptions{PlanFile: filepath.Join(t.TempDir(), "missing.json")}), "failed to read the plan file")
}

func TestValidateDumpEngine(t *testing.T) {
	require.NoError(t, ValidateDumpEngine(EnginePostgres, &DumpOptions{Explain: true}))
	require.EqualError(t, ValidateDumpEngine(EngineDSQL, &DumpOptions{Explain: true}), "--explain cannot be used with --engine dsql")
	require.NoError(t, ValidateDumpEngine(EngineDSQL, &DumpOptions{}))
}

// DSQL rejects default_transaction_read_only, so a read-only connection sets
// the session's transactions read-only once it is open. Local PostgreSQL
// takes the same statement.
func TestConnect_DSQLReadOnly(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{ConnString: testutil.ConnString(), Schemas: []string{"public"}, Engine: EngineDSQL})

	for readOnly, want := range map[bool]string{true: "on", false: "off"} {
		conn, err := client.connect(ctx, readOnly)
		require.NoError(t, err)
		var got string
		require.NoError(t, conn.QueryRow(ctx, "SHOW transaction_read_only").Scan(&got))
		assert.Equal(t, want, got)
		require.NoError(t, conn.Close(ctx))
	}
}
