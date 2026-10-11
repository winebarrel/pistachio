package command_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/cmd/command"
)

// The hooks refuse what --engine dsql cannot use, and let the rest through.
func TestAfterApply_EngineDSQL(t *testing.T) {
	dsql := pistachio.Options{Engine: pistachio.EngineDSQL}
	postgres := pistachio.Options{Engine: pistachio.EnginePostgres}

	plan := &command.Plan{Options: dsql}
	require.NoError(t, plan.AfterApply())
	plan.Explain = true
	require.EqualError(t, plan.AfterApply(), "--explain cannot be used with --engine dsql")

	apply := &command.Apply{Options: dsql}
	require.NoError(t, apply.AfterApply())
	apply.WithTx = true
	require.EqualError(t, apply.AfterApply(), "--with-tx cannot be used with --engine dsql")

	apply = &command.Apply{Options: postgres}
	apply.DSQLNoWaitJob = true
	require.EqualError(t, apply.AfterApply(), "--dsql-no-wait-job requires --engine dsql")

	dump := &command.Dump{Options: dsql}
	require.NoError(t, dump.AfterApply())
	dump.Explain = true
	require.EqualError(t, dump.AfterApply(), "--explain cannot be used with --engine dsql")
}

// apply-from takes the engine from the plan file.
func TestApplyFrom_AfterApply_EngineDSQL(t *testing.T) {
	planFile := filepath.Join(t.TempDir(), "plan.json")
	require.NoError(t, os.WriteFile(planFile, []byte(`{"version":5,"scope":{"engine":"dsql"}}`), 0o644))

	cmd := &command.ApplyFrom{}
	cmd.PlanFile = planFile
	require.NoError(t, cmd.AfterApply())
	cmd.Exclusive = true
	require.EqualError(t, cmd.AfterApply(), "--exclusive cannot be used with --engine dsql")
}
