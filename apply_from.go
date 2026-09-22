package pistachio

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// driftWarning is what --force writes in place of the error, in the comment
// form the rest of the output takes.
const driftWarning = "-- Warning: the database has drifted since the plan was written"

type ApplyFromOptions struct {
	ExecOptions
	PlanFile string `arg:"" type:"path" help:"Path to the plan file written by plan --out."`
	// No env var: --force stands down the one check the plan file is for, and
	// a variable exported once in a CI environment would stand it down for
	// every run there. It is typed where it is meant.
	Force bool `help:"Apply the plan file even where the database has drifted since it was written. The drift is reported as a warning instead of an error."`
}

// ApplyFrom executes a plan file. The statements are the plan's, decided when
// it was written, so nothing is diffed here: the database is read only to
// check that it is still the one the plan was computed against.
//
// The scope comes from the file rather than from the command line. The state
// hash means nothing unless it is recomputed over the same read, and a flag
// that quietly narrowed the read here would report drift that is not there.
func (client *Client) ApplyFrom(ctx context.Context, options *ApplyFromOptions, w io.Writer) (*ApplyResult, error) {
	plan, err := readPlanFile(options.PlanFile)
	if err != nil {
		return nil, err
	}

	// The connection is this run's, everything else the plan file's.
	scoped := NewClient(&Options{ConnOptions: client.ConnOptions, ScopeOptions: plan.Scope})
	if err := scoped.validateSchemas(); err != nil {
		return nil, err
	}

	conn, err := scoped.connect(ctx, false)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx) //nolint:errcheck

	// Before the state is read, as apply does, so the plan is not checked
	// against a state another exclusive apply is still changing.
	if err := acquireExclusiveIfAsked(ctx, conn, &options.ExecOptions, w); err != nil {
		return nil, err
	}

	// The major version decides what the catalog reads and what DDL the server
	// takes, so a plan file does not travel between two of them. --force is
	// about drift and does not reach this.
	serverVersion, err := serverMajorVersion(ctx, conn)
	if err != nil {
		return nil, err
	}
	if serverVersion != plan.ServerVersion {
		return nil, fmt.Errorf(
			"plan file %s was written against PostgreSQL %d, and this server is %d: run plan again",
			options.PlanFile, plan.ServerVersion, serverVersion,
		)
	}

	current, err := scoped.currentState(ctx, conn)
	if err != nil {
		return nil, err
	}
	stateHash, err := current.stateHash()
	if err != nil {
		return nil, err
	}

	if stateHash != plan.StateHash {
		if !options.Force {
			return nil, fmt.Errorf(
				"the database has drifted since plan file %s was written: run plan again, or pass --force to apply it as it is",
				options.PlanFile,
			)
		}
		fmt.Fprintln(w, driftWarning) //nolint:errcheck
	}

	result := &ApplyResult{
		Count:           plan.Count,
		DisallowedDrops: strings.Join(plan.DisallowedDrops, "\n"),
		Ignored:         strings.Join(plan.Ignored, "\n"),
	}

	if err := scoped.applyStmts(ctx, conn, &applyInput{
		PreSQL:               plan.PreSQL,
		ConcurrentlyPreSQL:   plan.ConcurrentlyPreSQL,
		HasConcurrentlyIndex: plan.HasConcurrentlyIndex,
		Stmts:                plan.Stmts,
		ExecuteStmts:         plan.ExecuteStmts,
	}, &options.ExecOptions, w, result); err != nil {
		return nil, err
	}

	return result, nil
}
