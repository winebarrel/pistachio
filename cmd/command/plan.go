package command

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/winebarrel/pistachio"
)

// ErrPlanDiff is returned by Plan.Run when --check is set and the plan
// contains executable DDL. main maps it to exit code 2. Suppressed drops
// alone do not trigger it.
var ErrPlanDiff = errors.New("plan contains changes")

type Plan struct {
	pistachio.PlanOptions
	Check bool `env:"PISTA_CHECK" help:"Exit with code 2 when the plan contains executable changes."`
}

func (cmd *Plan) Run(ctx context.Context, client *pistachio.Client, w io.Writer) error {
	result, err := client.Plan(ctx, &cmd.PlanOptions)
	if err != nil {
		return err
	}

	if connInfo, err := client.ConnInfoComment(); err == nil {
		fmt.Fprintln(w, connInfo) //nolint:errcheck
	}

	fmt.Fprintf(w, "-- Plan for %s (%s)\n", result.Count.SchemaLabel(), result.Count.Summary()) //nolint:errcheck

	// Order: executable SQL (incl. pre-SQL) first so it can be piped/copied
	// as a runnable script; skipped DROPs follow as informational comments.
	// In the no-SQL case, skipped DROPs come before "-- No changes" so the
	// summary line reads naturally at the end.
	if !result.HasChanges {
		if result.Ignored != "" {
			fmt.Fprintln(w, result.Ignored) //nolint:errcheck
		}
		if result.DisallowedDrops != "" {
			fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
		}
		fmt.Fprintln(w, "-- No changes") //nolint:errcheck
	} else {
		fmt.Fprintln(w, result.SQL) //nolint:errcheck
		if result.Ignored != "" {
			fmt.Fprintln(w, result.Ignored) //nolint:errcheck
		}
		if result.DisallowedDrops != "" {
			fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
		}
	}

	if cmd.Check && result.HasChanges {
		return ErrPlanDiff
	}

	return nil
}
