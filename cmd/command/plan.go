package command

import (
	"context"
	"fmt"
	"io"

	"github.com/winebarrel/pistachio"
)

type Plan struct {
	pistachio.PlanOptions
}

func (cmd *Plan) Run(ctx context.Context, client *pistachio.Client, w io.Writer) error {
	result, err := client.Plan(ctx, &cmd.PlanOptions)
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "-- Plan for %s (%s)\n", result.Count.SchemaLabel(), result.Count.Summary()) //nolint:errcheck

	// Order: executable SQL (incl. pre-SQL) first so it can be piped/copied
	// as a runnable script; skipped DROPs follow as informational comments.
	// In the no-SQL case, skipped DROPs come before "-- No changes" so the
	// summary line reads naturally at the end.
	if result.SQL == "" {
		if result.DisallowedDrops != "" {
			fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
		}
		fmt.Fprintln(w, "-- No changes") //nolint:errcheck
	} else {
		fmt.Fprintln(w, result.SQL) //nolint:errcheck
		if result.DisallowedDrops != "" {
			fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
		}
	}

	return nil
}
