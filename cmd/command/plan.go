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

	if result.SQL == "" {
		fmt.Fprintf(w, "-- No changes (%s)\n", result.Count) //nolint:errcheck
	} else {
		fmt.Fprintln(w, result.SQL) //nolint:errcheck
	}

	return nil
}
