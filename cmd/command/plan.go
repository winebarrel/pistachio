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
	plan, err := client.Plan(ctx, &cmd.PlanOptions)
	if err != nil {
		return err
	}

	if plan == "" {
		fmt.Fprintln(w, "-- No changes") //nolint:errcheck
	} else {
		fmt.Fprintln(w, plan) //nolint:errcheck
	}

	return nil
}
