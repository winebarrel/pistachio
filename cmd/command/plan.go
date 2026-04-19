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

	fmt.Fprintln(w, plan) //nolint:errcheck

	return nil
}
