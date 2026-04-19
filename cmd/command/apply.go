package command

import (
	"context"
	"fmt"
	"io"

	"github.com/winebarrel/pistachio"
)

type Apply struct {
	pistachio.ApplyOptions
}

func (cmd *Apply) Run(ctx context.Context, client *pistachio.Client, w io.Writer) error {
	applied, err := client.Apply(ctx, &cmd.ApplyOptions, w)
	if err != nil {
		return err
	}

	if !applied {
		fmt.Fprintln(w, "No changes") //nolint:errcheck
	}

	return nil
}
