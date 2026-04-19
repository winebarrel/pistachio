package command

import (
	"context"
	"io"

	"github.com/winebarrel/pistachio"
)

type Apply struct {
	pistachio.ApplyOptions
}

func (cmd *Apply) Run(ctx context.Context, client *pistachio.Client, w io.Writer) error {
	return client.Apply(ctx, &cmd.ApplyOptions, w)
}
