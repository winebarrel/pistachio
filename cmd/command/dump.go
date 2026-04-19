package command

import (
	"context"
	"fmt"
	"io"

	"github.com/winebarrel/pistachio"
)

type Dump struct {
	pistachio.DumpOptions
}

func (cmd *Dump) Run(ctx context.Context, client *pistachio.Client, w io.Writer) error {
	dump, err := client.Dump(ctx, &cmd.DumpOptions)
	if err != nil {
		return err
	}

	fmt.Fprintln(w, dump) //nolint:errcheck

	return nil
}
