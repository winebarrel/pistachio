package command

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/winebarrel/pistachio"
)

type Apply struct {
	pistachio.ApplyOptions
}

func (cmd *Apply) Run(ctx context.Context, client *pistachio.Client, w io.Writer) error {
	var buf bytes.Buffer
	count, err := client.Apply(ctx, &cmd.ApplyOptions, &buf)
	if err != nil {
		return err
	}

	if buf.Len() == 0 {
		fmt.Fprintf(w, "-- No changes (%s)\n", count) //nolint:errcheck
	} else {
		w.Write(buf.Bytes()) //nolint:errcheck
	}

	return nil
}
