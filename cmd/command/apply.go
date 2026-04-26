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
	result, err := client.Apply(ctx, &cmd.ApplyOptions, &buf)
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "-- Apply to %s (%s)\n", result.Count.SchemaLabel(), result.Count.Summary()) //nolint:errcheck

	// Same ordering as Plan: executed SQL (incl. pre-SQL) first, then skipped
	// DROPs as comments. When nothing was executed, skipped DROPs precede
	// "-- No changes".
	if buf.Len() == 0 {
		if result.DisallowedDrops != "" {
			fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
		}
		fmt.Fprintln(w, "-- No changes") //nolint:errcheck
	} else {
		w.Write(buf.Bytes()) //nolint:errcheck
		if result.DisallowedDrops != "" {
			fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
		}
	}

	return nil
}
