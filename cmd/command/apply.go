package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/winebarrel/pistachio"
)

type Apply struct {
	pistachio.ApplyOptions
}

func (cmd *Apply) Run(ctx context.Context, client *pistachio.Client, w io.Writer) error {
	var buf bytes.Buffer
	result, err := client.Apply(ctx, &cmd.ApplyOptions, &buf)
	if err != nil {
		// Flush any partial output (e.g. transaction-state comments and SQL
		// that ran before the error) so the user can see what happened.
		w.Write(buf.Bytes()) //nolint:errcheck
		return err
	}

	if connInfo, err := client.ConnInfoComment(); err == nil {
		fmt.Fprintln(w, connInfo) //nolint:errcheck
	}

	fmt.Fprintf(w, "-- Apply to %s (%s)\n", result.Count.SchemaLabel(), result.Count.Summary()) //nolint:errcheck

	// Same ordering as Plan: executed SQL (incl. pre-SQL) first, then skipped
	// DROPs as comments. When nothing was applied, skipped DROPs precede
	// "-- No changes". The buffer may still hold output (e.g. --with-tx
	// transaction comments) even when no schema change was applied, so the
	// "-- No changes" and timing decisions are driven by result.Applied rather
	// than the buffer length.
	if !result.Applied {
		w.Write(buf.Bytes()) //nolint:errcheck
		if result.DisallowedDrops != "" {
			fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
		}
		fmt.Fprintln(w, "-- No changes") //nolint:errcheck
	} else {
		w.Write(buf.Bytes()) //nolint:errcheck
		if result.DisallowedDrops != "" {
			fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
		}
		// Only report the execution time when statements were applied. With no
		// changes there is nothing to time.
		fmt.Fprintf(w, "-- Apply finished in %s\n", result.Duration.Round(time.Millisecond)) //nolint:errcheck
	}

	return nil
}
