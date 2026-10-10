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
	pistachio.Options
	pistachio.ApplyOptions
}

func (cmd *Apply) Run(ctx context.Context, w io.Writer) error {
	client := pistachio.NewClient(&cmd.Options)

	// The output is buffered until the apply is done, so the header can
	// carry the count. The line that says apply is waiting for another
	// exclusive apply has to reach the terminal while it waits.
	cmd.WaitWriter = w

	var buf bytes.Buffer
	result, err := client.Apply(ctx, &cmd.ApplyOptions, &buf)
	if err != nil {
		// Flush any partial output (e.g. transaction-state comments and SQL
		// that ran before the error) so the user can see what happened.
		w.Write(buf.Bytes()) //nolint:errcheck
		return err
	}

	writeApplyResult(w, client, result, buf.Bytes())
	return nil
}

// writeApplyResult prints the connection line, the header, the output apply
// buffered and the closing line.
func writeApplyResult(w io.Writer, client *pistachio.Client, result *pistachio.ApplyResult, out []byte) {
	writeHeader(w, client, "Apply to", result.Count)

	// The invalid-index warnings come first, as in plan.
	if result.InvalidIndexes != "" {
		fmt.Fprintln(w, result.InvalidIndexes) //nolint:errcheck
	}

	// Same ordering as Plan: executed SQL (incl. pre-SQL) first, then skipped
	// DROPs as comments. When nothing was applied, skipped DROPs precede
	// "-- No changes". The buffer may still hold output (e.g. --with-tx
	// transaction comments) even when no schema change was applied, so the
	// "-- No changes" and timing decisions are driven by result.Applied rather
	// than the buffer length.
	w.Write(out) //nolint:errcheck
	if result.Ignored != "" {
		fmt.Fprintln(w, result.Ignored) //nolint:errcheck
	}
	if result.DisallowedDrops != "" {
		fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
	}
	if !result.Applied {
		fmt.Fprintln(w, "-- No changes") //nolint:errcheck
	} else {
		// Only report the apply phase duration when statements were applied.
		// With no changes there is nothing to time.
		fmt.Fprintf(w, "-- Apply finished in %s\n", result.Duration.Round(time.Millisecond)) //nolint:errcheck
	}
}
