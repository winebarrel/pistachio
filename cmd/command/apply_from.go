package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/winebarrel/pistachio"
)

// ApplyFrom takes the connection and the flags that decide how the statements
// are run. Everything that decides what is read or what is run comes from the
// plan file, so those flags are not offered here: one given on the command
// line would either be ignored or turn the plan file's own scope into drift.
type ApplyFrom struct {
	pistachio.ConnOptions
	pistachio.ApplyFromOptions
}

func (cmd *ApplyFrom) Run(ctx context.Context, w io.Writer) error {
	client := pistachio.NewClient(&pistachio.Options{ConnOptions: cmd.ConnOptions})

	// The output is buffered until the apply is done, so the header can
	// carry the count. The line that says apply is waiting for another
	// exclusive apply has to reach the terminal while it waits.
	cmd.WaitWriter = w

	var buf bytes.Buffer
	result, err := client.ApplyFrom(ctx, &cmd.ApplyFromOptions, &buf)
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

	// Same ordering as apply: executed SQL first, then skipped DROPs as
	// comments. Both were decided when the plan file was written.
	w.Write(buf.Bytes()) //nolint:errcheck
	if result.Ignored != "" {
		fmt.Fprintln(w, result.Ignored) //nolint:errcheck
	}
	if result.DisallowedDrops != "" {
		fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
	}
	if !result.Applied {
		fmt.Fprintln(w, "-- No changes") //nolint:errcheck
	} else {
		fmt.Fprintf(w, "-- Apply finished in %s\n", result.Duration.Round(time.Millisecond)) //nolint:errcheck
	}

	return nil
}
