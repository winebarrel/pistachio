package command

import (
	"errors"
	"fmt"
	"io"

	"github.com/winebarrel/pistachio"
)

// ErrDiffChanges is returned by Diff.Run when --check is set and the diff
// contains executable DDL. main maps it to exit code 2. Suppressed drops
// alone do not trigger it.
var ErrDiffChanges = errors.New("diff contains changes")

type Diff struct {
	// Schemas is the one connection option diff reads. No database is opened,
	// but the parser qualifies an unqualified name with the first entry, and
	// only objects in these schemas are compared, the same way plan reads the
	// catalog.
	Schemas []string `short:"n" env:"PISTA_SCHEMAS" default:"public" help:"Schemas to compare. Unqualified names are qualified with the first."`
	pistachio.DiffOptions
	Check bool `env:"PISTA_CHECK" help:"Exit with code 2 when the diff contains executable changes."`
}

func (cmd *Diff) Run(w io.Writer) error {
	client := pistachio.NewClient(&pistachio.Options{Schemas: cmd.Schemas})

	result, err := client.Diff(&cmd.DiffOptions)
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "-- Diff for %s (%s)\n", result.Count.SchemaLabel(), result.Count.Summary()) //nolint:errcheck

	// Order matches plan: executable SQL first so it can be piped/copied as a
	// runnable script; skipped DROPs follow as informational comments. In the
	// no-SQL case, skipped DROPs come before "-- No changes" so the summary
	// line reads naturally at the end.
	if !result.HasChanges {
		if result.Ignored != "" {
			fmt.Fprintln(w, result.Ignored) //nolint:errcheck
		}
		if result.DisallowedDrops != "" {
			fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
		}
		fmt.Fprintln(w, "-- No changes") //nolint:errcheck
	} else {
		fmt.Fprintln(w, result.SQL) //nolint:errcheck
		if result.Ignored != "" {
			fmt.Fprintln(w, result.Ignored) //nolint:errcheck
		}
		if result.DisallowedDrops != "" {
			fmt.Fprintln(w, result.DisallowedDrops) //nolint:errcheck
		}
	}

	if cmd.Check && result.HasChanges {
		return ErrDiffChanges
	}

	return nil
}
