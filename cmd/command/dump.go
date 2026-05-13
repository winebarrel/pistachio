package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/winebarrel/pistachio"
)

type Dump struct {
	pistachio.DumpOptions
}

func (cmd *Dump) Run(ctx context.Context, client *pistachio.Client, w io.Writer) error {
	result, err := client.Dump(ctx, &cmd.DumpOptions)
	if err != nil {
		return err
	}

	connInfo, _ := client.ConnInfoComment()

	if cmd.Split == "" {
		if connInfo != "" {
			fmt.Fprintln(w, connInfo) //nolint:errcheck
		}
		fmt.Fprintf(w, "-- Dump of %s (%s)\n", result.Count.SchemaLabel(), result.Count.Summary()) //nolint:errcheck
		fmt.Fprintln(w, result)                                                                    //nolint:errcheck
		return nil
	}

	if err := os.MkdirAll(cmd.Split, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if connInfo != "" {
		fmt.Fprintln(w, connInfo) //nolint:errcheck
	}
	fmt.Fprintf(w, "-- Dump of %s (%s)\n", result.Count.SchemaLabel(), result.Count.Summary()) //nolint:errcheck

	count, err := writeDumpFiles(cmd.Split, result.Files())
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "-- Wrote %d file(s) to %s\n", count, cmd.Split) //nolint:errcheck
	return nil
}

// writeDumpFiles writes each entry of files into dir. Names are required to
// be flat basenames sitting directly under dir, so three guards are applied:
//   - No path separator ("/" or "\") may appear in the name. DumpResult.Files()
//     never produces nested names from real PostgreSQL identifiers; a separator
//     would only come from a hostile quoted identifier such as "subdir/foo",
//     and allowing it would let a pre-existing symlink under dir redirect the
//     write outside dir.
//   - filepath.IsLocal must accept the name, which forbids "..", absolute
//     paths, empty strings, and (on Windows) reserved names.
//   - The name must already be in Clean form so the on-disk filename matches
//     the map key returned by DumpResult.Files() — otherwise a non-canonical
//     entry could silently alias a sibling whose dedup check ran on the
//     unreduced key.
func writeDumpFiles(dir string, files map[string]string) (int, error) {
	count := 0
	for name, content := range files {
		if strings.ContainsAny(name, `/\`) || !filepath.IsLocal(name) || name != filepath.Clean(name) {
			return count, fmt.Errorf("refusing to write dump file with unsafe name %q: name must be a flat basename under --split", name)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return count, fmt.Errorf("failed to write %s: %w", path, err)
		}
		count++
	}
	return count, nil
}
