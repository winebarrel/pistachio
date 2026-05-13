package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

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

	if cmd.Split == "" {
		fmt.Fprintf(w, "-- Dump of %s (%s)\n", result.Count.SchemaLabel(), result.Count.Summary()) //nolint:errcheck
		fmt.Fprintln(w, result)                                                                    //nolint:errcheck
		return nil
	}

	if err := os.MkdirAll(cmd.Split, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	fmt.Fprintf(w, "-- Dump of %s (%s)\n", result.Count.SchemaLabel(), result.Count.Summary()) //nolint:errcheck

	count, err := writeDumpFiles(cmd.Split, result.Files())
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "-- Wrote %d file(s) to %s\n", count, cmd.Split) //nolint:errcheck
	return nil
}

// writeDumpFiles writes each entry of files into dir. Names must satisfy two
// guards before they are written:
//   - filepath.IsLocal, to reject hostile PostgreSQL identifiers (quoted names
//     containing "/", "..", absolute paths, etc.) that would otherwise escape
//     the target directory via filepath.Join.
//   - Canonical form (name == filepath.Clean(name)), so that the map key
//     returned by DumpResult.Files() matches the on-disk filename. Without
//     this a name like "foo/../bar.sql" would Clean to "bar.sql" inside Join
//     and could silently collide with a sibling "bar.sql" entry whose dedup
//     check ran on the original map key.
func writeDumpFiles(dir string, files map[string]string) (int, error) {
	count := 0
	for name, content := range files {
		if !filepath.IsLocal(name) || name != filepath.Clean(name) {
			return count, fmt.Errorf("refusing to write dump file with unsafe or non-canonical name %q: would escape or alias the --split directory", name)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return count, fmt.Errorf("failed to write %s: %w", path, err)
		}
		count++
	}
	return count, nil
}
