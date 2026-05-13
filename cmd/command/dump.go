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

// writeDumpFiles writes each entry of files into dir. Names are validated with
// filepath.IsLocal so that hostile PostgreSQL identifiers (quoted names that
// contain "/", "..", absolute paths, etc.) cannot escape the target directory.
func writeDumpFiles(dir string, files map[string]string) (int, error) {
	count := 0
	for name, content := range files {
		if !filepath.IsLocal(name) {
			return count, fmt.Errorf("refusing to write dump file with unsafe name %q: would escape --split directory", name)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return count, fmt.Errorf("failed to write %s: %w", path, err)
		}
		count++
	}
	return count, nil
}
