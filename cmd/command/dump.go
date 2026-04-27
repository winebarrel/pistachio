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

	for name, content := range result.Files() {
		path := filepath.Join(cmd.Split, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return fmt.Errorf("failed to write %s: %w", path, err)
		}
		fmt.Fprintln(w, path) //nolint:errcheck
	}

	return nil
}
