package command

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/winebarrel/pistachio"
)

type Fmt struct {
	pistachio.FmtOptions
}

func (cmd *Fmt) Run(client *pistachio.Client, w io.Writer) error {
	results, err := client.Format(&cmd.FmtOptions)
	if err != nil {
		return err
	}

	if cmd.Write {
		for path, content := range results {
			// Preserve original file permissions
			mode := os.FileMode(0o644)
			if info, err := os.Stat(path); err == nil {
				mode = info.Mode()
			}
			if err := os.WriteFile(path, []byte(content+"\n"), mode); err != nil {
				return fmt.Errorf("failed to write %s: %w", path, err)
			}
		}
		return nil
	}

	// Print all results to stdout
	var parts []string
	for _, path := range cmd.Files {
		parts = append(parts, results[path])
	}
	fmt.Fprintln(w, strings.Join(parts, "\n\n")) //nolint:errcheck
	return nil
}
