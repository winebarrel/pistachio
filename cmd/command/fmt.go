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
			if err := os.WriteFile(path, []byte(content+"\n"), 0o644); err != nil {
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
