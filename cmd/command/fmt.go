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

// ErrNotFormatted is returned when --check finds unformatted files.
type ErrNotFormatted struct {
	Files []string
}

func (e *ErrNotFormatted) Error() string {
	return fmt.Sprintf("files not formatted: %s", strings.Join(e.Files, ", "))
}

func (cmd *Fmt) Run(client *pistachio.Client, w io.Writer) error {
	results, err := client.Format(&cmd.FmtOptions)
	if err != nil {
		return err
	}

	if cmd.Check {
		var unformatted []string
		for _, path := range cmd.Files {
			original, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read %s: %w", path, err)
			}
			expected := results[path] + "\n"
			if string(original) != expected {
				unformatted = append(unformatted, path)
			}
		}
		if len(unformatted) > 0 {
			for _, path := range unformatted {
				fmt.Fprintln(w, path) //nolint:errcheck
			}
			return &ErrNotFormatted{Files: unformatted}
		}
		return nil
	}

	if cmd.Write {
		for _, path := range cmd.Files {
			content := results[path]
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
