package command

import (
	"fmt"
	"io"
	"os"

	"github.com/winebarrel/pistachio"
)

type Fmt struct {
	pistachio.FmtOptions
}

func (cmd *Fmt) Run(client *pistachio.Client, w io.Writer) error {
	if cmd.Write {
		// Format each file individually and write back
		for _, path := range cmd.Files {
			result, err := client.FormatFile(path)
			if err != nil {
				return err
			}
			if err := os.WriteFile(path, []byte(result+"\n"), 0o644); err != nil {
				return fmt.Errorf("failed to write %s: %w", path, err)
			}
		}
		return nil
	}

	result, err := client.Format(&cmd.FmtOptions)
	if err != nil {
		return err
	}

	fmt.Fprintln(w, result) //nolint:errcheck
	return nil
}
