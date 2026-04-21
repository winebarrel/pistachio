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
	defaultSchema := "public"
	if len(client.Schemas) > 0 {
		defaultSchema = client.Schemas[0]
	}

	result, err := pistachio.FmtSQL(&cmd.FmtOptions, defaultSchema)
	if err != nil {
		return err
	}

	if cmd.Write {
		content := result + "\n"
		for _, path := range cmd.Files {
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				return fmt.Errorf("failed to write %s: %w", path, err)
			}
		}
		return nil
	}

	fmt.Fprintln(w, result) //nolint:errcheck
	return nil
}
