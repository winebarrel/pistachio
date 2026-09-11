package command

import (
	"io"

	"github.com/winebarrel/pistachio"
)

type Parse struct {
	Files []string `arg:"" help:"Path to the schema SQL file(s)."`
}

func (cmd *Parse) Run(client *pistachio.Client, w io.Writer) error {
	result, err := client.ParseSchema(cmd.Files)
	if err != nil {
		return err
	}

	return writeJSON(w, result)
}
