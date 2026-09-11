package command

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
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

	if err := json.MarshalWrite(w, result, jsontext.WithIndent("  ")); err != nil {
		return err
	}
	fmt.Fprintln(w) //nolint:errcheck

	return nil
}
