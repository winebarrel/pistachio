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

	// Deterministic sorts the keys of a Go map. Everything in the document is
	// an ordered map or a slice but for an enum's value_rename_from, which
	// would otherwise come out in a different order on every run and make two
	// runs over one schema differ.
	if err := json.MarshalWrite(w, result, jsontext.WithIndent("  "), json.Deterministic(true)); err != nil {
		return err
	}
	fmt.Fprintln(w) //nolint:errcheck

	return nil
}
