package command

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"

	"github.com/winebarrel/pistachio/model"
)

// writeJSON writes v as the JSON document pistachio prints, and the newline a
// text file ends on. Deterministic sorts the keys of a Go map, without which
// an enum's value_rename_from would come out in a different order on every
// run. model.JSONMarshalers writes what the struct tags do not say by
// themselves. parse and dump share both so one schema describes what they
// write.
func writeJSON(w io.Writer, v any) error {
	if err := json.MarshalWrite(w, v, jsontext.WithIndent("  "), json.Deterministic(true), model.JSONMarshalers); err != nil {
		return err
	}

	fmt.Fprintln(w) //nolint:errcheck

	return nil
}
