package command

import (
	"io"

	"github.com/winebarrel/pistachio"
)

type Parse struct {
	Files []string `arg:"" help:"Path to the schema SQL file(s)."`
	// Schemas is the one connection option parse reads. No database is opened,
	// but the parser qualifies an unqualified name with the first entry, the
	// same way plan and apply read their input.
	Schemas []string `short:"n" env:"PISTA_SCHEMAS" default:"public" help:"Schema to qualify unqualified names with. Only the first is used."`
}

func (cmd *Parse) Run(w io.Writer) error {
	client := pistachio.NewClient(&pistachio.Options{Schemas: cmd.Schemas})

	result, err := client.ParseSchema(cmd.Files)
	if err != nil {
		return err
	}

	return writeJSON(w, result)
}
