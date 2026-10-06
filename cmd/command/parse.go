package command

import (
	"io"
	"strings"

	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/document"
)

type Parse struct {
	Files []string `arg:"" help:"Path to the schema SQL file(s)."`
	// Schemas is the one connection option parse reads. No database is opened,
	// but the parser qualifies an unqualified name with the first entry, the
	// same way plan and apply read their input.
	Schemas []string `short:"n" env:"PISTA_SCHEMAS" default:"public" help:"Schema to qualify unqualified names with. Only the first is used."`
}

// AfterApply trims each schema name the way the global option does.
func (cmd *Parse) AfterApply() error {
	trimSchemas(cmd.Schemas)
	return nil
}

// trimSchemas trims each schema name in place.
func trimSchemas(schemas []string) {
	for i, s := range schemas {
		schemas[i] = strings.TrimSpace(s)
	}
}

func (cmd *Parse) Run(w io.Writer) error {
	client := pistachio.NewClient(&pistachio.Options{Schemas: cmd.Schemas})

	result, err := client.ParseSchema(cmd.Files)
	if err != nil {
		return err
	}

	// parse qualifies names with the first schema only, so domains are looked
	// up there.
	return writeJSON(w, document.New(result, cmd.Schemas[:1]))
}
