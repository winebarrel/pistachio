package command

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/winebarrel/pistachio"
	"github.com/winebarrel/pistachio/lint"
)

// ErrLintViolations is returned by Lint.Run when a rule is false for an
// object. The violations have been written already.
var ErrLintViolations = errors.New("lint rules violated")

type Lint struct {
	Files []string `arg:"" help:"Path to the schema SQL file(s)."`
	Rules []string `short:"r" required:"" type:"path" env:"PISTA_LINT_RULES" help:"Rule file, or directory of .yml and .yaml rule files (can be repeated)."`
	// Schemas is read as parse reads it: the parser qualifies an unqualified
	// name with the first entry.
	Schemas []string `short:"n" env:"PISTA_SCHEMAS" default:"public" help:"Schema to qualify unqualified names with. Only the first is used."`
}

// AfterApply trims each schema name the way the global option does.
func (cmd *Lint) AfterApply() error {
	trimSchemas(cmd.Schemas)
	return nil
}

func (cmd *Lint) Run(w io.Writer) error {
	linter, err := lint.Load(cmd.Rules, os.Stderr)
	if err != nil {
		return err
	}

	client := pistachio.NewClient(&pistachio.Options{Schemas: cmd.Schemas})
	result, err := client.ParseSchema(cmd.Files)
	if err != nil {
		return err
	}

	violations, err := linter.Run(result, cmd.Schemas[:1])
	if err != nil {
		return err
	}

	for _, v := range violations {
		fmt.Fprintln(w, v) //nolint:errcheck
	}
	if len(violations) > 0 {
		return ErrLintViolations
	}

	return nil
}
