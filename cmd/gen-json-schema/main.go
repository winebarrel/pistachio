// Command gen-json-schema writes the JSON Schema of the document pistachio
// writes in JSON. `make json-schema` runs it, and a test fails when the file
// on disk is not what it writes.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/winebarrel/pistachio/internal/jsonschema"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run parses args and writes the schema, returning the exit code. The schema
// goes to out, or to the file -o names; errors go to errOut.
func run(args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("gen-json-schema", flag.ContinueOnError)
	fs.SetOutput(errOut)
	path := fs.String("o", "", "File to write the schema to. Writes to stdout when empty.")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	b, err := jsonschema.Marshal()
	if err != nil {
		fmt.Fprintln(errOut, "pistachio:", err) //nolint:errcheck

		return 1
	}

	if *path == "" {
		if _, err := out.Write(b); err != nil {
			fmt.Fprintln(errOut, "pistachio:", err) //nolint:errcheck

			return 1
		}

		return 0
	}

	if err := os.WriteFile(*path, b, 0o644); err != nil { //nolint:gosec
		fmt.Fprintln(errOut, "pistachio:", err) //nolint:errcheck

		return 1
	}

	return 0
}
