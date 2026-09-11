// Command gen-json-schema writes the JSON Schema of the document pistachio
// writes in JSON. `make json-schema` runs it, and a test fails when the file
// on disk is not what it writes.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/winebarrel/pistachio/internal/jsonschema"
)

func main() {
	os.Exit(run(os.Stderr))
}

// run writes the schema where the package keeps it, relative to the working
// directory, and returns the exit code. Errors go to errOut.
func run(errOut io.Writer) int {
	b, err := jsonschema.Marshal()
	if err != nil {
		fmt.Fprintln(errOut, "pistachio:", err) //nolint:errcheck

		return 1
	}

	if err := os.WriteFile(jsonschema.Path, b, 0o644); err != nil { //nolint:gosec
		fmt.Fprintln(errOut, "pistachio:", err) //nolint:errcheck

		return 1
	}

	return 0
}
