// Command gen-json-schema writes the JSON Schema of the `pista parse`
// document. `make json-schema` runs it, and a test fails when the file on disk
// is not what it writes.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/winebarrel/pistachio/internal/jsonschema"
)

func main() {
	out := flag.String("o", "", "File to write the schema to. Writes to stdout when empty.")
	flag.Parse()

	b, err := jsonschema.Marshal()
	if err != nil {
		fmt.Fprintln(os.Stderr, "pistachio:", err) //nolint:errcheck
		os.Exit(1)
	}

	if *out == "" {
		os.Stdout.Write(b) //nolint:errcheck

		return
	}

	if err := os.WriteFile(*out, b, 0o644); err != nil { //nolint:gosec
		fmt.Fprintln(os.Stderr, "pistachio:", err) //nolint:errcheck
		os.Exit(1)
	}
}
