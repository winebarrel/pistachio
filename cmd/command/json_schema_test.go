package command_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
	pistaschema "github.com/winebarrel/pistachio/internal/jsonschema"
)

// validateAgainstJSONSchema runs a document through the schema as it is
// committed, which is what the documentation site carries.
func validateAgainstJSONSchema(t *testing.T, document []byte) error {
	t.Helper()

	f, err := os.Open(filepath.Join("..", "..", filepath.FromSlash(pistaschema.Path)))
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	doc, err := jsonschema.UnmarshalJSON(f)
	require.NoError(t, err)

	c := jsonschema.NewCompiler()
	require.NoError(t, c.AddResource(pistaschema.ID, doc))

	schema, err := c.Compile(pistaschema.ID)
	require.NoError(t, err)

	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(document))
	require.NoError(t, err)

	return schema.Validate(inst)
}
