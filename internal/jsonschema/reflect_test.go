package jsonschema_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/pistachio/internal/jsonschema"
)

// repoFile resolves a path against the repository root, which is two
// directories above this package.
func repoFile(t *testing.T, path string) string {
	t.Helper()

	return filepath.Join("..", "..", filepath.FromSlash(path))
}

// TestMarshal_MatchesCommittedFile fails when the schema on disk is not what
// the generator writes, which is the whole point of committing it: run
// `make json-schema`.
func TestMarshal_MatchesCommittedFile(t *testing.T) {
	want, err := os.ReadFile(repoFile(t, jsonschema.Path))
	require.NoError(t, err, "run: make json-schema")

	got, err := jsonschema.Marshal()
	require.NoError(t, err)

	assert.Equal(t, string(want), string(got), "%s is out of date: run `make json-schema`", jsonschema.Path)
}

// The published file names the version it is, so neither can move without the
// other.
func TestID_NamesTheFile(t *testing.T) {
	assert.Contains(t, jsonschema.ID, "schema-"+jsonschema.Version+".json")
	assert.Contains(t, jsonschema.Path, "schema-"+jsonschema.Version+".json")
}

func TestBuild(t *testing.T) {
	schema, err := jsonschema.Build()
	require.NoError(t, err)

	assert.Equal(t, jsonschema.ID, schema.ID.String())
	assert.NotEmpty(t, schema.Title)

	// Every object kind the parser reports is a property of the document.
	for _, name := range []string{
		"tables", "views", "enums", "domains", "composite_types", "sequences",
		"routines", "execute_stmts",
	} {
		_, ok := schema.Properties.Get(name)
		assert.True(t, ok, "missing property %q", name)
	}

	// Each is described by a definition rather than inlined.
	for _, name := range []string{
		"Table", "Column", "Constraint", "ForeignKey", "Index", "Policy",
		"Trigger", "View", "Enum", "Domain", "CompositeType", "Sequence",
		"Routine",
	} {
		assert.Contains(t, schema.Definitions, name)
	}
}

// A table's columns are written as an array, in the physical column order.
// Everything else an ordered map holds stays an object keyed by name.
func TestBuild_ColumnsAreAnArray(t *testing.T) {
	schema, err := jsonschema.Build()
	require.NoError(t, err)

	table, ok := schema.Definitions["Table"]
	require.True(t, ok)

	columns, ok := table.Properties.Get("columns")
	require.True(t, ok)
	require.Len(t, columns.OneOf, 2, "a pointer property is a choice of the type and null")
	assert.Equal(t, "array", columns.OneOf[0].Type)
	require.NotNil(t, columns.OneOf[0].Items)
	assert.Equal(t, "#/$defs/Column", columns.OneOf[0].Items.Ref)

	indexes, ok := table.Properties.Get("indexes")
	require.True(t, ok)
	require.Len(t, indexes.OneOf, 2)
	assert.Equal(t, "object", indexes.OneOf[0].Type)
	require.NotNil(t, indexes.OneOf[0].AdditionalProperties)
	assert.Equal(t, "#/$defs/Index", indexes.OneOf[0].AdditionalProperties.Ref)
}

// The words a byte enum marshals as are read off the marshaler, so the schema
// cannot name a value the output never holds, or miss one it does.
func TestBuild_EnumValues(t *testing.T) {
	schema, err := jsonschema.Build()
	require.NoError(t, err)

	con, ok := schema.Definitions["Constraint"]
	require.True(t, ok)
	typ, ok := con.Properties.Get("type")
	require.True(t, ok)

	assert.ElementsMatch(t,
		[]any{"", "check", "foreign_key", "not_null", "primary_key", "unique", "exclusion"},
		typ.Enum)
}

// A pointer field reaches the reader as null when it is unset, so the schema
// has to accept null beside the type it points at.
func TestBuild_PointerFieldsAcceptNull(t *testing.T) {
	schema, err := jsonschema.Build()
	require.NoError(t, err)

	col, ok := schema.Definitions["Column"]
	require.True(t, ok)

	prop, ok := col.Properties.Get("default")
	require.True(t, ok)
	require.Len(t, prop.OneOf, 2, "a *string property is a choice of the type and null")
	assert.Equal(t, "string", prop.OneOf[0].Type)
	assert.Equal(t, "null", prop.OneOf[1].Type)

	// A field that is not a pointer is left alone.
	notNull, ok := col.Properties.Get("not_null")
	require.True(t, ok)
	assert.Empty(t, notNull.OneOf)
	assert.Equal(t, "boolean", notNull.Type)
}
