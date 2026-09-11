package jsonschema

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/invopop/jsonschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// word marshals as a string, the way the model's byte enums do.
type word byte

func (w word) MarshalJSON() ([]byte, error) {
	if w == 1 {
		return []byte(`"one"`), nil
	}

	return []byte(`""`), nil
}

// number is byte-backed but marshals as a number, so it is not one of the
// enums.
type number byte

func (n number) MarshalJSON() ([]byte, error) {
	return json.Marshal(int(n))
}

// broken fails to marshal.
type broken byte

func (broken) MarshalJSON() ([]byte, error) {
	return nil, errors.New("nope")
}

// plain is byte-backed with no marshaler of its own.
type plain byte

func TestEnumValues(t *testing.T) {
	assert.Equal(t, []string{"", "one"}, enumValues(reflect.TypeFor[word]()))
	assert.Equal(t, []string{"check", "foreign_key", "not_null", "primary_key", "unique", "exclusion"},
		enumValues(reflect.TypeFor[model.ConstraintType]())[1:])
}

func TestEnumValues_NotAnEnum(t *testing.T) {
	assert.Nil(t, enumValues(reflect.TypeFor[number]()), "marshals as a number")
	assert.Nil(t, enumValues(reflect.TypeFor[broken]()), "fails to marshal")
	assert.Nil(t, enumValues(reflect.TypeFor[plain]()), "has no marshaler")
	assert.Nil(t, enumValues(reflect.TypeFor[byte]()), "is not a named type")
	assert.Nil(t, enumValues(reflect.TypeFor[string]()), "is not byte-backed")
}

func TestOrderedMapValue(t *testing.T) {
	omType := deref(reflect.TypeOf(orderedmap.New[string, *model.Column]()))
	value, ok := orderedMapValue(omType)
	require.True(t, ok)
	assert.Equal(t, reflect.TypeFor[*model.Column](), value)

	scalar := deref(reflect.TypeOf(orderedmap.New[string, string]()))
	value, ok = orderedMapValue(scalar)
	require.True(t, ok)
	assert.Equal(t, reflect.TypeFor[string](), value)
}

func TestOrderedMapValue_OtherTypes(t *testing.T) {
	_, ok := orderedMapValue(reflect.TypeFor[model.Column]())
	assert.False(t, ok)

	_, ok = orderedMapValue(reflect.TypeFor[string]())
	assert.False(t, ok, "not a struct")
}

func TestRefFor(t *testing.T) {
	assert.Equal(t, "#/$defs/Column", refFor(reflect.TypeFor[*model.Column]()).Ref)
	assert.Equal(t, "string", refFor(reflect.TypeFor[string]()).Type)
}

func TestJSONType(t *testing.T) {
	assert.Equal(t, "string", jsonType(reflect.TypeFor[string]()))
	assert.Equal(t, "boolean", jsonType(reflect.TypeFor[bool]()))
	assert.Equal(t, "number", jsonType(reflect.TypeFor[float64]()))
	assert.Equal(t, "number", jsonType(reflect.TypeFor[float32]()))
	assert.Equal(t, "integer", jsonType(reflect.TypeFor[int64]()))
	assert.Equal(t, "integer", jsonType(reflect.TypeFor[uint32]()))
}

func TestJSONName(t *testing.T) {
	type s struct {
		Named    string `json:"named"`
		Options  string `json:"options,omitempty"`
		Excluded string `json:"-"`
		Untagged string
		hidden   string //nolint:unused
	}

	fields := map[string]reflect.StructField{}
	for _, f := range reflect.VisibleFields(reflect.TypeFor[s]()) {
		fields[f.Name] = f
	}

	name, ok := jsonName(fields["Named"])
	assert.True(t, ok)
	assert.Equal(t, "named", name)

	name, ok = jsonName(fields["Options"])
	assert.True(t, ok)
	assert.Equal(t, "options", name, "the options after the name are not part of it")

	_, ok = jsonName(fields["Excluded"])
	assert.False(t, ok)

	_, ok = jsonName(fields["Untagged"])
	assert.False(t, ok)

	_, ok = jsonName(fields["hidden"])
	assert.False(t, ok)
}

func TestDeref(t *testing.T) {
	assert.Equal(t, reflect.TypeFor[string](), deref(reflect.TypeFor[**string]()))
	assert.Equal(t, reflect.TypeFor[string](), deref(reflect.TypeFor[string]()))
}

func TestDefinitionTypes(t *testing.T) {
	type root struct {
		A *model.Column                           `json:"a"`
		B *orderedmap.Map[string, *model.Trigger] `json:"b"`
		C []*model.DomainConstraint               `json:"c"`
		D string                                  `json:"d"`
		E *model.Column                           `json:"-"`
	}

	types, err := definitionTypes(reflect.TypeFor[root]())
	require.NoError(t, err)

	// A field is followed through a pointer, an ordered map and a slice.
	assert.Contains(t, types, "Column")
	assert.Contains(t, types, "Trigger")
	assert.Contains(t, types, "DomainConstraint")
	assert.Contains(t, types, "root")

	// A scalar is not a definition, and neither is a field that is not written.
	assert.NotContains(t, types, "string")
}

// The definitions are keyed by name alone, so two types of the same name
// cannot both be written.
func TestDefinitionTypes_DuplicateName(t *testing.T) {
	type Column struct {
		N int `json:"n"`
	}
	type root struct {
		Mine  *Column       `json:"mine"`
		Model *model.Column `json:"model"`
	}

	_, err := definitionTypes(reflect.TypeFor[root]())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `two types named "Column"`)
}

// markStruct reports a property the schema does not carry rather than
// silently leaving it alone.
func TestMarkStruct_MissingProperty(t *testing.T) {
	schema := &jsonschema.Schema{Properties: jsonschema.NewProperties()}
	schema.Properties.Set("name", &jsonschema.Schema{Type: "string"})

	err := markStruct(schema, reflect.TypeFor[model.Column]())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no property")
}

// A schema with no properties at all is left alone: there is nothing to widen.
func TestMarkStruct_NoProperties(t *testing.T) {
	require.NoError(t, markStruct(&jsonschema.Schema{}, reflect.TypeFor[model.Column]()))
}
