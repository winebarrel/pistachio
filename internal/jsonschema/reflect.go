// Package jsonschema builds the JSON Schema of the document `pista parse`
// writes. It reflects over parser.ParseResult, so the schema follows the
// structs rather than a description kept beside them.
package jsonschema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/invopop/jsonschema"
	"github.com/winebarrel/pistachio/parser"
)

// Version is the version the schema's file name and URL carry.
const Version = "1.0"

// Path is where the file sits in the repository and on the documentation site.
const Path = "docs/json/schema-" + Version + ".json"

// ID is the URL the documentation site carries the schema at.
const ID = "https://winebarrel.github.io/pistachio/json/schema-" + Version + ".json"

const (
	title       = "pistachio schema document"
	description = "The PostgreSQL schema objects pistachio manages, as it writes them in JSON."
)

// Marshal returns the schema as the file holds it: indented, and with the
// trailing newline a text file ends on.
func Marshal() ([]byte, error) {
	schema, err := Build()
	if err != nil {
		return nil, err
	}

	b, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(b, '\n'), nil
}

// Build returns the schema of a `pista parse` document.
func Build() (*jsonschema.Schema, error) {
	r := &jsonschema.Reflector{
		// The document is the ParseResult itself, not a $ref to it.
		ExpandedStruct: true,
		// A field absent from the JSON is not a thing the output has: every
		// field is written, so every field is required.
		RequiredFromJSONSchemaTags: false,
	}
	r.Mapper = mapper

	root := reflect.TypeFor[parser.ParseResult]()

	schema := r.Reflect(&parser.ParseResult{})
	schema.ID = ID
	schema.Title = title
	schema.Description = description

	// The mapper answers for an ordered map with a $ref, so the reflector
	// never walks into the type it holds and never writes its definition.
	// Reflect each reachable type and collect what they define. This one
	// leaves the type it is given in the definitions rather than expanding it
	// into the document.
	defs := &jsonschema.Reflector{Mapper: mapper}
	if err := addDefinitions(defs, schema, root); err != nil {
		return nil, err
	}

	if err := markNullable(schema, root); err != nil {
		return nil, err
	}

	return schema, nil
}

// addDefinitions writes a definition for every struct reachable from root that
// the reflector left out.
func addDefinitions(r *jsonschema.Reflector, schema *jsonschema.Schema, root reflect.Type) error {
	types, err := definitionTypes(root)
	if err != nil {
		return err
	}

	if schema.Definitions == nil {
		schema.Definitions = jsonschema.Definitions{}
	}

	for name, t := range types {
		if t == root {
			continue
		}

		for defName, def := range r.ReflectFromType(t).Definitions {
			if _, ok := schema.Definitions[defName]; !ok {
				schema.Definitions[defName] = def
			}
		}

		if _, ok := schema.Definitions[name]; !ok {
			return fmt.Errorf("jsonschema: %s produced no definition named %q", t, name)
		}
	}

	return nil
}

// mapper answers for the types reflection alone reads wrong: a byte that
// marshals as a word, and the ordered map, whose entries live in unexported
// fields.
func mapper(t reflect.Type) *jsonschema.Schema {
	if values := enumValues(t); values != nil {
		return &jsonschema.Schema{Type: "string", Enum: toAny(values)}
	}

	if valueType, ok := orderedMapValue(t); ok {
		value, err := refFor(valueType)
		if err != nil {
			// The mapper cannot fail, so a value type it does not know is
			// left to the reflector, which describes it as best it can. Build
			// catches it: the definition the $ref would have named is missing.
			return nil
		}

		return &jsonschema.Schema{Type: "object", AdditionalProperties: value}
	}

	return nil
}

// enumValues returns the words a byte-backed MarshalJSON can write, asked of
// it for every value the byte holds, and nil for any other type. Deriving them
// keeps the schema and the marshaler from drifting apart.
func enumValues(t reflect.Type) []string {
	if t.Kind() != reflect.Uint8 || t.PkgPath() == "" {
		return nil
	}

	seen := map[string]bool{}
	values := []string{}

	for i := range 256 {
		v := reflect.New(t).Elem()
		v.SetUint(uint64(i))

		m, ok := reflect.TypeAssert[json.Marshaler](v)
		if !ok {
			return nil
		}

		b, err := m.MarshalJSON()
		if err != nil {
			return nil
		}

		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			// The type marshals as something other than a string, so it is
			// not one of the words.
			return nil
		}

		if !seen[s] {
			seen[s] = true
			values = append(values, s)
		}
	}

	return values
}

// orderedMapValue reports the value type of an orderedmap.Map. The entries sit
// in unexported fields, so the type argument is read off the map that indexes
// them rather than from a field of the value type.
func orderedMapValue(t reflect.Type) (reflect.Type, bool) {
	if t.Kind() != reflect.Struct || !strings.HasPrefix(t.String(), "orderedmap.Map[") {
		return nil, false
	}

	f, ok := t.FieldByName("elementByKey")
	if !ok {
		return nil, false
	}

	// map[K]*linkedlist.Element[*orderedmap.Pair[K, V]]
	element := deref(f.Type.Elem())

	held, ok := element.FieldByName("Value")
	if !ok {
		return nil, false
	}

	pair := deref(held.Type)

	value, ok := pair.FieldByName("Value")
	if !ok {
		return nil, false
	}

	return value.Type, true
}

// deref strips the pointers off a type.
func deref(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	return t
}

// refFor names the definition of a type the way the reflector does, or the
// JSON type a scalar is written as.
func refFor(t reflect.Type) (*jsonschema.Schema, error) {
	t = deref(t)

	if t.Kind() == reflect.Struct {
		return &jsonschema.Schema{Ref: "#/$defs/" + t.Name()}, nil
	}

	name, err := jsonType(t)
	if err != nil {
		return nil, err
	}

	return &jsonschema.Schema{Type: name}, nil
}

// jsonType is the JSON type a Go scalar is written as. A kind it does not name
// is an error rather than a guess: calling it an integer would put a wrong
// type in a published schema, which is worse than failing the generator.
func jsonType(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.String:
		return "string", nil
	case reflect.Bool:
		return "boolean", nil
	case reflect.Float32, reflect.Float64:
		return "number", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer", nil
	default:
		return "", fmt.Errorf("jsonschema: no JSON type for %s", t)
	}
}

func toAny(values []string) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = v
	}

	return out
}
