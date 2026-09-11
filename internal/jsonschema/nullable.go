package jsonschema

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/invopop/jsonschema"
)

// markNullable widens every property whose Go field is a pointer to accept
// null. A field that holds its zero value is written rather than left out, so
// an unset pointer reaches the reader as null and a schema that named only the
// pointed-to type would reject the output.
//
// The walk starts at the root struct and follows the definitions the reflector
// wrote, so a type reached only through an ordered map is covered as well.
func markNullable(schema *jsonschema.Schema, root reflect.Type) error {
	types, err := definitionTypes(root)
	if err != nil {
		return err
	}

	// The root is expanded into the document itself rather than a definition.
	if err := markStruct(schema, root); err != nil {
		return err
	}

	for name, def := range schema.Definitions {
		t, ok := types[name]
		if !ok {
			return fmt.Errorf("jsonschema: no Go type for definition %q", name)
		}
		if err := markStruct(def, t); err != nil {
			return err
		}
	}

	return nil
}

// markStruct widens the properties of one struct's schema.
func markStruct(schema *jsonschema.Schema, t reflect.Type) error {
	if schema.Properties == nil {
		return nil
	}

	for _, f := range reflect.VisibleFields(t) {
		if f.Type.Kind() != reflect.Pointer {
			continue
		}

		name, ok := jsonName(f)
		if !ok {
			continue
		}

		prop, ok := schema.Properties.Get(name)
		if !ok {
			return fmt.Errorf("jsonschema: %s has no property %q", t.Name(), name)
		}

		schema.Properties.Set(name, nullable(prop))
	}

	return nil
}

// nullable returns a schema that accepts what prop accepts, and null.
func nullable(prop *jsonschema.Schema) *jsonschema.Schema {
	return &jsonschema.Schema{
		OneOf: []*jsonschema.Schema{prop, {Type: "null"}},
	}
}

// definitionTypes maps the name the reflector gives a definition to the Go
// type behind it, for every struct reachable from root.
func definitionTypes(root reflect.Type) (map[string]reflect.Type, error) {
	types := map[string]reflect.Type{}

	var walk func(reflect.Type) error
	walk = func(t reflect.Type) error {
		for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
			t = t.Elem()
		}

		if value, ok := orderedMapValue(t); ok {
			return walk(value)
		}

		if t.Kind() != reflect.Struct || t.PkgPath() == "" {
			return nil
		}

		name := t.Name()
		if seen, ok := types[name]; ok {
			if seen != t {
				return fmt.Errorf("jsonschema: two types named %q: %s and %s", name, seen, t)
			}

			return nil
		}
		types[name] = t

		for _, f := range reflect.VisibleFields(t) {
			if _, ok := jsonName(f); !ok {
				continue
			}
			if err := walk(f.Type); err != nil {
				return err
			}
		}

		return nil
	}

	if err := walk(root); err != nil {
		return nil, err
	}

	return types, nil
}

// jsonName returns the name a field is written under, and false when it is not
// written at all.
func jsonName(f reflect.StructField) (string, bool) {
	if !f.IsExported() {
		return "", false
	}

	tag, ok := f.Tag.Lookup("json")
	if !ok {
		return "", false
	}

	name, _, _ := strings.Cut(tag, ",")
	if name == "" || name == "-" {
		return "", false
	}

	return name, true
}
