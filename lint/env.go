package lint

import (
	"encoding/json/v2"
	"fmt"
	"io"
	"reflect"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"cel.dev/cel-go/ext"
	"github.com/winebarrel/pistachio/parser"
	"google.golang.org/protobuf/types/known/structpb"
)

// trace says which object and rule an evaluation is for, so that debug() can
// name them. Rules are evaluated one at a time.
type trace struct {
	w      io.Writer
	object string
	rule   string
}

// variables lists the variables a rule of each kind reads, besides doc.
var variables = map[parser.LintKind][]string{
	parser.LintTable:      {"table"},
	parser.LintColumn:     {"table", "column"},
	parser.LintIndex:      {"table", "index"},
	parser.LintForeignKey: {"table", "fk"},
}

// must returns v, and panics on an error that only a bug in this package can
// cause, such as a CEL declaration that does not build.
func must[T any](v T, err error) T {
	mustNoError(err)
	return v
}

func mustNoError(err error) {
	if err != nil {
		panic(err)
	}
}

// newEnvs builds one CEL environment per kind. A rule can use only the
// variables of its kind, so a column rule that reads index fails to compile.
func newEnvs(tr *trace) map[parser.LintKind]*cel.Env {
	funcs := []cel.EnvOption{
		// The CEL extensions for strings (indexOf, substring), regular
		// expressions (regex.replace) and cel.bind. The regular expression
		// functions need optional types.
		ext.Strings(),
		cel.OptionalTypes(),
		ext.Regex(),
		ext.Bindings(),
		// m.values() lists the values of a map. Most of the document is
		// keyed by name, and the macros iterate a map's keys.
		cel.Function("values",
			cel.MemberOverload("map_values", []*cel.Type{cel.MapType(cel.DynType, cel.DynType)}, cel.ListType(cel.DynType),
				cel.UnaryBinding(mapValues))),
		// hasPrefix(list, prefix) is true when list starts with prefix.
		cel.Function("hasPrefix",
			cel.Overload("list_has_prefix", []*cel.Type{cel.ListType(cel.DynType), cel.ListType(cel.DynType)}, cel.BoolType,
				cel.BinaryBinding(listHasPrefix))),
		// debug(label, value) writes value and returns it unchanged.
		cel.Function("debug",
			cel.Overload("debug_dyn", []*cel.Type{cel.StringType, cel.DynType}, cel.DynType,
				cel.BinaryBinding(func(label, value ref.Val) ref.Val {
					fmt.Fprintf(tr.w, "debug: %s: %s: %v = %s\n", tr.object, tr.rule, label.Value(), toJSON(value)) //nolint:errcheck
					return value
				}))),
	}

	envs := map[parser.LintKind]*cel.Env{}
	for kind, names := range variables {
		opts := append([]cel.EnvOption{cel.Variable("doc", cel.DynType)}, funcs...)
		for _, name := range names {
			opts = append(opts, cel.Variable(name, cel.DynType))
		}
		envs[kind] = must(cel.NewEnv(opts...))
	}

	return envs
}

// mapValues and listHasPrefix are declared for a map and for lists, so CEL
// calls them with nothing else.
func mapValues(v ref.Val) ref.Val {
	m := v.(traits.Mapper)

	var values []ref.Val
	it := m.Iterator()
	for it.HasNext() == types.True {
		values = append(values, m.Get(it.Next()))
	}

	return types.DefaultTypeAdapter.NativeToValue(values)
}

func listHasPrefix(list, prefix ref.Val) ref.Val {
	l := list.(traits.Lister)
	p := prefix.(traits.Lister)

	n := p.Size().(types.Int)
	if l.Size().(types.Int) < n {
		return types.False
	}
	for i := range n {
		if l.Get(i).Equal(p.Get(i)) != types.True {
			return types.False
		}
	}

	return types.True
}

// toJSON writes a CEL value as JSON, or as Go writes it when it has no JSON
// form.
func toJSON(v ref.Val) string {
	// protojson varies its whitespace on purpose, so the value is written
	// through encoding/json instead.
	native, err := v.ConvertToNative(reflect.TypeFor[*structpb.Value]())
	if err == nil {
		if b, err := json.Marshal(native.(*structpb.Value).AsInterface()); err == nil {
			return string(b)
		}
	}
	return fmt.Sprint(v.Value())
}
