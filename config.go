package pistachio

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/alecthomas/kong"
	"gopkg.in/yaml.v3"
)

// Built-in kong meta flags are not loadable from the config file. --help is
// matched by pointer via Application.HelpFlag; the rest by their kong type.
var metaFlagTypes = map[reflect.Type]bool{
	reflect.TypeFor[kong.ConfigFlag]():    true,
	reflect.TypeFor[kong.VersionFlag]():   true,
	reflect.TypeFor[kong.ChangeDirFlag](): true,
}

// YAMLConfig is a kong.ConfigurationLoader that reads options from a YAML file.
// Keys must exactly match CLI flag names (e.g. "conn-string"); snake_case or
// camelCase variants are not accepted. Keys that do not match any flag are
// rejected. Values only apply to flags not set on the command line; a config
// file passed with --config takes precedence over environment variables.
func YAMLConfig(r io.Reader) (kong.Resolver, error) {
	values := map[string]any{}
	if err := yaml.NewDecoder(r).Decode(&values); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return &yamlResolver{values: values}, nil
}

type yamlResolver struct {
	values map[string]any
}

func (y *yamlResolver) Resolve(_ *kong.Context, _ *kong.Path, flag *kong.Flag) (any, error) {
	if isMetaFlag(flag) {
		return nil, nil
	}
	v, ok := y.values[flag.Name]
	if !ok {
		return nil, nil
	}
	// Environment variables take precedence over the config file. Kong applies
	// an env value before resolvers run, so defer to it by returning nil when
	// one is set for this flag. Precedence: flag > env > config > default.
	for _, env := range flag.Tag.Envs {
		if _, set := os.LookupEnv(env); set {
			return nil, nil
		}
	}
	return v, nil
}

func (y *yamlResolver) Validate(app *kong.Application) error {
	known := map[string]bool{}
	collectFlagNames(app.Node, app.HelpFlag, known)

	var unknown []string
	for key := range y.values {
		if !known[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("unknown config key(s): %s", strings.Join(unknown, ", "))
	}
	return nil
}

func collectFlagNames(node *kong.Node, help *kong.Flag, out map[string]bool) {
	for _, flag := range node.Flags {
		if flag == help || isMetaFlag(flag) {
			continue
		}
		out[flag.Name] = true
	}
	for _, child := range node.Children {
		collectFlagNames(child, help, out)
	}
}

func isMetaFlag(flag *kong.Flag) bool {
	return metaFlagTypes[flag.Target.Type()]
}
