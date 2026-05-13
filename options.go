package pistachio

import (
	"fmt"
	"os"
	"path"

	"github.com/mattn/go-isatty"
)

type Options struct {
	ConnString string            `short:"c" env:"PISTA_CONN_STR" default:"postgres://postgres@localhost/postgres" help:"PostgreSQL connection string. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING"`
	DBName     string            `name:"dbname" short:"d" env:"PISTA_DBNAME" help:"PostgreSQL database name. Overrides the dbname in --conn-string."`
	Password   string            `env:"PISTA_PASSWORD" help:"PostgreSQL password."`
	Schemas    []string          `short:"n" env:"PISTA_SCHEMAS" default:"public" help:"Schemas to inspect and modify."`
	SchemaMap  map[string]string `short:"m" help:"Schema name mapping (e.g. -m old=new)."`
	Color      bool              `negatable:"" help:"Colorize SQL output. Auto-detected from TTY/NO_COLOR."`
}

// BeforeApply seeds Options.Color from the runtime environment so the
// --color / --no-color flag has a sensible default. Kong invokes this before
// flag values are applied, so an explicit flag still wins. NO_COLOR is honored
// per https://no-color.org/ — presence (regardless of value, including empty)
// disables color, so os.LookupEnv is used to distinguish "unset" from
// "set to empty".
func (o *Options) BeforeApply() error {
	_, noColor := os.LookupEnv("NO_COLOR")
	o.Color = isatty.IsTerminal(os.Stdout.Fd()) && !noColor
	return nil
}

type FilterOptions struct {
	Include []string `short:"I" env:"PISTA_INCLUDE" help:"Include only tables/views/enums/domains matching the pattern (wildcard: *, ?)."`
	Exclude []string `short:"E" env:"PISTA_EXCLUDE" help:"Exclude tables/views/enums/domains matching the pattern (wildcard: *, ?)."`
	Enable  []string `enum:"table,view,enum,domain" env:"PISTA_ENABLE" help:"Enable only specified object types (can be repeated)."`
	Disable []string `enum:"table,view,enum,domain" env:"PISTA_DISABLE" help:"Disable specified object types (can be repeated)."`
}

// IsTypeEnabled returns true if the given object type should be included.
// Enable takes precedence: if set, only listed types are enabled.
// Disable excludes listed types (ignored when Enable is set).
// If neither is set, all types are enabled.
func (f *FilterOptions) IsTypeEnabled(typeName string) bool {
	if len(f.Enable) > 0 {
		for _, t := range f.Enable {
			if t == typeName {
				return true
			}
		}
		return false
	}
	for _, t := range f.Disable {
		if t == typeName {
			return false
		}
	}
	return true
}

func (f *FilterOptions) MatchName(name string) bool {
	if len(f.Include) > 0 {
		matched := false
		for _, pattern := range f.Include {
			if ok, _ := path.Match(pattern, name); ok {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, pattern := range f.Exclude {
		if ok, _ := path.Match(pattern, name); ok {
			return false
		}
	}
	return true
}

func (f *FilterOptions) AfterApply() error {
	return f.ValidatePatterns()
}

func (f *FilterOptions) ValidatePatterns() error {
	for _, pattern := range f.Include {
		if _, err := path.Match(pattern, ""); err != nil {
			return fmt.Errorf("invalid --include pattern %q: %w", pattern, err)
		}
	}
	for _, pattern := range f.Exclude {
		if _, err := path.Match(pattern, ""); err != nil {
			return fmt.Errorf("invalid --exclude pattern %q: %w", pattern, err)
		}
	}
	return nil
}

func (o *Options) AfterApply() error {
	return o.ValidateSchemaMap()
}

func (o *Options) ValidateSchemaMap() error {
	if len(o.SchemaMap) <= 1 {
		return nil
	}
	seen := make(map[string]string, len(o.SchemaMap))
	for from, to := range o.SchemaMap {
		if prev, ok := seen[to]; ok {
			return fmt.Errorf("duplicate schema-map destination %q: both %q and %q map to it", to, prev, from)
		}
		seen[to] = from
	}
	return nil
}

func (o *Options) RemapSchema(schema string) string {
	if o.SchemaMap == nil {
		return schema
	}
	if mapped, ok := o.SchemaMap[schema]; ok {
		return mapped
	}
	return schema
}

func (o *Options) ReverseRemapSchema(schema string) string {
	if o.SchemaMap == nil {
		return schema
	}
	for k, v := range o.SchemaMap {
		if v == schema {
			return k
		}
	}
	return schema
}
