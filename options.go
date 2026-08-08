package pistachio

import (
	"fmt"
	"path"
	"strings"
)

// DefaultSearchPath is PostgreSQL's own default, and the search_path every
// connection is opened with unless --search-path says otherwise. The catalog
// reads the schema through pg_get_viewdef, pg_get_constraintdef, pg_get_expr
// and format_type, each of which drops the schema from an object the session
// can reach unqualified, so this value decides what dump writes and what the
// diff compares. Left to a server-side "ALTER ROLE ... SET search_path", one
// database would read differently depending on who connected.
//
// The "$user" entry keeps one case of that: a schema named after the
// connecting role is reached unqualified, so a dump taken as that role writes
// its objects without a schema and another role cannot reload it. Keeping
// PostgreSQL's own default is what pre-SQL and anything else on the connection
// expect, so the entry stays; --search-path=public drops it.
//
// kong defaults the Options field to this, and connect falls back to it, so a
// library caller that builds Options directly connects the same way the CLI
// does.
const DefaultSearchPath = `"$user", public`

type Options struct {
	ConnString string            `short:"c" env:"PISTA_CONN_STR" default:"postgres://postgres@localhost/postgres" help:"PostgreSQL connection string. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING"`
	DBName     string            `name:"dbname" short:"d" env:"PISTA_DBNAME" help:"PostgreSQL database name. Overrides the dbname in --conn-string."`
	Password   string            `env:"PISTA_PASSWORD" help:"PostgreSQL password."`
	Schemas    []string          `short:"n" env:"PISTA_SCHEMAS" default:"public" help:"Schemas to inspect and modify."`
	SchemaMap  map[string]string `short:"m" help:"Schema name mapping (e.g. -m old=new)."`
	// SearchPath is a pointer so that an empty value is a path of its own,
	// under which the catalog qualifies everything, rather than a request for
	// the default. nil means the default.
	SearchPath *string `env:"PISTA_SEARCH_PATH" default:"\"$user\", public" help:"search_path for the database connection. The catalog reports an object reachable through it without its schema, so this decides how dump writes that object. Pass an empty value to qualify everything."`
}

type FilterOptions struct {
	Include []string `short:"I" env:"PISTA_INCLUDE" help:"Include only tables/views/enums/domains/composite types/sequences matching the pattern (wildcard: *, ?)."`
	Exclude []string `short:"E" env:"PISTA_EXCLUDE" help:"Exclude tables/views/enums/domains/composite types/sequences matching the pattern (wildcard: *, ?)."`
	Enable  []string `enum:"table,view,enum,domain,composite_type,sequence" env:"PISTA_ENABLE" help:"Enable only specified object types (can be repeated)."`
	Disable []string `enum:"table,view,enum,domain,composite_type,sequence" env:"PISTA_DISABLE" help:"Disable specified object types (can be repeated)."`
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
	for i, pattern := range f.Include {
		f.Include[i] = strings.TrimSpace(pattern)
	}
	for i, pattern := range f.Exclude {
		f.Exclude[i] = strings.TrimSpace(pattern)
	}
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
