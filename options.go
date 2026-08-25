package pistachio

import (
	"fmt"
	"path"
	"strings"
)

// DefaultSearchPath is the search_path every connection is opened with unless
// --search-path says otherwise. The catalog reads the schema through
// pg_get_viewdef, pg_get_constraintdef, pg_get_expr and format_type, each of
// which drops the schema from an object the session can reach unqualified, so
// this value decides what dump writes and what the diff compares. Left to a
// server-side "ALTER ROLE ... SET search_path", one database would read
// differently depending on who connected.
//
// PostgreSQL's own default, `"$user", public`, would leave the same gap: a
// schema named after the connecting role sits on the path, so a dump taken as
// that role writes its objects without a schema and another role cannot reload
// it. The role that runs migrations is often not the role the application
// connects as, so this drops the "$user" entry and keeps public alone. Pre-SQL
// runs under this setting.
//
// kong defaults the Options field to this, and connect falls back to it, so a
// library caller that builds Options directly connects the same way the CLI
// does.
const DefaultSearchPath = "public"

type Options struct {
	ConnString string            `short:"c" env:"PISTA_CONN_STR" default:"postgres://postgres@localhost/postgres" help:"PostgreSQL connection string. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING"`
	DBName     string            `name:"dbname" short:"d" env:"PISTA_DBNAME" help:"PostgreSQL database name. Overrides the dbname in --conn-string."`
	Password   string            `env:"PISTA_PASSWORD" help:"PostgreSQL password."`
	Schemas    []string          `short:"n" env:"PISTA_SCHEMAS" default:"public" help:"Schemas to inspect and modify."`
	SchemaMap  map[string]string `short:"m" help:"Schema name mapping (e.g. -m old=new)."`
	// SearchPath is a pointer so that an empty value is a path of its own,
	// under which the catalog qualifies everything, rather than a request for
	// the default. nil means the default.
	SearchPath *string `env:"PISTA_SEARCH_PATH" default:"public" help:"search_path for the database connection. The catalog reports an object reachable through it without its schema, so this decides how dump writes that object. Pass an empty value to qualify everything."`
}

type FilterOptions struct {
	Include []string `short:"I" env:"PISTA_INCLUDE" help:"Include only tables/views/enums/domains/composite types/sequences/routines matching the pattern (wildcard: *, ?)."`
	Exclude []string `short:"E" env:"PISTA_EXCLUDE" help:"Exclude tables/views/enums/domains/composite types/sequences/routines matching the pattern (wildcard: *, ?)."`
	Enable  []string `enum:"table,view,enum,domain,composite_type,sequence,routine" env:"PISTA_ENABLE" help:"Enable only specified object types (can be repeated)."`
	Disable []string `enum:"table,view,enum,domain,composite_type,sequence,routine" env:"PISTA_DISABLE" help:"Disable specified object types (can be repeated)."`
	// ManageRoutine opts into functions and procedures. They are unmanaged by
	// default: a schema that has been maintained with -- pista:execute holds
	// routines the desired schema does not declare, and reading pg_proc
	// unasked would report every one of them as a drop.
	ManageRoutine bool `env:"PISTA_MANAGE_ROUTINE" help:"Manage functions and procedures. Off by default; --allow-drop routine still gates dropping them."`
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
