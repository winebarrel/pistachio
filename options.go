package pistachio

import (
	"fmt"
	"path"
	"regexp"
	"slices"
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
	Include []string `short:"I" env:"PISTA_INCLUDE" help:"Include only tables/views/enums/domains/composite types/sequences/routines matching the pattern (wildcard: *, ?; /re/ for a regular expression)."`
	Exclude []string `short:"E" env:"PISTA_EXCLUDE" help:"Exclude tables/views/enums/domains/composite types/sequences/routines matching the pattern (wildcard: *, ?; /re/ for a regular expression)."`
	Enable  []string `enum:"table,view,enum,domain,composite_type,sequence,routine" env:"PISTA_ENABLE" help:"Enable only specified object types (can be repeated)."`
	Disable []string `enum:"table,view,enum,domain,composite_type,sequence,routine" env:"PISTA_DISABLE" help:"Disable specified object types (can be repeated)."`
	// ManageRoutine opts into functions and procedures. They are unmanaged by
	// default: a schema that has been maintained with -- pista:execute holds
	// routines the desired schema does not declare, and reading pg_proc
	// unasked would report every one of them as a drop.
	ManageRoutine bool `env:"PISTA_MANAGE_ROUTINE" help:"Manage functions and procedures. Off by default; --allow-drop routine still gates dropping them."`
	// ManageStorageParam opts into a table's storage parameters, the WITH
	// clause. They are unmanaged by default: the autovacuum settings a table
	// is tuned with are usually set on the database, not written in the
	// schema file, and reading them unasked would RESET every parameter the
	// desired schema does not name.
	ManageStorageParam bool `env:"PISTA_MANAGE_STORAGE_PARAM" help:"Manage a table's storage parameters, the WITH (...) clause. Off by default; without it the clause is ignored on both sides and dump does not write it."`
	// SkipPartitionChild leaves the partitions of a partitioned table
	// unmanaged. Where another tool creates them, their names follow no
	// pattern --exclude can state, and a schema file that declares the parent
	// alone would plan a DROP for each one.
	SkipPartitionChild bool `env:"PISTA_SKIP_PARTITION_CHILD" help:"Manage a partitioned table without its partitions. For a schema whose partitions another tool creates. An INHERITS child is unaffected."`
}

// IsTypeEnabled returns true if the given object type should be included.
// Enable takes precedence: if set, only listed types are enabled.
// Disable excludes listed types (ignored when Enable is set).
// If neither is set, all types are enabled.
func (f *FilterOptions) IsTypeEnabled(typeName string) bool {
	if len(f.Enable) > 0 {
		return slices.Contains(f.Enable, typeName)
	}
	return !slices.Contains(f.Disable, typeName)
}

// regexpPattern returns the expression inside a /.../ pattern. An unquoted
// identifier cannot hold a slash, so the delimiters do not collide with real
// names.
func regexpPattern(pattern string) (string, bool) {
	if len(pattern) >= 2 && strings.HasPrefix(pattern, "/") && strings.HasSuffix(pattern, "/") {
		return pattern[1 : len(pattern)-1], true
	}
	return "", false
}

// matchPattern reports whether name matches one --include / --exclude pattern.
// A pattern wrapped in slashes is a regular expression, which matches anywhere
// in the name unless it is anchored. Anything else is a wildcard and has to
// match the whole name.
func matchPattern(pattern, name string) (bool, error) {
	if expr, ok := regexpPattern(pattern); ok {
		re, err := regexp.Compile(expr)
		if err != nil {
			return false, err
		}
		return re.MatchString(name), nil
	}
	return path.Match(pattern, name)
}

func (f *FilterOptions) MatchName(name string) bool {
	if len(f.Include) > 0 {
		matched := false
		for _, pattern := range f.Include {
			if ok, _ := matchPattern(pattern, name); ok {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, pattern := range f.Exclude {
		if ok, _ := matchPattern(pattern, name); ok {
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
		if _, err := matchPattern(pattern, ""); err != nil {
			return fmt.Errorf("invalid --include pattern %q: %w", pattern, err)
		}
	}
	for _, pattern := range f.Exclude {
		if _, err := matchPattern(pattern, ""); err != nil {
			return fmt.Errorf("invalid --exclude pattern %q: %w", pattern, err)
		}
	}
	return nil
}

func (o *Options) AfterApply() error {
	return o.ValidateSchemaMap()
}

// ValidateSchemaMap rejects two sources mapping to one destination, which
// would leave ReverseRemapSchema no way to pick between them. The sources are
// walked in name order so the pair the message names is the same on every run;
// a Go map iterates in a random one, and three sources on one destination then
// produced a different message each time the same command was run.
func (o *Options) ValidateSchemaMap() error {
	if len(o.SchemaMap) <= 1 {
		return nil
	}
	froms := make([]string, 0, len(o.SchemaMap))
	for from := range o.SchemaMap {
		froms = append(froms, from)
	}
	slices.Sort(froms)

	seen := make(map[string]string, len(o.SchemaMap))
	for _, from := range froms {
		to := o.SchemaMap[from]
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
