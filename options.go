package pistachio

import (
	"fmt"
	"path"
)

type Options struct {
	ConnString string            `short:"c" env:"PIST_CONN_STR" default:"postgres://postgres@localhost/postgres" help:"PostgreSQL connection string. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING"`
	Password   string            `env:"PIST_PASSWORD" help:"PostgreSQL password."`
	Schemas    []string          `short:"n" env:"PGSCHEMAS" default:"public" help:"Schemas to inspect and modify."`
	SchemaMap  map[string]string `short:"m" help:"Schema name mapping (e.g. -m old=new)."`
	Include    []string          `short:"I" help:"Include only tables/views matching the pattern (wildcard: *, ?)."`
	Exclude    []string          `short:"E" help:"Exclude tables/views matching the pattern (wildcard: *, ?)."`
}

func (o *Options) MatchName(name string) bool {
	if len(o.Include) > 0 {
		matched := false
		for _, pattern := range o.Include {
			if ok, _ := path.Match(pattern, name); ok {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, pattern := range o.Exclude {
		if ok, _ := path.Match(pattern, name); ok {
			return false
		}
	}
	return true
}

func (o *Options) AfterApply() error {
	if err := o.ValidateSchemaMap(); err != nil {
		return err
	}
	return o.ValidatePatterns()
}

func (o *Options) ValidatePatterns() error {
	for _, pattern := range o.Include {
		if _, err := path.Match(pattern, ""); err != nil {
			return fmt.Errorf("invalid --include pattern %q: %w", pattern, err)
		}
	}
	for _, pattern := range o.Exclude {
		if _, err := path.Match(pattern, ""); err != nil {
			return fmt.Errorf("invalid --exclude pattern %q: %w", pattern, err)
		}
	}
	return nil
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
