package pistachio

import "fmt"

type Options struct {
	ConnString string            `short:"c" env:"DATABASE_URL" default:"postgres://postgres@localhost/postgres" help:"PostgreSQL connection string. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING"`
	Password   string            `env:"PGPASSWORD" help:"PostgreSQL password."`
	Schemas    []string          `short:"n" env:"PGSCHEMAS" default:"public" help:"Schemas to inspect and modify."`
	SchemaMap  map[string]string `short:"m" help:"Schema name mapping (e.g. -m old=new)."`
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
