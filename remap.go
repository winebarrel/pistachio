package pistachio

import (
	"strings"

	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

// buildDefReplacer builds a strings.Replacer that replaces schema-qualified
// prefixes in raw SQL definitions (e.g. "staging." → "public.").
// It handles both unquoted and quoted schema identifiers.
func buildDefReplacer(schemaMap map[string]string) *strings.Replacer {
	var pairs []string
	for from, to := range schemaMap {
		fromIdent := model.Ident(from)
		toIdent := model.Ident(to)
		// Always add the Ident form (handles quoting)
		pairs = append(pairs, fromIdent+".", toIdent+".")
		// If Ident differs from raw name, also add the raw form
		if fromIdent != from {
			pairs = append(pairs, from+".", to+".")
		}
	}
	return strings.NewReplacer(pairs...)
}

func buildReverseDefReplacer(schemaMap map[string]string) *strings.Replacer {
	reversed := make(map[string]string, len(schemaMap))
	for k, v := range schemaMap {
		reversed[v] = k
	}
	return buildDefReplacer(reversed)
}

func (client *Client) remapTableSchemas(tables *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
	if len(client.SchemaMap) == 0 {
		return tables
	}

	replacer := buildDefReplacer(client.SchemaMap)
	remapped := orderedmap.New[string, *model.Table]()

	for _, t := range tables.CollectValues() {
		t.Schema = client.RemapSchema(t.Schema)

		for _, idx := range t.Indexes.CollectValues() {
			idx.Schema = client.RemapSchema(idx.Schema)
			idx.Definition = replacer.Replace(idx.Definition)
		}

		for _, fk := range t.ForeignKeys.CollectValues() {
			fk.Schema = client.RemapSchema(fk.Schema)
			fk.Definition = replacer.Replace(fk.Definition)
			if fk.RefSchema != nil {
				mapped := client.RemapSchema(*fk.RefSchema)
				fk.RefSchema = &mapped
			}
		}

		remapped.Set(t.FQTN(), t)
	}

	return remapped
}

func (client *Client) remapViewSchemas(views *orderedmap.Map[string, *model.View]) *orderedmap.Map[string, *model.View] {
	if len(client.SchemaMap) == 0 {
		return views
	}

	replacer := buildDefReplacer(client.SchemaMap)
	remapped := orderedmap.New[string, *model.View]()

	for _, v := range views.CollectValues() {
		v.Schema = client.RemapSchema(v.Schema)
		v.Definition = replacer.Replace(v.Definition)
		remapped.Set(v.FQVN(), v)
	}

	return remapped
}

func (client *Client) reverseRemapTableSchemas(tables *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
	if len(client.SchemaMap) == 0 {
		return tables
	}

	replacer := buildReverseDefReplacer(client.SchemaMap)
	remapped := orderedmap.New[string, *model.Table]()

	for _, t := range tables.CollectValues() {
		t.Schema = client.ReverseRemapSchema(t.Schema)

		for _, idx := range t.Indexes.CollectValues() {
			idx.Schema = client.ReverseRemapSchema(idx.Schema)
			idx.Definition = replacer.Replace(idx.Definition)
		}

		for _, fk := range t.ForeignKeys.CollectValues() {
			fk.Schema = client.ReverseRemapSchema(fk.Schema)
			fk.Definition = replacer.Replace(fk.Definition)
			if fk.RefSchema != nil {
				mapped := client.ReverseRemapSchema(*fk.RefSchema)
				fk.RefSchema = &mapped
			}
		}

		remapped.Set(t.FQTN(), t)
	}

	return remapped
}

func (client *Client) reverseRemapViewSchemas(views *orderedmap.Map[string, *model.View]) *orderedmap.Map[string, *model.View] {
	if len(client.SchemaMap) == 0 {
		return views
	}

	replacer := buildReverseDefReplacer(client.SchemaMap)
	remapped := orderedmap.New[string, *model.View]()

	for _, v := range views.CollectValues() {
		v.Schema = client.ReverseRemapSchema(v.Schema)
		v.Definition = replacer.Replace(v.Definition)
		remapped.Set(v.FQVN(), v)
	}

	return remapped
}

func (client *Client) remapEnumSchemas(enums *orderedmap.Map[string, *model.Enum]) *orderedmap.Map[string, *model.Enum] {
	if len(client.SchemaMap) == 0 {
		return enums
	}

	remapped := orderedmap.New[string, *model.Enum]()

	for _, e := range enums.CollectValues() {
		e.Schema = client.RemapSchema(e.Schema)
		remapped.Set(e.FQEN(), e)
	}

	return remapped
}

func (client *Client) reverseRemapEnumSchemas(enums *orderedmap.Map[string, *model.Enum]) *orderedmap.Map[string, *model.Enum] {
	if len(client.SchemaMap) == 0 {
		return enums
	}

	remapped := orderedmap.New[string, *model.Enum]()

	for _, e := range enums.CollectValues() {
		e.Schema = client.ReverseRemapSchema(e.Schema)
		remapped.Set(e.FQEN(), e)
	}

	return remapped
}

func (client *Client) remapDomainSchemas(domains *orderedmap.Map[string, *model.Domain]) *orderedmap.Map[string, *model.Domain] {
	if len(client.SchemaMap) == 0 {
		return domains
	}

	remapped := orderedmap.New[string, *model.Domain]()

	for _, d := range domains.CollectValues() {
		d.Schema = client.RemapSchema(d.Schema)
		remapped.Set(d.FQDN(), d)
	}

	return remapped
}

func (client *Client) reverseRemapDomainSchemas(domains *orderedmap.Map[string, *model.Domain]) *orderedmap.Map[string, *model.Domain] {
	if len(client.SchemaMap) == 0 {
		return domains
	}

	remapped := orderedmap.New[string, *model.Domain]()

	for _, d := range domains.CollectValues() {
		d.Schema = client.ReverseRemapSchema(d.Schema)
		remapped.Set(d.FQDN(), d)
	}

	return remapped
}
