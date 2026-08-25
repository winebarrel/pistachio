package pistachio

import (
	"strings"

	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// buildDefReplacer builds a strings.Replacer that replaces schema-qualified
// prefixes in raw SQL definitions (e.g. "staging." -> "public.").
//
// All inputs to this replacer come from canonical SQL; pg_get_*def output
// from the catalog or pg_query deparse output from the parser; so any
// identifier that requires quoting is already wrapped in double quotes.
// Only the model.Ident form is added to the pair list. Adding an unquoted
// fallback (e.g. raw `a.b.` for a schema literally named `a.b`) would
// dangerously collide with three-part references like `a.b.col` (schema
// `a`, table `b`, column `col`).
func buildDefReplacer(schemaMap map[string]string) *strings.Replacer {
	var pairs []string
	for from, to := range schemaMap {
		pairs = append(pairs, model.Ident(from)+".", model.Ident(to)+".")
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

		remapPolicies(t.Policies, client.RemapSchema, replacer)
		remapTriggers(t.Triggers, client.RemapSchema, replacer)

		remapped.Set(t.FQTN(), t)
	}

	return remapped
}

// remapPolicies rewrites the Schema field on each policy and applies the
// schema replacer to USING / WITH CHECK expressions so cross-schema
// references in subqueries / function calls follow the table schema.
// `policies` is always non-nil because both parser and catalog initialize
// Table.Policies.
func remapPolicies(
	policies *orderedmap.Map[string, *model.Policy],
	mapSchema func(string) string,
	replacer *strings.Replacer,
) {
	for _, p := range policies.CollectValues() {
		p.Schema = mapSchema(p.Schema)
		if p.Using != nil {
			expr := replacer.Replace(*p.Using)
			p.Using = &expr
		}
		if p.WithCheck != nil {
			expr := replacer.Replace(*p.WithCheck)
			p.WithCheck = &expr
		}
	}
}

// remapTriggers rewrites the Schema field on each trigger and applies the
// schema replacer to the definition, which names the relation the trigger is
// on and may name a function in another schema. `triggers` is always non-nil
// because both parser and catalog initialize the map.
func remapTriggers(
	triggers *orderedmap.Map[string, *model.Trigger],
	mapSchema func(string) string,
	replacer *strings.Replacer,
) {
	for _, trg := range triggers.CollectValues() {
		trg.Schema = mapSchema(trg.Schema)
		trg.Definition = replacer.Replace(trg.Definition)
	}
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
		remapTriggers(v.Triggers, client.RemapSchema, replacer)
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

		remapPolicies(t.Policies, client.ReverseRemapSchema, replacer)
		remapTriggers(t.Triggers, client.ReverseRemapSchema, replacer)

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
		remapTriggers(v.Triggers, client.ReverseRemapSchema, replacer)
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

func (client *Client) remapSequenceSchemas(sequences *orderedmap.Map[string, *model.Sequence]) *orderedmap.Map[string, *model.Sequence] {
	if len(client.SchemaMap) == 0 {
		return sequences
	}

	remapped := orderedmap.New[string, *model.Sequence]()

	for _, s := range sequences.CollectValues() {
		s.Schema = client.RemapSchema(s.Schema)
		remapped.Set(s.FQN(), s)
	}

	return remapped
}

func (client *Client) reverseRemapSequenceSchemas(sequences *orderedmap.Map[string, *model.Sequence]) *orderedmap.Map[string, *model.Sequence] {
	if len(client.SchemaMap) == 0 {
		return sequences
	}

	remapped := orderedmap.New[string, *model.Sequence]()

	for _, s := range sequences.CollectValues() {
		s.Schema = client.ReverseRemapSchema(s.Schema)
		remapped.Set(s.FQN(), s)
	}

	return remapped
}

func (client *Client) remapDomainSchemas(domains *orderedmap.Map[string, *model.Domain]) *orderedmap.Map[string, *model.Domain] {
	if len(client.SchemaMap) == 0 {
		return domains
	}

	replacer := buildDefReplacer(client.SchemaMap)
	remapped := orderedmap.New[string, *model.Domain]()

	for _, d := range domains.CollectValues() {
		d.Schema = client.RemapSchema(d.Schema)
		d.BaseType = replacer.Replace(d.BaseType)
		remapped.Set(d.FQDN(), d)
	}

	return remapped
}

func (client *Client) reverseRemapDomainSchemas(domains *orderedmap.Map[string, *model.Domain]) *orderedmap.Map[string, *model.Domain] {
	if len(client.SchemaMap) == 0 {
		return domains
	}

	replacer := buildReverseDefReplacer(client.SchemaMap)
	remapped := orderedmap.New[string, *model.Domain]()

	for _, d := range domains.CollectValues() {
		d.Schema = client.ReverseRemapSchema(d.Schema)
		d.BaseType = replacer.Replace(d.BaseType)
		remapped.Set(d.FQDN(), d)
	}

	return remapped
}

func (client *Client) remapCompositeTypeSchemas(compositeTypes *orderedmap.Map[string, *model.CompositeType]) *orderedmap.Map[string, *model.CompositeType] {
	if len(client.SchemaMap) == 0 {
		return compositeTypes
	}

	replacer := buildDefReplacer(client.SchemaMap)
	remapped := orderedmap.New[string, *model.CompositeType]()

	for _, ct := range compositeTypes.CollectValues() {
		ct.Schema = client.RemapSchema(ct.Schema)
		for _, a := range ct.Attributes {
			a.TypeName = replacer.Replace(a.TypeName)
		}
		remapped.Set(ct.FQCN(), ct)
	}

	return remapped
}

func (client *Client) reverseRemapCompositeTypeSchemas(compositeTypes *orderedmap.Map[string, *model.CompositeType]) *orderedmap.Map[string, *model.CompositeType] {
	if len(client.SchemaMap) == 0 {
		return compositeTypes
	}

	replacer := buildReverseDefReplacer(client.SchemaMap)
	remapped := orderedmap.New[string, *model.CompositeType]()

	for _, ct := range compositeTypes.CollectValues() {
		ct.Schema = client.ReverseRemapSchema(ct.Schema)
		for _, a := range ct.Attributes {
			a.TypeName = replacer.Replace(a.TypeName)
		}
		remapped.Set(ct.FQCN(), ct)
	}

	return remapped
}

// remapRoutineSchemas rewrites the schema of each routine and of the type
// names in its signature. The body is left untouched: it is opaque text in
// whatever language the routine is written in, and a blind prefix substitution
// over it would be a guess.
func (client *Client) remapRoutineSchemas(routines *orderedmap.Map[string, *model.Routine]) *orderedmap.Map[string, *model.Routine] {
	if len(client.SchemaMap) == 0 {
		return routines
	}
	return remapRoutines(routines, client.RemapSchema, buildDefReplacer(client.SchemaMap))
}

func (client *Client) reverseRemapRoutineSchemas(routines *orderedmap.Map[string, *model.Routine]) *orderedmap.Map[string, *model.Routine] {
	if len(client.SchemaMap) == 0 {
		return routines
	}
	return remapRoutines(routines, client.ReverseRemapSchema, buildReverseDefReplacer(client.SchemaMap))
}

func remapRoutines(
	routines *orderedmap.Map[string, *model.Routine],
	remapSchema func(string) string,
	replacer *strings.Replacer,
) *orderedmap.Map[string, *model.Routine] {
	remapped := orderedmap.New[string, *model.Routine]()

	for _, r := range routines.CollectValues() {
		r.Schema = remapSchema(r.Schema)
		r.ReturnType = replacer.Replace(r.ReturnType)
		for _, a := range r.Args {
			a.Type = replacer.Replace(a.Type)
			a.Default = replacer.Replace(a.Default)
		}
		remapped.Set(r.FQRN(), r)
	}

	return remapped
}
