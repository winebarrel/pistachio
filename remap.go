package pistachio

import (
	"regexp"
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

// defReplacer rewrites schema-qualified prefixes in raw SQL definitions,
// "staging." to "public.". A prefix counts only where a name can start: at the
// beginning of the text or after a character that cannot continue an
// identifier, so a schema whose name ends in the mapped one, mystaging., and a
// table named like the schema in a three-part reference, staging.staging.col,
// keep their second part. A string literal that starts the same way is still
// rewritten, since the text is not parsed here.
//
// All inputs come from canonical SQL, pg_get_*def output from the catalog or
// pg_query deparse output from the parser, so an identifier that requires
// quoting is already wrapped in double quotes and only the model.Ident form is
// matched. An unquoted fallback, the raw `a.b.` for a schema literally named
// `a.b`, would collide with a three-part reference `a.b.col`.
type defReplacer struct {
	re *regexp.Regexp
	to map[string]string
}

func buildDefReplacer(schemaMap map[string]string) *defReplacer {
	r := &defReplacer{to: make(map[string]string, len(schemaMap))}

	// Longest first, so a name that starts another whole name is not matched
	// short: the alternation takes the first branch that matches.
	froms := make([]string, 0, len(schemaMap))
	for from, to := range schemaMap {
		ident := model.Ident(from)
		froms = append(froms, ident)
		r.to[ident] = model.Ident(to)
	}
	slices.SortFunc(froms, func(a, b string) int {
		return len(b) - len(a)
	})
	for i, from := range froms {
		froms[i] = regexp.QuoteMeta(from)
	}

	// A double quote is not a boundary either: a schema literally named
	// staging.x is written "staging.x".t, and the unquoted branch must not
	// find staging. inside it.
	r.re = regexp.MustCompile(`(^|[^A-Za-z0-9_"])(` + strings.Join(froms, "|") + `)\.`)
	return r
}

// Replace returns s with every mapped schema prefix rewritten.
func (r *defReplacer) Replace(s string) string {
	return r.re.ReplaceAllStringFunc(s, func(m string) string {
		sub := r.re.FindStringSubmatch(m)
		return sub[1] + r.to[sub[2]] + "."
	})
}

func buildReverseDefReplacer(schemaMap map[string]string) *defReplacer {
	reversed := make(map[string]string, len(schemaMap))
	for k, v := range schemaMap {
		reversed[v] = k
	}
	return buildDefReplacer(reversed)
}

// remapQualifiedName rewrites the schema of a schema-qualified name, a column
// type or a partition parent, and leaves an unqualified one alone. The name is
// split the way an identifier is read, so a type modifier or an array suffix
// stays with the type, and a schema whose name ends in the mapped one is not
// touched the way a prefix substitution would touch it.
func remapQualifiedName(name string, mapSchema func(string) string) string {
	parts := model.SplitQualifiedName(name)
	if len(parts) != 2 {
		return name
	}
	schema := model.UnquoteIdent(parts[0])
	mapped := mapSchema(schema)
	if mapped == schema {
		return name
	}
	return model.Ident(mapped) + "." + parts[1]
}

// remapDefaultExpr rewrites the schema in a default expression, a column's or
// a parameter's: the name of a function it calls, the type of a cast, and the
// relation a reg* literal names, which is how nextval('myschema.seq'::regclass)
// carries one. The expression is parsed for it, since a prefix substitution
// over the text would also rewrite a string literal that happens to start the
// same way. An expression that names no mapped schema, or does not parse, is
// returned as it was.
func remapDefaultExpr(expr string, mapSchema func(string) string) string {
	result, target, err := pgast.ParseExpr(expr)
	if err != nil {
		return expr
	}

	remapName := func(names []*pg_query.Node) bool {
		if len(names) != 2 {
			return false
		}
		s := names[0].GetString_()
		if s == nil || mapSchema(s.Sval) == s.Sval {
			return false
		}
		s.Sval = mapSchema(s.Sval)
		return true
	}

	changed := false
	pgast.Walk(target.Val, pgast.WalkOptions{}, func(_ pgast.Ctx, node *pg_query.Node) *pg_query.Node {
		switch n := node.Node.(type) {
		case *pg_query.Node_FuncCall:
			if remapName(n.FuncCall.Funcname) {
				changed = true
			}
			if lit := sequenceLiteral(n.FuncCall); lit != nil {
				if mapped := remapQualifiedName(lit.Sval, mapSchema); mapped != lit.Sval {
					lit.Sval = mapped
					changed = true
				}
			}
		case *pg_query.Node_TypeCast:
			if remapName(n.TypeCast.GetTypeName().GetNames()) {
				changed = true
			}
			if lit := regLiteral(n.TypeCast); lit != nil {
				if mapped := remapQualifiedName(lit.Sval, mapSchema); mapped != lit.Sval {
					lit.Sval = mapped
					changed = true
				}
			}
		}
		return node
	})
	if !changed {
		return expr
	}

	sql, err := pg_query.Deparse(result)
	if err != nil {
		return expr
	}
	return strings.TrimPrefix(sql, "SELECT ")
}

// sequenceLiteral returns the bare string literal a sequence function is
// called with, as in nextval('myschema.seq'), or nil. A schema file writes the
// call that way; the catalog writes the literal cast to regclass, which
// regLiteral reads.
func sequenceLiteral(fc *pg_query.FuncCall) *pg_query.String {
	if !builtinName(fc.Funcname, "nextval", "currval", "setval") || len(fc.Args) == 0 {
		return nil
	}
	c := fc.Args[0].GetAConst()
	if c == nil {
		return nil
	}
	return c.GetSval()
}

// regTypes are the object identifier types. A literal cast to one names a
// catalog object, schema-qualified or not.
var regTypes = []string{
	"regclass", "regcollation", "regconfig", "regdictionary", "regnamespace",
	"regoper", "regoperator", "regproc", "regprocedure", "regrole", "regtype",
}

// regLiteral returns the string literal a cast to an object identifier type
// is applied to, or nil for any other cast.
func regLiteral(tc *pg_query.TypeCast) *pg_query.String {
	if !builtinName(tc.GetTypeName().GetNames(), regTypes...) {
		return nil
	}
	c := tc.Arg.GetAConst()
	if c == nil {
		return nil
	}
	return c.GetSval()
}

// builtinName reports whether a function or type name is one of the given
// pg_catalog names, written bare or qualified with pg_catalog. A user-defined
// object of the same name in another schema is not one, so its argument is
// left alone.
func builtinName(names []*pg_query.Node, candidates ...string) bool {
	switch len(names) {
	case 1:
	case 2:
		if names[0].GetString_().GetSval() != "pg_catalog" {
			return false
		}
	default:
		return false
	}
	return slices.Contains(candidates, names[len(names)-1].GetString_().GetSval())
}

// remapColumns rewrites the schema in each column's type, collation and
// default, in the partition parent, and in the constraint definitions.
func remapColumns(t *model.Table, mapSchema func(string) string, replacer *defReplacer) {
	for _, col := range t.Columns.CollectValues() {
		col.TypeName = remapQualifiedName(col.TypeName, mapSchema)
		col.Collation = remapQualifiedNamePtr(col.Collation, mapSchema)
		col.Default = remapDefaultExprPtr(col.Default, mapSchema)
	}
	t.PartitionOf = remapQualifiedNamePtr(t.PartitionOf, mapSchema)
	for _, con := range t.Constraints.CollectValues() {
		con.Definition = replacer.Replace(con.Definition)
	}
}

// remapDomain rewrites the schema in a domain's base type, collation and
// default, and in its constraint definitions.
func remapDomain(d *model.Domain, mapSchema func(string) string, replacer *defReplacer) {
	d.BaseType = remapQualifiedName(d.BaseType, mapSchema)
	d.Collation = remapQualifiedNamePtr(d.Collation, mapSchema)
	d.Default = remapDefaultExprPtr(d.Default, mapSchema)
	for _, con := range d.Constraints {
		con.Definition = replacer.Replace(con.Definition)
	}
}

func remapQualifiedNamePtr(name *string, mapSchema func(string) string) *string {
	if name == nil {
		return nil
	}
	mapped := remapQualifiedName(*name, mapSchema)
	return &mapped
}

func remapDefaultExprPtr(expr *string, mapSchema func(string) string) *string {
	if expr == nil {
		return nil
	}
	mapped := remapDefaultExpr(*expr, mapSchema)
	return &mapped
}

func (client *Client) remapTableSchemas(tables *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
	if len(client.SchemaMap) == 0 {
		return tables
	}

	replacer := buildDefReplacer(client.SchemaMap)
	remapped := orderedmap.New[string, *model.Table]()

	for _, t := range tables.CollectValues() {
		t.Schema = client.RemapSchema(t.Schema)
		remapColumns(t, client.RemapSchema, replacer)

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
	replacer *defReplacer,
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
	replacer *defReplacer,
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
		remapColumns(t, client.ReverseRemapSchema, replacer)

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
		remapDomain(d, client.RemapSchema, replacer)
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
		remapDomain(d, client.ReverseRemapSchema, replacer)
		remapped.Set(d.FQDN(), d)
	}

	return remapped
}

func (client *Client) remapCompositeTypeSchemas(compositeTypes *orderedmap.Map[string, *model.CompositeType]) *orderedmap.Map[string, *model.CompositeType] {
	if len(client.SchemaMap) == 0 {
		return compositeTypes
	}

	remapped := orderedmap.New[string, *model.CompositeType]()

	for _, ct := range compositeTypes.CollectValues() {
		ct.Schema = client.RemapSchema(ct.Schema)
		for _, a := range ct.Attributes {
			a.TypeName = remapQualifiedName(a.TypeName, client.RemapSchema)
			a.Collation = remapQualifiedNamePtr(a.Collation, client.RemapSchema)
		}
		remapped.Set(ct.FQCN(), ct)
	}

	return remapped
}

func (client *Client) reverseRemapCompositeTypeSchemas(compositeTypes *orderedmap.Map[string, *model.CompositeType]) *orderedmap.Map[string, *model.CompositeType] {
	if len(client.SchemaMap) == 0 {
		return compositeTypes
	}

	remapped := orderedmap.New[string, *model.CompositeType]()

	for _, ct := range compositeTypes.CollectValues() {
		ct.Schema = client.ReverseRemapSchema(ct.Schema)
		for _, a := range ct.Attributes {
			a.TypeName = remapQualifiedName(a.TypeName, client.ReverseRemapSchema)
			a.Collation = remapQualifiedNamePtr(a.Collation, client.ReverseRemapSchema)
		}
		remapped.Set(ct.FQCN(), ct)
	}

	return remapped
}

// remapRoutineSchemas rewrites the schema of each routine, of the type names
// in its signature and of a parameter default. The body is left untouched: it
// is opaque text in whatever language the routine is written in, and a blind
// prefix substitution over it would be a guess.
func (client *Client) remapRoutineSchemas(routines *orderedmap.Map[string, *model.Routine]) *orderedmap.Map[string, *model.Routine] {
	if len(client.SchemaMap) == 0 {
		return routines
	}
	return remapRoutines(routines, client.RemapSchema)
}

func (client *Client) reverseRemapRoutineSchemas(routines *orderedmap.Map[string, *model.Routine]) *orderedmap.Map[string, *model.Routine] {
	if len(client.SchemaMap) == 0 {
		return routines
	}
	return remapRoutines(routines, client.ReverseRemapSchema)
}

func remapRoutines(
	routines *orderedmap.Map[string, *model.Routine],
	remapSchema func(string) string,
) *orderedmap.Map[string, *model.Routine] {
	remapped := orderedmap.New[string, *model.Routine]()

	for _, r := range routines.CollectValues() {
		r.Schema = remapSchema(r.Schema)
		r.ReturnType = remapQualifiedName(r.ReturnType, remapSchema)
		for _, a := range r.Args {
			a.Type = remapQualifiedName(a.Type, remapSchema)
			if a.Default != "" {
				a.Default = remapDefaultExpr(a.Default, remapSchema)
			}
		}
		remapped.Set(r.FQRN(), r)
	}

	return remapped
}
