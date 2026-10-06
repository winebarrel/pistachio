// Package document builds the JSON document that pista parse and pista dump
// --json write. Each type embeds a model type and adds fields that repeat part
// of another field in a form that needs no SQL parsing.
//
// The fields live here and not in the model. The model is what plan and apply
// read, and the state hash that a plan file records is taken over the model as
// JSON, so a field added to it would change the hash of an unchanged database.
package document

import (
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

// Document is the whole JSON document. Tables and Views replace the fields of
// the same name in ParseResult.
type Document struct {
	*parser.ParseResult
	Tables *orderedmap.Map[string, *Table] `json:"tables"`
	Views  *orderedmap.Map[string, *View]  `json:"views"`
}

// Table is a table with its columns, indexes and foreign keys replaced by the
// types of this package. Columns is an array in the column order, as the model
// writes it.
type Table struct {
	*model.Table
	Columns     []*Column                            `json:"columns"`
	Indexes     *orderedmap.Map[string, *Index]      `json:"indexes"`
	ForeignKeys *orderedmap.Map[string, *ForeignKey] `json:"foreign_keys"`
}

// View is a view with its indexes replaced by the type of this package.
type View struct {
	*model.View
	Indexes *orderedmap.Map[string, *Index] `json:"indexes"`
}

// Column adds BaseType, the column type without its modifier and array
// marker, with a domain replaced by the type it is based on and a serial type
// by its integer type. IsArray reports an array type.
type Column struct {
	*model.Column
	BaseType string `json:"base_type"`
	IsArray  bool   `json:"is_array"`
}

// Index adds the parts of the index definition. Columns holds the key column
// names, with nil for an expression. Partial reports a WHERE clause.
type Index struct {
	*model.Index
	Columns []*string `json:"columns"`
	Include []string  `json:"include"`
	Unique  bool      `json:"unique"`
	Method  string    `json:"method"`
	Partial bool      `json:"partial"`
}

// ForeignKey adds the parts of the key definition. OnDelete and OnUpdate hold
// the action in lower case, "no action" when the key specifies none. Match is
// simple or full.
type ForeignKey struct {
	*model.ForeignKey
	RefColumns []string `json:"ref_columns"`
	OnDelete   string   `json:"on_delete"`
	OnUpdate   string   `json:"on_update"`
	Match      string   `json:"match"`
}

// New builds the document for a parse result. It reads only the model, so a
// document built from the catalog gets the same values as one built from
// files.
//
// An unqualified domain name is looked up in schemas and then in public, the
// search path that plan, apply and dump set.
//
// A definition that pg_query cannot read, such as a PostgreSQL 18 foreign key
// with NOT ENFORCED, leaves the added fields of that index or key empty rather
// than failing the document.
func New(r *parser.ParseResult, schemas []string) *Document {
	if !slices.Contains(schemas, "public") {
		schemas = append(slices.Clip(schemas), "public")
	}

	domains := r.Domains
	if domains == nil {
		domains = orderedmap.New[string, *model.Domain]()
	}

	doc := &Document{ParseResult: r}

	if r.Tables != nil {
		doc.Tables = orderedmap.New[string, *Table]()
		for key, t := range r.Tables.All() {
			doc.Tables.Set(key, newTable(t, domains, schemas))
		}
	}

	if r.Views != nil {
		doc.Views = orderedmap.New[string, *View]()
		for key, v := range r.Views.All() {
			doc.Views.Set(key, &View{View: v, Indexes: newIndexes(v.Indexes)})
		}
	}

	return doc
}

func newTable(t *model.Table, domains *orderedmap.Map[string, *model.Domain], schemas []string) *Table {
	table := &Table{Table: t, Indexes: newIndexes(t.Indexes)}

	if t.Columns != nil {
		table.Columns = []*Column{}
		for col := range t.Columns.Values() {
			c := &Column{Column: col}
			c.BaseType, c.IsArray = baseType(col.TypeName, domains, schemas)
			table.Columns = append(table.Columns, c)
		}
	}

	if t.ForeignKeys != nil {
		table.ForeignKeys = orderedmap.New[string, *ForeignKey]()
		for key, fk := range t.ForeignKeys.All() {
			table.ForeignKeys.Set(key, newForeignKey(fk))
		}
	}

	return table
}

// baseType returns the type name without its modifier and array marker, with
// a domain replaced by the type it is based on and a serial type by its
// integer type. It also reports whether the type is an array.
func baseType(typeName string, domains *orderedmap.Map[string, *model.Domain], schemas []string) (string, bool) {
	isArray := false
	seen := map[string]bool{}

	for {
		name, array := stripTypeSuffixes(typeName)
		isArray = isArray || array

		switch name {
		case "smallserial":
			return "smallint", isArray
		case "serial":
			return "integer", isArray
		case "bigserial":
			return "bigint", isArray
		}

		d := lookupDomain(name, domains, schemas)
		if d == nil || seen[name] {
			return name, isArray
		}
		seen[name] = true
		typeName = d.BaseType
	}
}

// stripTypeSuffixes removes the modifiers and the array marker from a type
// name, and reports whether there was an array marker. Text inside double
// quotes is kept. "timestamp(3) without time zone" becomes
// "timestamp without time zone".
func stripTypeSuffixes(typeName string) (string, bool) {
	var b strings.Builder
	quoted := false
	depth := 0

	for i := 0; i < len(typeName); i++ {
		c := typeName[i]
		switch {
		case c == '"' && depth == 0:
			quoted = !quoted
			b.WriteByte(c)
		case quoted:
			b.WriteByte(c)
		case c == '(':
			depth++
		case c == ')' && depth > 0:
			depth--
		case depth > 0:
		case c == '[':
			return strings.TrimSpace(b.String()), true
		default:
			b.WriteByte(c)
		}
	}

	return strings.TrimSpace(b.String()), false
}

// lookupDomain returns the domain that a type name refers to, or nil. An
// unqualified name is looked up in each schema in turn.
func lookupDomain(name string, domains *orderedmap.Map[string, *model.Domain], schemas []string) *model.Domain {
	if d, ok := domains.GetOk(name); ok {
		return d
	}

	for _, s := range schemas {
		if d, ok := domains.GetOk(model.Ident(s) + "." + name); ok {
			return d
		}
	}

	return nil
}

func newIndexes(indexes *orderedmap.Map[string, *model.Index]) *orderedmap.Map[string, *Index] {
	if indexes == nil {
		return nil
	}

	m := orderedmap.New[string, *Index]()
	for key, idx := range indexes.All() {
		m.Set(key, newIndex(idx))
	}

	return m
}

func newIndex(idx *model.Index) *Index {
	index := &Index{Index: idx, Columns: []*string{}, Include: []string{}}

	result, err := pg_query.Parse(idx.Definition)
	if err != nil || len(result.Stmts) != 1 {
		return index
	}
	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil {
		return index
	}

	for _, p := range is.IndexParams {
		var name *string
		if ie := p.GetIndexElem(); ie != nil && ie.Name != "" {
			name = &ie.Name
		}
		index.Columns = append(index.Columns, name)
	}

	for _, p := range is.IndexIncludingParams {
		if ie := p.GetIndexElem(); ie != nil {
			index.Include = append(index.Include, ie.Name)
		}
	}

	index.Unique = is.Unique
	index.Method = is.AccessMethod
	index.Partial = is.WhereClause != nil

	return index
}

var fkActions = map[string]string{
	"a": "no action",
	"r": "restrict",
	"c": "cascade",
	"n": "set null",
	"d": "set default",
}

var fkMatchTypes = map[string]string{
	"s": "simple",
	"f": "full",
}

func newForeignKey(fk *model.ForeignKey) *ForeignKey {
	key := &ForeignKey{ForeignKey: fk, RefColumns: []string{}}

	// The definition is only the constraint clause, so parse it inside an
	// ALTER TABLE.
	result, err := pg_query.Parse("ALTER TABLE t ADD CONSTRAINT c " + fk.Definition)
	if err != nil || len(result.Stmts) != 1 {
		return key
	}
	cmds := result.Stmts[0].Stmt.GetAlterTableStmt().GetCmds()
	if len(cmds) != 1 {
		return key
	}
	con := cmds[0].GetAlterTableCmd().GetDef().GetConstraint()
	if con == nil || con.Contype != pg_query.ConstrType_CONSTR_FOREIGN {
		return key
	}

	for _, n := range con.PkAttrs {
		if s := n.GetString_(); s != nil {
			key.RefColumns = append(key.RefColumns, s.Sval)
		}
	}

	key.OnDelete = fkActions[con.FkDelAction]
	key.OnUpdate = fkActions[con.FkUpdAction]
	key.Match = fkMatchTypes[con.FkMatchtype]

	return key
}
