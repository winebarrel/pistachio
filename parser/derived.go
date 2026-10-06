package parser

import (
	"fmt"
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// FillDerived sets the fields that repeat part of another field in a form that
// needs no SQL parsing: a column's base type, and the parts of an index and a
// foreign key definition. It reads only the model, so a document built from
// the catalog gets the same values as one built from files.
//
// An unqualified domain name is looked up in schemas and then in public, the
// search path that plan, apply and dump set.
func (r *ParseResult) FillDerived(schemas []string) error {
	if !slices.Contains(schemas, "public") {
		schemas = append(slices.Clip(schemas), "public")
	}

	var domains *orderedmap.Map[string, *model.Domain]
	if r.Domains != nil {
		domains = r.Domains
	} else {
		domains = orderedmap.New[string, *model.Domain]()
	}

	if r.Tables != nil {
		for _, t := range r.Tables.All() {
			if t.Columns != nil {
				for _, col := range t.Columns.All() {
					col.BaseType, col.IsArray = baseType(col.TypeName, domains, schemas)
				}
			}
			if err := fillIndexes(t.Indexes); err != nil {
				return err
			}
			if t.ForeignKeys != nil {
				for _, fk := range t.ForeignKeys.All() {
					if err := fillForeignKey(fk); err != nil {
						return fmt.Errorf("failed to read foreign key %s on %s: %w", fk.Name, t.FQTN(), err)
					}
				}
			}
		}
	}

	if r.Views != nil {
		for _, v := range r.Views.All() {
			if err := fillIndexes(v.Indexes); err != nil {
				return err
			}
		}
	}

	return nil
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

func fillIndexes(indexes *orderedmap.Map[string, *model.Index]) error {
	if indexes == nil {
		return nil
	}

	for _, idx := range indexes.All() {
		if err := fillIndex(idx); err != nil {
			return fmt.Errorf("failed to read index %s: %w", model.Ident(idx.Schema, idx.Name), err)
		}
	}

	return nil
}

func fillIndex(idx *model.Index) error {
	result, err := pg_query.Parse(idx.Definition)
	if err != nil {
		return err
	}

	var is *pg_query.IndexStmt
	if len(result.Stmts) == 1 {
		is = result.Stmts[0].Stmt.GetIndexStmt()
	}
	if is == nil {
		return fmt.Errorf("not a CREATE INDEX: %s", idx.Definition)
	}

	idx.Columns = []*string{}
	for _, p := range is.IndexParams {
		var name *string
		if ie := p.GetIndexElem(); ie != nil && ie.Name != "" {
			name = &ie.Name
		}
		idx.Columns = append(idx.Columns, name)
	}

	idx.Include = []string{}
	for _, p := range is.IndexIncludingParams {
		if ie := p.GetIndexElem(); ie != nil {
			idx.Include = append(idx.Include, ie.Name)
		}
	}

	idx.Unique = is.Unique
	idx.Method = is.AccessMethod
	idx.Partial = is.WhereClause != nil

	return nil
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

func fillForeignKey(fk *model.ForeignKey) error {
	// The definition is only the constraint clause, so parse it inside an
	// ALTER TABLE.
	result, err := pg_query.Parse("ALTER TABLE t ADD CONSTRAINT c " + fk.Definition)
	if err != nil {
		return err
	}

	var con *pg_query.Constraint
	if len(result.Stmts) == 1 {
		if at := result.Stmts[0].Stmt.GetAlterTableStmt(); at != nil && len(at.Cmds) == 1 {
			con = at.Cmds[0].GetAlterTableCmd().GetDef().GetConstraint()
		}
	}
	if con == nil || con.Contype != pg_query.ConstrType_CONSTR_FOREIGN {
		return fmt.Errorf("not a foreign key: %s", fk.Definition)
	}

	fk.RefColumns = []string{}
	for _, n := range con.PkAttrs {
		if s := n.GetString_(); s != nil {
			fk.RefColumns = append(fk.RefColumns, s.Sval)
		}
	}

	fk.OnDelete = fkActions[con.FkDelAction]
	fk.OnUpdate = fkActions[con.FkUpdAction]
	fk.Match = fkMatchTypes[con.FkMatchtype]

	return nil
}
