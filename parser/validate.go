package parser

import (
	"errors"
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/internal/pgast"
	"github.com/winebarrel/pistachio/model"
)

// validateColumnRefs returns an error if any column DEFAULT or GENERATED
// expression, index, constraint, or foreign-key definition on a desired table
// references a column that does not exist in that table's desired column set.
// All violations across all tables are aggregated via errors.Join so a single
// plan run reports every problem. Within a single object (column expression /
// index / constraint / FK), each missing column name is reported at most once
// so multiple references to the same name don't produce duplicate error
// lines.
//
// Scope: only same-table references are checked. Foreign-key referenced
// columns (PkAttrs, on the parent table) are out of scope. Tables that
// inherit columns from a parent (declarative partition children and
// INHERITS-style children) are skipped.
func validateColumnRefs(tables *orderedmap.Map[string, *model.Table]) error {
	var errs []error
	for fqtn, t := range tables.All() {
		// Ignored tables are unmanaged, so their internal consistency is not
		// checked.
		if t.Ignore {
			continue
		}
		// Skip both partition children (PartitionOf + PartitionBound set) and
		// INHERITS-style children (PartitionOf set, PartitionBound nil); both
		// inherit their columns from the parent rather than declaring their
		// own complete column list.
		if t.PartitionOf != nil {
			continue
		}

		cols := make(map[string]bool, t.Columns.Len())
		for name := range t.Columns.Keys() {
			cols[name] = true
		}

		for name, col := range t.Columns.All() {
			if col.Default == nil {
				continue
			}
			kind := "DEFAULT"
			if col.Generated.IsGeneratedColumn() {
				kind = "GENERATED"
			}
			reportMissing(&errs, cols, collectColumnRefsInColumnExpr(*col.Default), func(ref string) error {
				return fmt.Errorf("column %s referenced in the %s expression on column %s does not exist on table %s",
					model.Ident(ref), kind, model.Ident(name), fqtn)
			})
		}

		for _, idx := range t.Indexes.CollectValues() {
			reportMissing(&errs, cols, collectColumnRefsInIndexDef(idx.Definition), func(ref string) error {
				return fmt.Errorf("column %s referenced in index %s does not exist on table %s",
					model.Ident(ref), model.Ident(idx.Name), fqtn)
			})
		}

		for _, con := range t.Constraints.CollectValues() {
			kind := constraintKindLabel(con.Type)
			reportMissing(&errs, cols, collectColumnRefsInConstraintDef(con.Definition), func(ref string) error {
				return fmt.Errorf("column %s referenced in %s %s does not exist on table %s",
					model.Ident(ref), kind, model.Ident(con.Name), fqtn)
			})
		}

		for _, fk := range t.ForeignKeys.CollectValues() {
			reportMissing(&errs, cols, collectColumnRefsInFKDef(fk.Definition), func(ref string) error {
				return fmt.Errorf("column %s referenced in foreign key %s does not exist on table %s",
					model.Ident(ref), model.Ident(fk.Name), fqtn)
			})
		}
	}
	return errors.Join(errs...)
}

// reportMissing appends one error per distinct refs[i] that is not in cols,
// preserving first-encounter order so the aggregated error is deterministic.
func reportMissing(errs *[]error, cols map[string]bool, refs []string, mkErr func(string) error) {
	seen := map[string]bool{}
	for _, ref := range refs {
		if cols[ref] || seen[ref] {
			continue
		}
		seen[ref] = true
		*errs = append(*errs, mkErr(ref))
	}
}

func constraintKindLabel(ct model.ConstraintType) string {
	switch {
	case ct.IsCheckConstraint():
		return "CHECK constraint"
	case ct.IsPrimaryKeyConstraint():
		return "PRIMARY KEY constraint"
	case ct.IsUniqueConstraint():
		return "UNIQUE constraint"
	case ct.IsForeignKeyConstraint():
		return "FOREIGN KEY constraint"
	case ct.IsExclusionConstraint():
		return "EXCLUDE constraint"
	default:
		return "constraint"
	}
}

// collectColumnRefsInIndexDef returns the unqualified column names referenced
// by an index definition (IndexParams, IndexIncludingParams, WhereClause).
// Returns nil on parse errors so validation degrades to a no-op for
// unparsable definitions.
func collectColumnRefsInIndexDef(def string) []string {
	result, err := pg_query.Parse(def)
	if err != nil || len(result.Stmts) == 0 {
		return nil
	}
	is := result.Stmts[0].Stmt.GetIndexStmt()
	if is == nil {
		return nil
	}
	var refs []string
	collect := func(params []*pg_query.Node) {
		for _, p := range params {
			ie := p.GetIndexElem()
			if ie == nil {
				continue
			}
			if ie.Name != "" {
				refs = append(refs, ie.Name)
			}
			refs = append(refs, walkExprColumnRefs(ie.Expr)...)
		}
	}
	collect(is.IndexParams)
	collect(is.IndexIncludingParams)
	refs = append(refs, walkExprColumnRefs(is.WhereClause)...)
	return refs
}

// collectColumnRefsInConstraintDef returns the unqualified column names
// referenced by a constraint definition fragment (Keys, Including, RawExpr,
// EXCLUDE Exclusions IndexElem).
func collectColumnRefsInConstraintDef(def string) []string {
	con := pgast.ParseConstraintDef(def)
	if con == nil {
		return nil
	}
	var refs []string
	collectStringList := func(nodes []*pg_query.Node) {
		for _, n := range nodes {
			if s := n.GetString_(); s != nil && s.Sval != "" {
				refs = append(refs, s.Sval)
			}
		}
	}
	collectStringList(con.Keys)
	collectStringList(con.Including)
	refs = append(refs, walkExprColumnRefs(con.RawExpr)...)
	for _, ex := range con.Exclusions {
		list := ex.GetList()
		if list == nil {
			continue
		}
		for _, item := range list.Items {
			ie := item.GetIndexElem()
			if ie == nil {
				continue
			}
			if ie.Name != "" {
				refs = append(refs, ie.Name)
			}
			refs = append(refs, walkExprColumnRefs(ie.Expr)...)
		}
	}
	return refs
}

// collectColumnRefsInFKDef returns the local-side column names (FkAttrs)
// referenced by a foreign-key definition. PkAttrs (parent-table columns) are
// intentionally excluded; cross-table validation is out of scope.
func collectColumnRefsInFKDef(def string) []string {
	con := pgast.ParseConstraintDef(def)
	if con == nil {
		return nil
	}
	var refs []string
	for _, n := range con.FkAttrs {
		if s := n.GetString_(); s != nil && s.Sval != "" {
			refs = append(refs, s.Sval)
		}
	}
	return refs
}

// collectColumnRefsInColumnExpr returns the unqualified column names
// referenced by a column DEFAULT or GENERATED expression, which is stored as
// a bare expression rather than a statement. Returns nil on parse errors so
// validation degrades to a no-op for unparsable expressions.
//
// A generated expression may reference only columns of its own table, so every
// name it carries belongs to the set checked here. A plain DEFAULT may
// reference no column at all, which PostgreSQL rejects on its own; the names
// it does carry are checked the same way rather than rejected outright.
//
// Only unqualified names are read, as they are for an index or a constraint.
// A generated expression may write one qualified, which the diff cannot
// compare at all; TODO.md records that.
func collectColumnRefsInColumnExpr(expr string) []string {
	_, target, err := pgast.ParseExpr(expr)
	if err != nil {
		return nil
	}
	return walkExprColumnRefs(target.Val)
}

// walkExprColumnRefs returns the unqualified ColumnRef names found in an
// expression tree.
func walkExprColumnRefs(node *pg_query.Node) []string {
	var refs []string
	pgast.WalkExprColumnRefs(node, func(s *pg_query.String) {
		if s.Sval != "" {
			refs = append(refs, s.Sval)
		}
	})
	return refs
}

// namespace tracks the names registered in one PostgreSQL catalog, mapping
// each name to the kind of object that claimed it. scope names the object the
// names belong to, for a catalog that keys them per object rather than per
// schema; it is empty otherwise.
type namespace struct {
	catalog string
	scope   string
	names   map[string]string
}

func newNamespace(catalog string) *namespace {
	return &namespace{catalog: catalog, names: map[string]string{}}
}

func newScopedNamespace(catalog, scope string) *namespace {
	return &namespace{catalog: catalog, scope: scope, names: map[string]string{}}
}

func (ns *namespace) add(name, kind string) error {
	if prev, ok := ns.names[name]; ok {
		on := ""
		if ns.scope != "" {
			on = " on " + ns.scope
		}
		// Two objects of the same kind reach here when their own map is keyed
		// by something narrower than the catalog: index names are unique per
		// schema in PostgreSQL, but pistachio keys them per table. The message
		// matches setUnique's, which reports the same clash within one table.
		if prev == kind {
			return fmt.Errorf("duplicate %s: %s%s", kind, name, on)
		}
		return fmt.Errorf("duplicate %s name: %s%s (%s and %s)", ns.catalog, name, on, prev, kind)
	}
	ns.names[name] = kind
	return nil
}

// validateNamespaces returns an error when one name is claimed twice within a
// PostgreSQL catalog: by two objects in a schema, or by two members of a single
// object. Left in, the file fails partway through an apply with
// `relation "x" already exists`, `type "x" already exists`, or
// `constraint "c" for relation "t" already exists`.
//
// pg_class holds tables, views, materialized views, sequences, composite types
// and indexes. The indexes include the ones PostgreSQL builds for PRIMARY KEY,
// UNIQUE and EXCLUDE constraints, which take the constraint name; CHECK and
// foreign-key constraints build no index, so their names are not registered.
// pg_type holds a row for every table, view, materialized view and composite
// type, plus domains and enums. Sequences and indexes have no type entry.
//
// pg_constraint is unique on (conrelid, contypid, conname), so a constraint
// name is scoped to the table or the domain that owns it rather than to the
// schema, and each of those gets a namespace of its own. Every constraint kind
// shares it, including the CHECK and foreign-key names pg_class does not see.
//
// pg_attribute and pg_enum scope a composite type's attribute names and an
// enum's labels to that one type. Such a namespace only ever sees one kind, so
// its catalog label never reaches a message.
//
// setUnique cannot stand in for any of this. Constraints and ForeignKeys are
// separate maps, so it compares a constraint name only against the same kind,
// and Domain.Constraints, CompositeType.Attributes and Enum.Values are plain
// slices with no uniqueness check at all.
//
// Ignored objects are checked as well, since an unmanaged relation still
// occupies its name in the database. All violations are aggregated via
// errors.Join, one line per object.
func validateNamespaces(r *ParseResult) error {
	var errs []error
	relations := newNamespace("relation")
	types := newNamespace("type")

	add := func(name, kind string, nss ...*namespace) {
		for _, ns := range nss {
			if err := ns.add(name, kind); err != nil {
				// A table and a composite type clash in both catalogs; the
				// first rejection is the whole report for this object.
				errs = append(errs, err)
				return
			}
		}
	}

	addIndexes := func(indexes *orderedmap.Map[string, *model.Index]) {
		for _, idx := range indexes.CollectValues() {
			add(model.Ident(idx.Schema, idx.Name), "index", relations)
		}
	}

	for fqtn, t := range r.Tables.All() {
		add(fqtn, "table", relations, types)
		addIndexes(t.Indexes)
		constraints := newScopedNamespace("constraint", fqtn)
		for _, con := range t.Constraints.CollectValues() {
			add(model.Ident(con.Name), constraintKindLabel(con.Type), constraints)
			if !con.Type.IsPrimaryKeyConstraint() && !con.Type.IsUniqueConstraint() && !con.Type.IsExclusionConstraint() {
				continue
			}
			add(model.Ident(t.Schema, con.Name), constraintKindLabel(con.Type), relations)
		}
		for _, fk := range t.ForeignKeys.CollectValues() {
			add(model.Ident(fk.Name), constraintKindLabel(fk.Type), constraints)
		}
	}

	for fqvn, v := range r.Views.All() {
		kind := "view"
		if v.Materialized {
			kind = "materialized view"
		}
		add(fqvn, kind, relations, types)
		addIndexes(v.Indexes)
	}

	for name := range r.Sequences.Keys() {
		add(name, "sequence", relations)
	}

	for fqcn, ct := range r.CompositeTypes.All() {
		add(fqcn, "composite type", relations, types)
		attributes := newScopedNamespace("attribute", fqcn)
		for _, attr := range ct.Attributes {
			add(model.Ident(attr.Name), "attribute", attributes)
		}
	}

	for fqdn, d := range r.Domains.All() {
		add(fqdn, "domain", types)
		constraints := newScopedNamespace("constraint", fqdn)
		for _, con := range d.Constraints {
			// A domain constraint is always CHECK; NOT NULL is a flag on the
			// domain rather than an entry here.
			add(model.Ident(con.Name), "CHECK constraint", constraints)
		}
	}

	for fqen, e := range r.Enums.All() {
		add(fqen, "enum", types)
		// Labels are literals, not identifiers, so they are not quoted.
		values := newScopedNamespace("enum value", fqen)
		for _, v := range e.Values {
			add(v, "enum value", values)
		}
	}

	return errors.Join(errs...)
}
