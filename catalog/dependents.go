package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/model"
)

// Dependent is an object that reads a view or a materialized view. PostgreSQL
// refuses to drop a relation while one of these exists rather than cascading,
// so a plan that drops the relation has to account for it.
type Dependent struct {
	// Kind is what PostgreSQL calls the object: "view", "materialized view",
	// "function", and so on.
	Kind string
	// Name is the object's identity, schema-qualified. A view or materialized
	// view is written the way the rest of the model writes a name; anything
	// else the way PostgreSQL identifies it, so a function carries its
	// argument types and a rule the relation it sits on.
	Name string
	// Relation is Name when the dependent is a view or a materialized view,
	// and "" otherwise. A plan that drops that relation too is not blocked by
	// it, since the drops run deepest first. A rule on a table records the
	// dependency a view does, so this stays empty for one: the field is what
	// the caller matches against the views it drops.
	Relation string
	// Constraint is <table>.<constraint> for a foreign key and "" otherwise.
	Constraint string
}

func (d Dependent) String() string {
	return d.Kind + " " + d.Name
}

// ViewDependents reads what depends on each view and materialized view in the
// managed schemas, keyed by the same schema-qualified name the Views map uses.
// A relation nothing reads is absent rather than present and empty.
//
// Only the direct dependents are read. A view two steps away depends on the
// view between them, and that one has to be dropped for the chain to come
// apart, so the caller meets it there. A dependent outside the managed schemas
// is included, since it blocks the drop just the same.
//
// The rewrite rule a view holds over itself is left out. Every view depends on
// its own columns that way, and the rule goes with the view.
func (c *Catalog) ViewDependents(ctx context.Context) (map[string][]Dependent, error) {
	// A view records its dependency through the rewrite rule it holds, so the
	// rule is resolved back to the relation it sits on and the dependent is
	// named as that relation. A rule on a plain table records the same kind of
	// row, and falls through to pg_identify_object with the rest, which names
	// it the way PostgreSQL's own error does.
	//
	// A relation is reached two ways. Most dependents record the relation
	// itself, and a routine that returns the view's row type records the type
	// that relation owns, which blocks the drop the same way. Both resolve to
	// the same target here.
	//
	// DISTINCT because a dependent reading several columns of the target has a
	// pg_depend row per column, and one that reads the relation and returns
	// its row type has a row for each.
	q := `
		SELECT DISTINCT
			tn.nspname,
			t.relname,
			CASE dc.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' ELSE (oi).type END,
			CASE WHEN dc.relkind IN ('v', 'm') THEN dn.nspname END,
			CASE WHEN dc.relkind IN ('v', 'm') THEN dc.relname ELSE (oi).identity END
		FROM
			pg_catalog.pg_depend d
			LEFT JOIN pg_catalog.pg_type rt ON rt.oid = d.refobjid AND d.refclassid = 'pg_catalog.pg_type'::regclass
			JOIN pg_catalog.pg_class t ON t.oid = CASE
				WHEN d.refclassid = 'pg_catalog.pg_class'::regclass THEN d.refobjid
				ELSE rt.typrelid
			END
			JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace
			CROSS JOIN LATERAL pg_catalog.pg_identify_object(d.classid, d.objid, 0) oi
			LEFT JOIN pg_catalog.pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_catalog.pg_rewrite'::regclass
			LEFT JOIN pg_catalog.pg_class dc ON dc.oid = r.ev_class
			LEFT JOIN pg_catalog.pg_namespace dn ON dn.oid = dc.relnamespace
		WHERE
			d.refclassid IN ('pg_catalog.pg_class'::regclass, 'pg_catalog.pg_type'::regclass)
			AND d.deptype = 'n'
			AND t.relkind IN ('v', 'm')
			AND tn.nspname = ANY(@schemas)
			AND (r.oid IS NULL OR r.ev_class <> t.oid)
		ORDER BY
			1, 2, 3, 4, 5
	`

	dependents := map[string][]Dependent{}
	var targetSchema, targetName, kind, name string
	var depSchema *string
	err := c.eachRow(ctx, "view dependents", q, []any{&targetSchema, &targetName, &kind, &depSchema, &name}, func() {
		dep := Dependent{Kind: kind}
		if depSchema != nil {
			dep.Name = model.Ident(*depSchema, name)
			dep.Relation = dep.Name
		} else {
			dep.Name = name
		}
		target := model.Ident(targetSchema, targetName)
		dependents[target] = append(dependents[target], dep)
	})
	if err != nil {
		return nil, err
	}
	return dependents, nil
}

// ColumnDependents reads what blocks a type change of each column in the
// managed schemas, keyed by <table>.<column>. PostgreSQL refuses ALTER
// COLUMN ... TYPE while a view, a rule, a trigger, a policy, a routine or a
// generated column depends on the column. Indexes, constraints and
// statistics objects are rebuilt instead and are not read.
func (c *Catalog) ColumnDependents(ctx context.Context) (map[string][]Dependent, error) {
	q := `
		SELECT DISTINCT
			tn.nspname,
			t.relname,
			a.attname,
			CASE
				WHEN dc.relkind = 'v' THEN 'view'
				WHEN dc.relkind = 'm' THEN 'materialized view'
				WHEN d.classid = 'pg_catalog.pg_attrdef'::regclass THEN 'generated column'
				ELSE (oi).type
			END,
			CASE WHEN dc.relkind IN ('v', 'm') THEN dn.nspname END,
			CASE WHEN dc.relkind IN ('v', 'm') THEN dc.relname END,
			ga.attname,
			(oi).identity
		FROM
			pg_catalog.pg_depend d
			JOIN pg_catalog.pg_class t ON t.oid = d.refobjid
			JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace
			JOIN pg_catalog.pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
			CROSS JOIN LATERAL pg_catalog.pg_identify_object(d.classid, d.objid, 0) oi
			LEFT JOIN pg_catalog.pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_catalog.pg_rewrite'::regclass
			LEFT JOIN pg_catalog.pg_class dc ON dc.oid = r.ev_class
			LEFT JOIN pg_catalog.pg_namespace dn ON dn.oid = dc.relnamespace
			LEFT JOIN pg_catalog.pg_attrdef ad ON ad.oid = d.objid AND d.classid = 'pg_catalog.pg_attrdef'::regclass
			LEFT JOIN pg_catalog.pg_attribute ga ON ga.attrelid = ad.adrelid AND ga.attnum = ad.adnum
		WHERE
			d.refclassid = 'pg_catalog.pg_class'::regclass
			AND d.refobjsubid > 0
			AND d.deptype = 'n'
			AND d.classid IN (
				'pg_catalog.pg_rewrite'::regclass,
				'pg_catalog.pg_trigger'::regclass,
				'pg_catalog.pg_policy'::regclass,
				'pg_catalog.pg_proc'::regclass,
				'pg_catalog.pg_attrdef'::regclass
			)
			AND t.relkind IN ('r', 'p')
			AND tn.nspname = ANY(@schemas)
		ORDER BY
			1, 2, 3, 4, 5, 6, 7, 8
	`

	dependents := map[string][]Dependent{}
	var targetSchema, targetName, column, kind, identity string
	var depSchema, depRelation, generated *string
	err := c.eachRow(ctx, "column dependents", q, []any{&targetSchema, &targetName, &column, &kind, &depSchema, &depRelation, &generated, &identity}, func() {
		target := model.Ident(targetSchema, targetName)
		dep := Dependent{Kind: kind, Name: identity}
		switch {
		case depRelation != nil:
			dep.Name = model.Ident(*depSchema, *depRelation)
			dep.Relation = dep.Name
		case generated != nil:
			dep.Name = target + "." + model.Ident(*generated)
		}
		key := target + "." + model.Ident(column)
		dependents[key] = append(dependents[key], dep)
	})
	if err != nil {
		return nil, err
	}
	return dependents, nil
}

// KeyDependents reads what blocks dropping a key or a plain index in the
// managed schemas, keyed by <table>.<constraint> and by index name. A foreign
// key depends on the referenced key's index, and a view that groups by a
// primary key depends on the constraint.
func (c *Catalog) KeyDependents(ctx context.Context) (map[string][]Dependent, map[string][]Dependent, error) {
	q := `
		WITH
			targets AS (
				SELECT
					con.oid AS con_oid,
					con.conindid AS idx_oid,
					n.nspname,
					t.relname AS table_name,
					con.conname AS name,
					true AS is_constraint
				FROM
					pg_catalog.pg_constraint con
					JOIN pg_catalog.pg_class t ON t.oid = con.conrelid
					JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
				WHERE
					con.contype IN ('p', 'u', 'x')
					AND n.nspname = ANY(@schemas)
				UNION ALL
				SELECT
					NULL,
					i.indexrelid,
					n.nspname,
					t.relname,
					ci.relname,
					false
				FROM
					pg_catalog.pg_index i
					JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
					JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
					JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace
				WHERE
					n.nspname = ANY(@schemas)
					-- A key's own index is reached through the key. A foreign
					-- key's conindid names the index it references, so it is
					-- not an owner.
					AND NOT EXISTS (
						SELECT FROM pg_catalog.pg_constraint con
						WHERE con.conindid = i.indexrelid AND con.contype IN ('p', 'u', 'x')
					)
			)
		SELECT DISTINCT
			tg.is_constraint,
			tg.nspname,
			tg.table_name,
			tg.name,
			CASE
				WHEN dc.relkind = 'v' THEN 'view'
				WHEN dc.relkind = 'm' THEN 'materialized view'
				WHEN fk.contype = 'f' THEN 'foreign key'
				ELSE (oi).type
			END,
			CASE WHEN dc.relkind IN ('v', 'm') THEN dn.nspname END,
			CASE WHEN dc.relkind IN ('v', 'm') THEN dc.relname END,
			CASE WHEN fk.contype = 'f' THEN fkn.nspname END,
			CASE WHEN fk.contype = 'f' THEN fkt.relname END,
			CASE WHEN fk.contype = 'f' THEN fk.conname END,
			(oi).identity
		FROM
			targets tg
			JOIN pg_catalog.pg_depend d ON d.deptype = 'n' AND (
				(d.refclassid = 'pg_catalog.pg_class'::regclass AND d.refobjid = tg.idx_oid)
				OR (d.refclassid = 'pg_catalog.pg_constraint'::regclass AND d.refobjid = tg.con_oid)
			)
			CROSS JOIN LATERAL pg_catalog.pg_identify_object(d.classid, d.objid, 0) oi
			LEFT JOIN pg_catalog.pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_catalog.pg_rewrite'::regclass
			LEFT JOIN pg_catalog.pg_class dc ON dc.oid = r.ev_class
			LEFT JOIN pg_catalog.pg_namespace dn ON dn.oid = dc.relnamespace
			LEFT JOIN pg_catalog.pg_constraint fk ON fk.oid = d.objid AND d.classid = 'pg_catalog.pg_constraint'::regclass
			LEFT JOIN pg_catalog.pg_class fkt ON fkt.oid = fk.conrelid
			LEFT JOIN pg_catalog.pg_namespace fkn ON fkn.oid = fkt.relnamespace
		WHERE
			-- A partition's copy of a foreign key goes with the parent's,
			-- which is listed on its own.
			fk.conparentid IS NULL OR fk.conparentid = 0
		ORDER BY
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
	`

	constraints := map[string][]Dependent{}
	indexes := map[string][]Dependent{}
	var isConstraint bool
	var targetSchema, targetTable, targetName, kind, identity string
	var viewSchema, viewName, fkSchema, fkTable, fkName *string
	dest := []any{
		&isConstraint, &targetSchema, &targetTable, &targetName, &kind,
		&viewSchema, &viewName, &fkSchema, &fkTable, &fkName, &identity,
	}
	err := c.eachRow(ctx, "key dependents", q, dest, func() {
		dep := Dependent{Kind: kind, Name: identity}
		switch {
		case viewName != nil:
			dep.Name = model.Ident(*viewSchema, *viewName)
			dep.Relation = dep.Name
		case fkName != nil:
			dep.Constraint = model.Ident(*fkSchema, *fkTable) + "." + model.Ident(*fkName)
		}
		if isConstraint {
			key := model.Ident(targetSchema, targetTable) + "." + model.Ident(targetName)
			constraints[key] = append(constraints[key], dep)
		} else {
			key := model.Ident(targetSchema, targetName)
			indexes[key] = append(indexes[key], dep)
		}
	})
	if err != nil {
		return nil, nil, err
	}
	return constraints, indexes, nil
}

// RoutineDependents reads what blocks dropping each routine in the managed
// schemas, keyed by the routine's OID.
func (c *Catalog) RoutineDependents(ctx context.Context) (map[uint32][]Dependent, error) {
	q := `
		SELECT DISTINCT
			p.oid,
			CASE
				WHEN dc.relkind = 'v' THEN 'view'
				WHEN dc.relkind = 'm' THEN 'materialized view'
				WHEN ga.attgenerated <> '' THEN 'generated column'
				ELSE (oi).type
			END,
			CASE WHEN dc.relkind IN ('v', 'm') THEN dn.nspname END,
			CASE WHEN dc.relkind IN ('v', 'm') THEN dc.relname END,
			CASE WHEN ga.attgenerated <> '' THEN gn.nspname END,
			CASE WHEN ga.attgenerated <> '' THEN gt.relname END,
			CASE WHEN ga.attgenerated <> '' THEN ga.attname END,
			(oi).identity
		FROM
			pg_catalog.pg_proc p
			JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
			JOIN pg_catalog.pg_depend d ON d.refclassid = 'pg_catalog.pg_proc'::regclass
			AND d.refobjid = p.oid
			AND d.deptype = 'n'
			CROSS JOIN LATERAL pg_catalog.pg_identify_object(d.classid, d.objid, 0) oi
			LEFT JOIN pg_catalog.pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_catalog.pg_rewrite'::regclass
			LEFT JOIN pg_catalog.pg_class dc ON dc.oid = r.ev_class
			LEFT JOIN pg_catalog.pg_namespace dn ON dn.oid = dc.relnamespace
			LEFT JOIN pg_catalog.pg_attrdef ad ON ad.oid = d.objid AND d.classid = 'pg_catalog.pg_attrdef'::regclass
			LEFT JOIN pg_catalog.pg_attribute ga ON ga.attrelid = ad.adrelid AND ga.attnum = ad.adnum
			LEFT JOIN pg_catalog.pg_class gt ON gt.oid = ga.attrelid
			LEFT JOIN pg_catalog.pg_namespace gn ON gn.oid = gt.relnamespace
		WHERE
			n.nspname = ANY(@schemas)
		ORDER BY
			1, 2, 3, 4, 5, 6, 7, 8
	`

	dependents := map[uint32][]Dependent{}
	var oid uint32
	var kind, identity string
	var viewSchema, viewName, genSchema, genTable, genColumn *string
	err := c.eachRow(ctx, "routine dependents", q, []any{&oid, &kind, &viewSchema, &viewName, &genSchema, &genTable, &genColumn, &identity}, func() {
		dep := Dependent{Kind: kind, Name: identity}
		switch {
		case viewName != nil:
			dep.Name = model.Ident(*viewSchema, *viewName)
			dep.Relation = dep.Name
		case genColumn != nil:
			dep.Name = model.Ident(*genSchema, *genTable) + "." + model.Ident(*genColumn)
		}
		dependents[oid] = append(dependents[oid], dep)
	})
	if err != nil {
		return nil, err
	}
	return dependents, nil
}

// eachRow runs q over the managed schemas, scans each row into dest and calls
// fn after each scan. what names the query in an error.
func (c *Catalog) eachRow(ctx context.Context, what, q string, dest []any, fn func()) error {
	rows, err := c.conn.Query(ctx, q, pgx.NamedArgs{"schemas": c.schemas})
	if err != nil {
		return fmt.Errorf("catalog: failed to get %s: %w", what, err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("catalog: failed to scan %s: %w", what, err)
		}
		fn()
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("catalog: failed to scan %s rows: %w", what, err)
	}
	return nil
}
