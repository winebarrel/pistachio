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

	rows, err := c.conn.Query(ctx, q, pgx.NamedArgs{"schemas": c.schemas})
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get view dependents: %w", err)
	}
	defer rows.Close()

	dependents := map[string][]Dependent{}
	for rows.Next() {
		var targetSchema, targetName, kind, name string
		var depSchema *string
		if err := rows.Scan(&targetSchema, &targetName, &kind, &depSchema, &name); err != nil {
			return nil, fmt.Errorf("catalog: failed to scan view dependents: %w", err)
		}
		dep := Dependent{Kind: kind}
		if depSchema != nil {
			dep.Name = model.Ident(*depSchema, name)
			dep.Relation = dep.Name
		} else {
			dep.Name = name
		}
		target := model.Ident(targetSchema, targetName)
		dependents[target] = append(dependents[target], dep)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan view dependents rows: %w", err)
	}
	return dependents, nil
}

// ColumnDependents reads what blocks a type change of each column of a table
// in the managed schemas, keyed by the table's schema-qualified name, a dot
// and the column name. A column nothing blocks is absent.
//
// PostgreSQL rebuilds an index, a constraint or a statistics object on the
// column as part of ALTER COLUMN ... TYPE, and refuses the change while a
// view or a rule, a trigger, a policy, a routine or a generated column
// depends on it. Only the second kind is read. A trigger blocks even when the
// column appears only in its UPDATE OF list.
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

	rows, err := c.conn.Query(ctx, q, pgx.NamedArgs{"schemas": c.schemas})
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get column dependents: %w", err)
	}
	defer rows.Close()

	dependents := map[string][]Dependent{}
	for rows.Next() {
		var targetSchema, targetName, column, kind, identity string
		var depSchema, depRelation, generated *string
		if err := rows.Scan(&targetSchema, &targetName, &column, &kind, &depSchema, &depRelation, &generated, &identity); err != nil {
			return nil, fmt.Errorf("catalog: failed to scan column dependents: %w", err)
		}
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
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan column dependents rows: %w", err)
	}
	return dependents, nil
}
