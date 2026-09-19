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
	// Name is the object's identity, schema-qualified. A relation is written
	// the way the rest of the model writes one; anything else is written the
	// way PostgreSQL identifies it, so a function carries its argument types.
	Name string
	// Relation is Name when the dependent is a view or a materialized view,
	// and "" otherwise. A plan that drops that relation too is not blocked by
	// it, since the drops run deepest first.
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
// view between them, which has to be dropped for the chain to come apart, so
// the caller meets it at that drop. A dependent outside the managed schemas is
// included, since it blocks the drop just the same.
//
// The rewrite rule a view holds over itself is left out. Every view depends on
// its own columns that way, and the rule goes with the view.
func (c *Catalog) ViewDependents(ctx context.Context) (map[string][]Dependent, error) {
	q := `
		SELECT
			tn.nspname,
			t.relname,
			CASE dc.relkind WHEN 'm' THEN 'materialized view' ELSE 'view' END,
			dn.nspname,
			dc.relname
		FROM
			pg_catalog.pg_depend d
			JOIN pg_catalog.pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_catalog.pg_rewrite'::regclass
			JOIN pg_catalog.pg_class dc ON dc.oid = r.ev_class
			JOIN pg_catalog.pg_namespace dn ON dn.oid = dc.relnamespace
			JOIN pg_catalog.pg_class t ON t.oid = d.refobjid
			JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace
		WHERE
			d.refclassid = 'pg_catalog.pg_class'::regclass
			AND d.deptype = 'n'
			AND t.relkind IN ('v', 'm')
			AND tn.nspname = ANY(@schemas)
			AND r.ev_class <> d.refobjid
		UNION
		SELECT
			tn.nspname,
			t.relname,
			(oi).type,
			NULL,
			(oi).identity
		FROM
			pg_catalog.pg_depend d
			JOIN pg_catalog.pg_class t ON t.oid = d.refobjid
			JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace
			CROSS JOIN LATERAL pg_catalog.pg_identify_object(d.classid, d.objid, 0) oi
		WHERE
			d.refclassid = 'pg_catalog.pg_class'::regclass
			AND d.deptype = 'n'
			AND t.relkind IN ('v', 'm')
			AND tn.nspname = ANY(@schemas)
			AND d.classid <> 'pg_catalog.pg_rewrite'::regclass
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
