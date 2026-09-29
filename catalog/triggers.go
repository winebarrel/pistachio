package catalog

import (
	"context"

	"github.com/winebarrel/pistachio/model"
)

// ListTriggers returns the triggers on the tables and views in the catalog's
// schemas. Both relation kinds come from one query, the way ListIndexes serves
// tables and materialized views together.
func (c *Catalog) ListTriggers(ctx context.Context) ([]*model.Trigger, error) {
	q := `
		WITH
			-- https://www.postgresql.org/docs/current/catalog-pg-depend.html
			dependency_extension AS (
				SELECT DISTINCT
					d.objid
				FROM
					pg_catalog.pg_depend d
				WHERE
					d.deptype = 'e'
			)
		SELECT
			n.nspname,
			c.relname,
			t.tgname,
			-- The pretty form keeps the WHEN expression free of the parentheses
			-- the plain one wraps every operator in, and adds no line breaks of
			-- its own, so the definition stays on one line either way.
			pg_catalog.pg_get_triggerdef(t.oid, true) AS definition,
			t.tgenabled
		FROM
			-- https://www.postgresql.org/docs/current/catalog-pg-trigger.html
			pg_catalog.pg_trigger t
			JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN dependency_extension de ON de.objid = t.oid
		WHERE
			n.nspname = ANY(@schemas)
			AND de.objid IS NULL
			-- The triggers behind a foreign key or a deferred unique
			-- constraint. PostgreSQL installs and drops them with the
			-- constraint, so they are not part of the schema.
			AND NOT t.tgisinternal
			-- A trigger on a partitioned table is cloned onto every partition
			-- with tgparentid pointing back at the parent. The clone cannot be
			-- dropped on its own, and the parent's definition already covers
			-- it, so only the parent's row belongs to the model.
			AND t.tgparentid = 0
		ORDER BY
			n.nspname,
			c.relname,
			t.tgname
	`

	var triggers []*model.Trigger
	var trg model.Trigger
	err := c.eachRow(ctx, "trigger info", q, []any{&trg.Schema, &trg.Table, &trg.Name, &trg.Definition, &trg.State}, func() {
		t := trg
		triggers = append(triggers, &t)
	})
	if err != nil {
		return nil, err
	}

	return triggers, nil
}
