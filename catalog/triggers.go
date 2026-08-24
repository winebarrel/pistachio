package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
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

	args := pgx.NamedArgs{
		"schemas": c.schemas,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get trigger info: %w", err)
	}
	defer rows.Close()

	var triggers []*model.Trigger
	for rows.Next() {
		var trg model.Trigger
		err := rows.Scan(
			&trg.Schema,
			&trg.Table,
			&trg.Name,
			&trg.Definition,
			&trg.State,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan trigger info: %w", err)
		}
		triggers = append(triggers, &trg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan trigger info rows: %w", err)
	}

	return triggers, nil
}
