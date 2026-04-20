package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/model"
)

func (c *Catalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	q := `
		-- https://www.postgresql.org/docs/current/catalog-pg-constraint.html
		WITH
			constraint_t AS (
				SELECT DISTINCT
					con.conindid
				FROM
					pg_catalog.pg_constraint con
				WHERE
					con.contype IN ('p', 'u', 'x')
			),
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
			ci.oid,
			n.nspname,
			ci.relname AS name,
			ct.relname AS table,
			pg_catalog.pg_get_indexdef(i.indexrelid) AS definition,
			ts.spcname
		FROM
			-- https://www.postgresql.org/docs/current/catalog-pg-index.html
			pg_catalog.pg_index i
			JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
			JOIN pg_catalog.pg_class ct ON ct.oid = i.indrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace
			LEFT JOIN pg_catalog.pg_tablespace ts ON ts.oid = ci.reltablespace
			LEFT JOIN dependency_extension de ON de.objid = ci.oid
			LEFT JOIN constraint_t con ON con.conindid = ci.oid
		WHERE
			n.nspname = ANY(@schemas)
			AND de.objid IS NULL
			AND con.conindid IS NULL
			AND i.indislive
		ORDER BY
			n.nspname,
			ct.relname,
			ci.relname
	`

	args := pgx.NamedArgs{
		"schemas": c.schemas,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get index info: %w", err)
	}
	defer rows.Close()

	var indexes []*model.Index
	for rows.Next() {
		var idx model.Index
		err := rows.Scan(
			&idx.OID,
			&idx.Schema,
			&idx.Name,
			&idx.Table,
			&idx.Definition,
			&idx.TableSpace,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan index info: %w", err)
		}
		indexes = append(indexes, &idx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan index info rows: %w", err)
	}

	return indexes, nil
}
