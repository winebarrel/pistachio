package catalog

import (
	"context"
	"strings"

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
			ts.spcname,
			descr.description AS comment,
			ci.relispartition AS attached,
			ct.relkind = 'p' AS on_partitioned,
			NOT i.indisvalid AS invalid
		FROM
			-- https://www.postgresql.org/docs/current/catalog-pg-index.html
			pg_catalog.pg_index i
			JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
			JOIN pg_catalog.pg_class ct ON ct.oid = i.indrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace
			LEFT JOIN pg_catalog.pg_tablespace ts ON ts.oid = ci.reltablespace
			-- https://www.postgresql.org/docs/current/catalog-pg-description.html
			LEFT JOIN pg_catalog.pg_description descr ON descr.objoid = ci.oid
			AND descr.classoid = 'pg_class'::regclass
			AND descr.objsubid = 0
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

	var indexes []*model.Index
	var idx model.Index
	var onPartitioned bool
	dest := []any{
		&idx.OID,
		&idx.Schema,
		&idx.Name,
		&idx.Table,
		&idx.Definition,
		&idx.TableSpace,
		&idx.Comment,
		&idx.Attached,
		&onPartitioned,
		&idx.Invalid,
	}
	err := c.eachRow(ctx, "index info", q, dest, func() {
		i := idx
		if onPartitioned {
			// pg_get_indexdef writes ONLY for every index on a partitioned
			// table. Without it, the index attaches the partitions' matching
			// indexes when the plan creates it after them.
			i.Definition = strings.Replace(i.Definition, " ON ONLY ", " ON ", 1)
		}
		indexes = append(indexes, &i)
	})
	if err != nil {
		return nil, err
	}

	return indexes, nil
}
