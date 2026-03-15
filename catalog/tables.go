package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func (c *Catalog) Tables(ctx context.Context) (*orderedmap.Map[string, *model.Table], error) {
	tables, err := c.ListTables(ctx)
	if err != nil {
		return nil, err
	}

	tableByKey := orderedmap.New[string, *model.Table]()
	for _, t := range tables {
		tableByKey.Set(t.FQTN(), t)
	}

	indexes, err := c.ListIndexes(ctx)
	if err != nil {
		return nil, err
	}

	for _, idx := range indexes {
		if t, ok := tableByKey.GetOk(idx.FQTN()); ok {
			t.Indexes.Set(idx.Name, idx)
			tableByKey.Set(t.FQTN(), t)
		}
	}

	return tableByKey, nil
}

func (c *Catalog) ListTables(ctx context.Context) ([]*model.Table, error) {
	q := `
		WITH
			-- https://www.postgresql.org/docs/current/catalog-pg-inherits.html
			partition AS (
				SELECT
					i.inhrelid,
					c.relname
				FROM
					pg_catalog.pg_inherits i
					JOIN pg_catalog.pg_class c ON c.oid = i.inhparent
				WHERE
					i.inhseqno = 1
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
			c.oid,
			n.nspname,
			c.relname,
			ts.spcname,
			c.relpersistence = 'u' AS unlogged,
			c.relkind = 'p' AS partitioned,
			pg_catalog.pg_get_partkeydef(c.oid) AS partition_def,
			p.relname AS partition_of,
			pg_catalog.pg_get_expr(c.relpartbound, c.oid) AS partition_bound,
			d.description
		FROM
			-- https://www.postgresql.org/docs/current/catalog-pg-class.html
			pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_catalog.pg_tablespace ts ON ts.oid = c.reltablespace
			LEFT JOIN partition p ON p.inhrelid = c.oid
			LEFT JOIN dependency_extension de ON de.objid = c.oid
			-- https://www.postgresql.org/docs/current/catalog-pg-description.html
			LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid
			AND d.objsubid = 0
		WHERE
			c.relkind IN ('r', 'p')
			AND n.nspname = ANY(@schemas)
			AND de.objid IS NULL
		ORDER BY
			n.nspname,
			c.relname
	`

	args := pgx.NamedArgs{
		"schemas": c.schemas,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get table info: %w", err)
	}
	defer rows.Close()

	var tables []*model.Table
	for rows.Next() {
		var t model.Table
		err := rows.Scan(
			&t.OID,
			&t.Schema,
			&t.Name,
			&t.TableSpace,
			&t.Unlogged,
			&t.Partitioned,
			&t.PartitionDef,
			&t.PartitionOf,
			&t.PartitionBound,
			&t.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan table info: %w", err)
		}
		t.Columns = orderedmap.New[string, *model.Column]()
		t.Indexes = orderedmap.New[string, *model.Index]()
		t.Constraints = orderedmap.New[string, *model.Constraint]()
		t.ForeignKeys = orderedmap.New[string, *model.ForeignKey]()
		tables = append(tables, &t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan table info rows: %w", err)
	}

	for _, t := range tables {
		cols, err := c.ListColumnsByTable(ctx, t)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to get columns for %s.%s: %w", t.Schema, t.Name, err)
		}
		for _, col := range cols {
			t.Columns.Set(col.Name, col)
		}

		cons, fks, err := c.ListConstraintsByTable(ctx, t)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to get constraints for %s.%s: %w", t.Schema, t.Name, err)
		}
		for _, con := range cons {
			t.Constraints.Set(con.Name, con)
		}
		for _, fk := range fks {
			t.ForeignKeys.Set(fk.Name, fk)
		}
	}

	return tables, nil
}
