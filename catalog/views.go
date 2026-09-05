package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func (c *Catalog) Views(ctx context.Context) (*orderedmap.Map[string, *model.View], error) {
	views, err := c.ListViews(ctx)
	if err != nil {
		return nil, err
	}

	viewByKey := orderedmap.New[string, *model.View]()
	for _, v := range views {
		viewByKey.Set(v.FQVN(), v)
	}

	// Attach indexes to materialized views
	indexes, err := c.ListIndexes(ctx)
	if err != nil {
		return nil, err
	}

	for _, idx := range indexes {
		fqvn := model.Ident(idx.Schema, idx.Table)
		if v, ok := viewByKey.GetOk(fqvn); ok && v.Materialized {
			v.Indexes.Set(idx.Name, idx)
		}
	}

	// INSTEAD OF triggers sit on plain views. A materialized view cannot carry
	// a trigger at all, so the lookup covers both without a kind check.
	triggers, err := c.ListTriggers(ctx)
	if err != nil {
		return nil, err
	}

	for _, trg := range triggers {
		if v, ok := viewByKey.GetOk(trg.FQTN()); ok {
			v.Triggers.Set(trg.Name, trg)
		}
	}

	return viewByKey, nil
}

func (c *Catalog) ListViews(ctx context.Context) ([]*model.View, error) {
	q := `
		WITH
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
			pg_catalog.pg_get_viewdef(c.oid, true) AS definition,
			c.relkind = 'm' AS materialized,
			(
				SELECT
					lower(split_part(o, '=', 2))
				FROM
					unnest(c.reloptions) AS o
				WHERE
					o LIKE 'check_option=%'
			) AS check_option,
			d.description
		FROM
			pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN dependency_extension de ON de.objid = c.oid
			LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid
			AND d.objsubid = 0
		WHERE
			c.relkind IN ('v', 'm')
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
		return nil, fmt.Errorf("catalog: failed to get view info: %w", err)
	}
	defer rows.Close()

	var views []*model.View
	for rows.Next() {
		var v model.View
		var checkOption *string
		err := rows.Scan(
			&v.OID,
			&v.Schema,
			&v.Name,
			&v.Definition,
			&v.Materialized,
			&checkOption,
			&v.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan view info: %w", err)
		}
		if checkOption != nil {
			v.CheckOption = *checkOption
		}
		v.Indexes = orderedmap.New[string, *model.Index]()
		v.Triggers = orderedmap.New[string, *model.Trigger]()
		views = append(views, &v)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan view info rows: %w", err)
	}

	return views, nil
}
