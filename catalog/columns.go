package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/model"
)

func (c *Catalog) ListColumnsByTable(ctx context.Context, table *model.Table) ([]*model.Column, error) {
	q := `
		-- https://www.postgresql.org/docs/current/catalog-pg-attribute.html
		SELECT
			a.attname,
			CASE
				WHEN a.attidentity = '' AND pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL
				THEN CASE pg_catalog.format_type(a.atttypid, a.atttypmod)
					WHEN 'integer' THEN 'serial'
					WHEN 'bigint' THEN 'bigserial'
					WHEN 'smallint' THEN 'smallserial'
					ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)
				END
				ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)
			END AS type_name,
			a.attnotnull,
			CASE
				WHEN a.attidentity = '' AND pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL
				THEN NULL
				ELSE pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)
			END AS default,
			a.attidentity,
			a.attgenerated,
			co.collname,
			d.description
		FROM
			pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
			LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid
			AND ad.adnum = a.attnum
			LEFT JOIN pg_catalog.pg_collation co ON co.OID = a.attcollation
			AND co.oid != t.typcollation
			-- https://www.postgresql.org/docs/current/catalog-pg-description.html
			LEFT JOIN pg_catalog.pg_description d ON d.objoid = a.attrelid
			AND d.objsubid = a.attnum
		WHERE
			a.attrelid = @table_oid
			AND a.attnum >= 1
			AND NOT a.attisdropped
		ORDER BY
			a.attnum
	`

	args := pgx.NamedArgs{
		"table_oid": table.OID,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get column info: %w", err)
	}
	defer rows.Close()

	var cols []*model.Column
	for rows.Next() {
		var col model.Column
		err := rows.Scan(
			&col.Name,
			&col.TypeName,
			&col.NotNull,
			&col.Default,
			&col.Identity,
			&col.Generated,
			&col.Collation,
			&col.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan column info: %w", err)
		}
		cols = append(cols, &col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan column info rows: %w", err)
	}

	return cols, nil
}
