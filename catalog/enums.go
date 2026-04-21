package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func (c *Catalog) Enums(ctx context.Context) (*orderedmap.Map[string, *model.Enum], error) {
	enums, err := c.ListEnums(ctx)
	if err != nil {
		return nil, err
	}

	enumByKey := orderedmap.New[string, *model.Enum]()
	for _, e := range enums {
		enumByKey.Set(e.FQEN(), e)
	}

	return enumByKey, nil
}

func (c *Catalog) ListEnums(ctx context.Context) ([]*model.Enum, error) {
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
			t.oid,
			n.nspname,
			t.typname,
			array_agg(e.enumlabel ORDER BY e.enumsortorder) AS vals,
			d.description
		FROM
			pg_catalog.pg_type t
			JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
			JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
			LEFT JOIN pg_catalog.pg_description d ON d.objoid = t.oid
			AND d.classoid = 'pg_type'::regclass
			LEFT JOIN dependency_extension de ON de.objid = t.oid
		WHERE
			t.typtype = 'e'
			AND n.nspname = ANY(@schemas)
			AND de.objid IS NULL
		GROUP BY
			t.oid,
			n.nspname,
			t.typname,
			d.description
		ORDER BY
			n.nspname,
			t.typname
	`

	args := pgx.NamedArgs{
		"schemas": c.schemas,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get enum info: %w", err)
	}
	defer rows.Close()

	var enums []*model.Enum
	for rows.Next() {
		var e model.Enum
		err := rows.Scan(
			&e.OID,
			&e.Schema,
			&e.Name,
			&e.Values,
			&e.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan enum info: %w", err)
		}
		enums = append(enums, &e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan enum info rows: %w", err)
	}

	return enums, nil
}
