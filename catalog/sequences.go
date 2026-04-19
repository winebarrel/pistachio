package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/model"
)

// Sequences returns all sequences in the filtered schemas.
func (c *Catalog) Sequences(ctx context.Context) ([]model.Sequence, error) {
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
			pg_catalog.format_type(s.seqtypid, NULL) AS data_type,
			s.seqstart,
			s.seqmin,
			s.seqmax,
			s.seqincrement,
			s.seqcache,
			s.seqcycle,
			d.refobjid::regclass::text AS owner_table,
			a.attname AS owner_column
		FROM
			pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			JOIN pg_catalog.pg_sequence s ON s.seqrelid = c.oid
			LEFT JOIN pg_catalog.pg_depend d ON d.objid = c.oid
			AND d.classid = 'pg_class'::regclass
			AND d.refclassid = 'pg_class'::regclass
			AND d.deptype = 'a'
			LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = d.refobjid
			AND a.attnum = d.refobjsubid
			LEFT JOIN dependency_extension de ON de.objid = c.oid
		WHERE
			c.relkind = 'S'
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
		return nil, fmt.Errorf("failed to get sequence info: %w", err)
	}
	defer rows.Close()

	var seqs []model.Sequence
	for rows.Next() {
		var s model.Sequence
		err := rows.Scan(
			&s.OID,
			&s.Schema,
			&s.Name,
			&s.DataType,
			&s.Start,
			&s.Min,
			&s.Max,
			&s.Increment,
			&s.Cache,
			&s.Cycle,
			&s.OwnerTable,
			&s.OwnerColumn,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan sequence info: %w", err)
		}
		seqs = append(seqs, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan sequence info rows: %w", err)
	}

	return seqs, nil
}
