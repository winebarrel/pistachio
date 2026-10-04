package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// Sequences returns the sequences the diff manages in the filtered schemas,
// keyed by FQN. The sequence behind an identity column, and the one behind a
// column read as a serial, are column attributes and are left out. Any other
// sequence a column owns is kept, with its owner.
func (c *Catalog) Sequences(ctx context.Context) (*orderedmap.Map[string, *model.Sequence], error) {
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
			c.relpersistence = 'u' AS unlogged,
			-- PostgreSQL keeps the owning table in the sequence's schema, so
			-- the bare name is enough.
			rc.relname AS owner_table,
			a.attname AS owner_column,
			descr.description AS comment
		FROM
			pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			JOIN pg_catalog.pg_sequence s ON s.seqrelid = c.oid
			-- deptype 'a' is OWNED BY, which serial sets up and which a user
			-- can set by hand; 'i' is an identity column's sequence.
			LEFT JOIN pg_catalog.pg_depend d ON d.objid = c.oid
			AND d.classid = 'pg_class'::regclass
			AND d.refclassid = 'pg_class'::regclass
			AND d.deptype IN ('a', 'i')
			LEFT JOIN pg_catalog.pg_class rc ON rc.oid = d.refobjid
			LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = d.refobjid
			AND a.attnum = d.refobjsubid
			LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid
			AND ad.adnum = a.attnum
			LEFT JOIN pg_catalog.pg_description descr ON descr.objoid = c.oid
			AND descr.classoid = 'pg_class'::regclass
			AND descr.objsubid = 0
			LEFT JOIN dependency_extension de ON de.objid = c.oid
		WHERE
			c.relkind = 'S'
			AND n.nspname = ANY(@schemas)
			AND de.objid IS NULL
			AND d.deptype IS DISTINCT FROM 'i'
			-- The sequence of a column catalog/columns.go reads as a serial:
			-- an integer column that draws its default from the sequence,
			-- under the name serial gave it.
			AND NOT COALESCE(
				a.attidentity = ''
				AND a.atttypid IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype)
				AND c.relname = rc.relname || '_' || a.attname || '_seq'
				AND pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) = 'nextval('
					|| quote_literal(c.oid::regclass::text)
					|| '::regclass)',
				false
			)
		ORDER BY
			n.nspname,
			c.relname
	`

	args := pgx.NamedArgs{
		"schemas": c.schemas,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get sequence info: %w", err)
	}
	defer rows.Close()

	seqByKey := orderedmap.New[string, *model.Sequence]()
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
			&s.Unlogged,
			&s.OwnerTable,
			&s.OwnerColumn,
			&s.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan sequence info: %w", err)
		}
		seqByKey.Set(s.FQN(), &s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan sequence info rows: %w", err)
	}

	return seqByKey, nil
}
