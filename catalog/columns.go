package catalog

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/model"
)

// ListColumnsByTables returns the columns of the given tables, keyed by table
// OID and in attnum order. One query serves every table; reading them one at a
// time cost a round trip per table.
func (c *Catalog) ListColumnsByTables(ctx context.Context, tables []*model.Table) (map[uint32][]*model.Column, error) {
	q := `
		-- https://www.postgresql.org/docs/current/catalog-pg-attribute.html
		SELECT
			a.attrelid,
			a.attname,
			CASE
				WHEN s.is_serial
				THEN CASE pg_catalog.format_type(a.atttypid, a.atttypmod)
					WHEN 'integer' THEN 'serial'
					WHEN 'bigint' THEN 'bigserial'
					WHEN 'smallint' THEN 'smallserial'
					ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)
				END
				ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)
			END AS type_name,
			a.attnotnull,
			-- PG18 stores per-column NOT NULL as pg_constraint rows with
			-- contype='n' and a single conkey entry. Pre-PG18 has no such row,
			-- so the LEFT JOIN yields NULL there.
			nn.conname AS not_null_name,
			CASE
				WHEN s.is_serial
				THEN NULL
				ELSE pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)
			END AS default,
			a.attidentity,
			a.attgenerated,
			quote_ident(con.nspname) || '.' || quote_ident(co.collname) AS collation,
			-- attstorage always holds a strategy, so the type's own is read
			-- alongside it: a column left at the default is only visible as
			-- the two being equal.
			COALESCE(CASE a.attstorage
				WHEN 'p' THEN 'plain'
				WHEN 'e' THEN 'external'
				WHEN 'x' THEN 'extended'
				WHEN 'm' THEN 'main'
			END, '') AS storage_type,
			COALESCE(CASE t.typstorage
				WHEN 'p' THEN 'plain'
				WHEN 'e' THEN 'external'
				WHEN 'x' THEN 'extended'
				WHEN 'm' THEN 'main'
			END, '') AS type_storage,
			CASE a.attcompression
				WHEN 'p' THEN 'pglz'
				WHEN 'l' THEN 'lz4'
				ELSE ''
			END AS compression,
			d.description
		FROM
			pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
			LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid
			AND ad.adnum = a.attnum
			-- A column is serial only when it owns a sequence *and* its default
			-- actually draws from that sequence. A plain
			-- "ALTER SEQUENCE ... OWNED BY" creates the same dependency without
			-- touching the default, and treating that as serial would drop the
			-- real default from the dumped definition.
			CROSS JOIN LATERAL (
				SELECT COALESCE(
					a.attidentity = ''
					-- Round-trip the sequence name through regclass so it is
					-- rendered the same way pg_get_expr renders it:
					-- pg_get_serial_sequence always schema-qualifies, while
					-- pg_get_expr omits the schema when it is on search_path.
					AND pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) = 'nextval('
						|| quote_literal(pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname)::regclass::text)
						|| '::regclass)',
					false
				) AS is_serial
			) s
			LEFT JOIN pg_catalog.pg_collation co ON co.OID = a.attcollation
			AND co.oid != t.typcollation
			LEFT JOIN pg_catalog.pg_namespace con ON con.oid = co.collnamespace
			-- https://www.postgresql.org/docs/current/catalog-pg-description.html
			LEFT JOIN pg_catalog.pg_description d ON d.objoid = a.attrelid
			AND d.objsubid = a.attnum
			LEFT JOIN pg_catalog.pg_constraint nn ON nn.conrelid = a.attrelid
			AND nn.contype = 'n'
			AND nn.conkey = ARRAY[a.attnum]
		WHERE
			a.attrelid = ANY(@table_oids::oid[])
			AND a.attnum >= 1
			AND NOT a.attisdropped
		ORDER BY
			a.attrelid,
			a.attnum
	`

	oids := tableOIDs(tables)
	if len(oids) == 0 {
		return map[uint32][]*model.Column{}, nil
	}

	args := pgx.NamedArgs{
		"table_oids": oids,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get column info: %w", err)
	}
	defer rows.Close()

	colsByTable := map[uint32][]*model.Column{}
	for rows.Next() {
		var col model.Column
		var tableOID uint32
		err := rows.Scan(
			&tableOID,
			&col.Name,
			&col.TypeName,
			&col.NotNull,
			&col.NotNullName,
			&col.Default,
			&col.Identity,
			&col.Generated,
			&col.Collation,
			&col.StorageType,
			&col.TypeStorage,
			&col.Compression,
			&col.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan column info: %w", err)
		}
		// PG18 auto-names unnamed NOT NULL constraints as <table>_<col>_not_null
		// (see ChooseConstraintName in src/backend/catalog/pg_constraint.c). The
		// auto-name does not follow column or table renames, so checking the
		// current <table>_<col>_ prefix is too strict. Strip any name with the
		// _not_null suffix as a heuristic so unnamed declarations round-trip as
		// unnamed across renames; the trade-off is that a user-supplied explicit
		// name ending in "_not_null" would be lost on round-trip.
		if col.NotNullName != nil && strings.HasSuffix(*col.NotNullName, "_not_null") {
			col.NotNullName = nil
		}
		colsByTable[tableOID] = append(colsByTable[tableOID], &col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan column info rows: %w", err)
	}

	return colsByTable, nil
}
