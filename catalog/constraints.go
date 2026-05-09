package catalog

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/model"
)

func (c *Catalog) ListConstraintsByTable(ctx context.Context, table *model.Table) ([]*model.Constraint, []*model.ForeignKey, error) {
	q := `
		WITH
			-- One row per pg_constraint row: the conkey columns in conkey
			-- order. Grouped by con.oid so each constraint gets only its own
			-- columns; previously this CTE grouped by attrelid, which made
			-- every constraint on the same table inherit the union of all
			-- constraint columns (and on PG18 the contype='n' rows added
			-- duplicates).
			column_t AS (
				SELECT
					con.oid AS con_oid,
					array_agg(
						a.attname
						ORDER BY
							array_position(con.conkey, a.attnum)
					) AS attnames
				FROM
					pg_catalog.pg_constraint con
					JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid
					AND a.attnum = ANY(con.conkey)
				WHERE
					con.conrelid = @table_oid
				GROUP BY
					con.oid
			)
		SELECT
			con.oid,
			con.conname,
			con.contype,
			pg_catalog.pg_get_constraintdef(con.oid, true) AS definition,
			col.attnames AS columns,
			con.condeferrable,
			con.condeferred,
			con.convalidated,
			rn.nspname AS ref_schema,
			rc.relname AS ref_table
		FROM
			-- https://www.postgresql.org/docs/current/catalog-pg-constraint.html
			pg_catalog.pg_constraint con
			LEFT JOIN pg_catalog.pg_class rc ON rc.oid = con.confrelid
			LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
			LEFT JOIN column_t col ON col.con_oid = con.oid
		WHERE
			con.conrelid = @table_oid
			-- PG18's per-column NOT NULL rows (contype='n') are read into
			-- Column.NotNull / Column.NotNullName by ListColumnsByTable; this
			-- query only returns table-level constraints.
			AND con.contype <> 'n'
		ORDER BY
			array_position('{p,u,c,x,f}'::"char"[], con.contype),
			con.conname
	`
	args := pgx.NamedArgs{
		"table_oid": table.OID,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, nil, fmt.Errorf("catalog: failed to get constraint info: %w", err)
	}
	defer rows.Close()

	var constraints []*model.Constraint
	var foreignKeys []*model.ForeignKey
	for rows.Next() {
		var con model.Constraint
		var refSchema, refTable *string

		err := rows.Scan(
			&con.OID,
			&con.Name,
			&con.Type,
			&con.Definition,
			&con.Columns,
			&con.Deferrable,
			&con.Deferred,
			&con.Validated,
			&refSchema,
			&refTable,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("catalog: failed to scan constraint info: %w", err)
		}

		// pg_get_constraintdef includes "NOT VALID" in the definition string
		// for unvalidated constraints. Strip it so Definition only contains
		// the constraint body; validation state is tracked via Validated.
		con.Definition = strings.TrimSuffix(con.Definition, " NOT VALID")

		if con.Type.IsForeignKeyConstraint() {
			fk := model.ForeignKey{
				Constraint: con,
				Schema:     table.Schema,
				Table:      table.Name,
				RefSchema:  refSchema,
				RefTable:   refTable,
			}
			foreignKeys = append(foreignKeys, &fk)
		} else {
			constraints = append(constraints, &con)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("catalog: failed to scan constraint info rows: %w", err)
	}

	return constraints, foreignKeys, nil
}
