package catalog

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/model"
)

// ListConstraintsByTables returns the table constraints and the foreign keys of
// the given tables, each keyed by table OID and in contype then name order. One
// query serves every table.
func (c *Catalog) ListConstraintsByTables(ctx context.Context, tables []*model.Table) (map[uint32][]*model.Constraint, map[uint32][]*model.ForeignKey, error) {
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
					con.conrelid = ANY(@table_oids::oid[])
				GROUP BY
					con.oid
			)
		SELECT
			con.conrelid,
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
			con.conrelid = ANY(@table_oids::oid[])
			-- The table-level constraint types, listed the way the ORDER BY
			-- below lists them. Two other types share the catalog and belong
			-- elsewhere. PG18's per-column NOT NULL rows (contype='n') are
			-- read into Column.NotNull / Column.NotNullName by
			-- ListColumnsByTables. CREATE CONSTRAINT TRIGGER records the
			-- trigger here too (contype='t'), where pg_get_constraintdef
			-- renders it as the bare word TRIGGER, followed by the deferral
			-- clause when it has one; reading that as a table constraint made
			-- dump write a CREATE TABLE that does not parse, and plan propose
			-- a DROP CONSTRAINT for the trigger. Naming the types pistachio
			-- manages also keeps a type a later major version adds out until
			-- the model has somewhere to put it.
			AND con.contype = ANY ('{p,u,c,x,f}'::"char"[])
		ORDER BY
			con.conrelid,
			array_position('{p,u,c,x,f}'::"char"[], con.contype),
			con.conname
	`

	constraints := map[uint32][]*model.Constraint{}
	foreignKeys := map[uint32][]*model.ForeignKey{}

	oids := tableOIDs(tables)
	if len(oids) == 0 {
		return constraints, foreignKeys, nil
	}

	tableByOID := tablesByOID(tables)
	args := pgx.NamedArgs{
		"table_oids": oids,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, nil, fmt.Errorf("catalog: failed to get constraint info: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var con model.Constraint
		var tableOID uint32
		var refSchema, refTable *string

		err := rows.Scan(
			&tableOID,
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
			table := tableByOID[tableOID]
			fk := model.ForeignKey{
				Constraint: con,
				Schema:     table.Schema,
				Table:      table.Name,
				RefSchema:  refSchema,
				RefTable:   refTable,
			}
			foreignKeys[tableOID] = append(foreignKeys[tableOID], &fk)
		} else {
			constraints[tableOID] = append(constraints[tableOID], &con)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("catalog: failed to scan constraint info rows: %w", err)
	}

	return constraints, foreignKeys, nil
}
