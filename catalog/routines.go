package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
	"github.com/winebarrel/pistachio/parser"
)

// Routines returns the managed functions and procedures in the filtered
// schemas, keyed by FQRN.
func (c *Catalog) Routines(ctx context.Context) (*orderedmap.Map[string, *model.Routine], error) {
	routines, err := c.ListRoutines(ctx)
	if err != nil {
		return nil, err
	}

	byKey := orderedmap.New[string, *model.Routine]()
	for _, r := range routines {
		byKey.Set(r.FQRN(), r)
	}

	return byKey, nil
}

// ListRoutines returns the functions and procedures pistachio manages.
//
// Each row is read as the CREATE statement pg_get_functiondef renders and
// handed to the same parser the desired schema goes through, so both sides of
// the diff produce the same model and render through the same SQL writer. That
// is what keeps a dump fed back as the desired schema planning clean, without
// the definition-text normalization views and triggers need.
//
// Aggregates (prokind 'a') and window functions (prokind 'w') are left out, as
// is a routine with a SQL-standard body (BEGIN ATOMIC): such a body records
// pg_depend entries on whatever it reads, which contradicts the order
// pistachio creates routines in. The parser skips the same ones, so neither
// side of the diff sees them.
func (c *Catalog) ListRoutines(ctx context.Context) ([]*model.Routine, error) {
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
			p.oid,
			pg_catalog.pg_get_functiondef(p.oid) AS definition,
			descr.description AS comment
		FROM
			pg_catalog.pg_proc p
			JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
			LEFT JOIN pg_catalog.pg_description descr ON descr.objoid = p.oid
			AND descr.classoid = 'pg_proc'::regclass
			AND descr.objsubid = 0
			LEFT JOIN dependency_extension de ON de.objid = p.oid
		WHERE
			p.prokind IN ('f', 'p')
			AND p.prosqlbody IS NULL
			AND n.nspname = ANY(@schemas)
			AND de.objid IS NULL
		ORDER BY
			n.nspname,
			p.proname,
			pg_catalog.pg_get_function_identity_arguments(p.oid)
	`

	args := pgx.NamedArgs{
		"schemas": c.schemas,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("failed to get routine info: %w", err)
	}
	defer rows.Close()

	var routines []*model.Routine
	for rows.Next() {
		var (
			oid        uint32
			definition string
			comment    *string
		)
		if err := rows.Scan(&oid, &definition, &comment); err != nil {
			return nil, fmt.Errorf("catalog: failed to scan routine info: %w", err)
		}

		// pg_get_functiondef always schema-qualifies the name, so the default
		// schema passed here is never consulted.
		routine, err := parser.ParseRoutineDef(definition, "")
		if errors.Is(err, parser.ErrUnsupportedRoutine) {
			// A routine pistachio does not manage. The parser drops the same
			// ones from the desired schema, so skipping the row here keeps the
			// two sides in step instead of failing the whole read.
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to parse routine definition: %w", err)
		}
		routine.OID = oid
		routine.Comment = comment
		routines = append(routines, routine)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan routine info rows: %w", err)
	}

	return routines, nil
}
