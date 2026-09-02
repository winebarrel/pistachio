package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/model"
)

// ListPoliciesByTables returns the row-level security policies attached to the
// given tables, keyed by table OID and in pg_policy.polname order. One query
// serves every table.
func (c *Catalog) ListPoliciesByTables(ctx context.Context, tables []*model.Table) (map[uint32][]*model.Policy, error) {
	// pg_policy.polroles uses OID 0 to represent PUBLIC. pg_roles does not
	// contain a row for OID 0, so a join would silently drop PUBLIC when it
	// appears alongside named roles (e.g. TO PUBLIC, app_user). UNION the
	// synthetic "public" entry with the named-role rows so all elements are
	// preserved and sorted together.
	q := `
		SELECT
			pol.polrelid,
			pol.polname,
			pol.polpermissive,
			pol.polcmd,
			COALESCE(
				(
					SELECT array_agg(rolname ORDER BY rolname)
					FROM (
						SELECT 'public'::name AS rolname
						WHERE 0 = ANY(pol.polroles)
						UNION
						SELECT r.rolname
						FROM pg_catalog.pg_roles r
						WHERE r.oid = ANY(pol.polroles)
					) AS roles
				),
				ARRAY[]::name[]
			) AS roles,
			pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) AS using_expr,
			pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check
		FROM
			-- https://www.postgresql.org/docs/current/catalog-pg-policy.html
			pg_catalog.pg_policy pol
		WHERE
			pol.polrelid = ANY(@table_oids::oid[])
		ORDER BY
			pol.polrelid,
			pol.polname
	`

	policies := map[uint32][]*model.Policy{}

	oids := tableOIDs(tables)
	if len(oids) == 0 {
		return policies, nil
	}

	tableByOID := tablesByOID(tables)
	args := pgx.NamedArgs{"table_oids": oids}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get policy info: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableOID uint32
		p := &model.Policy{}
		err := rows.Scan(
			&tableOID,
			&p.Name,
			&p.Permissive,
			&p.Command,
			&p.Roles,
			&p.Using,
			&p.WithCheck,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan policy info: %w", err)
		}
		table := tableByOID[tableOID]
		p.Schema = table.Schema
		p.Table = table.Name
		policies[tableOID] = append(policies[tableOID], p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan policy info rows: %w", err)
	}

	return policies, nil
}
