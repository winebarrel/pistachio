package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/model"
)

// ListPoliciesByTable returns row-level security policies attached to the
// given table, in pg_policy.polname order.
func (c *Catalog) ListPoliciesByTable(ctx context.Context, table *model.Table) ([]*model.Policy, error) {
	// pg_policy.polroles uses OID 0 to represent PUBLIC. pg_roles does not
	// contain a row for OID 0, so a join would silently drop PUBLIC when it
	// appears alongside named roles (e.g. TO PUBLIC, app_user). UNION the
	// synthetic "public" entry with the named-role rows so all elements are
	// preserved and sorted together.
	q := `
		SELECT
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
			pol.polrelid = @table_oid
		ORDER BY
			pol.polname
	`
	args := pgx.NamedArgs{"table_oid": table.OID}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get policy info: %w", err)
	}
	defer rows.Close()

	var policies []*model.Policy
	for rows.Next() {
		p := &model.Policy{
			Schema: table.Schema,
			Table:  table.Name,
		}
		err := rows.Scan(
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
		policies = append(policies, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan policy info rows: %w", err)
	}

	return policies, nil
}
