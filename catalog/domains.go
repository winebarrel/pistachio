package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func (c *Catalog) Domains(ctx context.Context) (*orderedmap.Map[string, *model.Domain], error) {
	domains, err := c.ListDomains(ctx)
	if err != nil {
		return nil, err
	}

	domainByKey := orderedmap.New[string, *model.Domain]()
	for _, d := range domains {
		domainByKey.Set(d.FQDN(), d)
	}

	return domainByKey, nil
}

func (c *Catalog) ListDomains(ctx context.Context) ([]*model.Domain, error) {
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
			pg_catalog.format_type(t.typbasetype, t.typtypmod) AS base_type,
			t.typnotnull,
			pg_catalog.pg_get_expr(t.typdefaultbin, 0) AS default_value,
			(SELECT cn.nspname || '.' || c.collname FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace cn ON cn.oid = c.collnamespace WHERE c.oid = t.typcollation AND t.typcollation <> 0 AND c.collname <> 'default') AS collation,
			d.description
		FROM
			pg_catalog.pg_type t
			JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
			LEFT JOIN pg_catalog.pg_description d ON d.objoid = t.oid
			AND d.classoid = 'pg_type'::regclass
			AND d.objsubid = 0
			LEFT JOIN dependency_extension de ON de.objid = t.oid
		WHERE
			t.typtype = 'd'
			AND n.nspname = ANY(@schemas)
			AND de.objid IS NULL
		ORDER BY
			n.nspname,
			t.typname
	`

	args := pgx.NamedArgs{
		"schemas": c.schemas,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get domain info: %w", err)
	}
	defer rows.Close()

	var domains []*model.Domain
	for rows.Next() {
		var d model.Domain
		err := rows.Scan(
			&d.OID,
			&d.Schema,
			&d.Name,
			&d.BaseType,
			&d.NotNull,
			&d.Default,
			&d.Collation,
			&d.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan domain info: %w", err)
		}
		domains = append(domains, &d)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan domain info rows: %w", err)
	}

	// Fetch constraints for each domain
	for _, d := range domains {
		constraints, err := c.listDomainConstraints(ctx, d.OID)
		if err != nil {
			return nil, err
		}
		d.Constraints = constraints
	}

	return domains, nil
}

func (c *Catalog) listDomainConstraints(ctx context.Context, domainOID uint32) ([]*model.DomainConstraint, error) {
	q := `
		SELECT
			con.conname,
			pg_catalog.pg_get_constraintdef(con.oid) AS definition
		FROM
			pg_catalog.pg_constraint con
		WHERE
			con.contypid = @oid
			AND con.contype = 'c'
		ORDER BY
			con.conname
	`

	args := pgx.NamedArgs{
		"oid": domainOID,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get domain constraints: %w", err)
	}
	defer rows.Close()

	var constraints []*model.DomainConstraint
	for rows.Next() {
		var dc model.DomainConstraint
		err := rows.Scan(&dc.Name, &dc.Definition)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan domain constraint: %w", err)
		}
		constraints = append(constraints, &dc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan domain constraint rows: %w", err)
	}

	return constraints, nil
}
