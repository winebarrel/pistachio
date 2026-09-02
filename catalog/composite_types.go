package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func (c *Catalog) CompositeTypes(ctx context.Context) (*orderedmap.Map[string, *model.CompositeType], error) {
	compositeTypes, err := c.ListCompositeTypes(ctx)
	if err != nil {
		return nil, err
	}

	byKey := orderedmap.New[string, *model.CompositeType]()
	for _, ct := range compositeTypes {
		byKey.Set(ct.FQCN(), ct)
	}

	return byKey, nil
}

func (c *Catalog) ListCompositeTypes(ctx context.Context) ([]*model.CompositeType, error) {
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
			t.typrelid,
			d.description
		FROM
			pg_catalog.pg_type t
			JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
			JOIN pg_catalog.pg_class cl ON cl.oid = t.typrelid AND cl.relkind = 'c'
			LEFT JOIN pg_catalog.pg_description d ON d.objoid = t.oid
			AND d.classoid = 'pg_type'::regclass
			AND d.objsubid = 0
			LEFT JOIN dependency_extension de ON de.objid = t.oid
		WHERE
			t.typtype = 'c'
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
		return nil, fmt.Errorf("catalog: failed to get composite type info: %w", err)
	}
	defer rows.Close()

	type ctRow struct {
		ct       *model.CompositeType
		typrelid uint32
	}
	var ctRows []ctRow
	for rows.Next() {
		var ct model.CompositeType
		var typrelid uint32
		err := rows.Scan(&ct.OID, &ct.Schema, &ct.Name, &typrelid, &ct.Comment)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan composite type info: %w", err)
		}
		ctRows = append(ctRows, ctRow{ct: &ct, typrelid: typrelid})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan composite type info rows: %w", err)
	}

	relids := make([]uint32, len(ctRows))
	for i, r := range ctRows {
		relids[i] = r.typrelid
	}

	attrsByRelid, err := c.listCompositeAttributes(ctx, relids)
	if err != nil {
		return nil, err
	}

	compositeTypes := make([]*model.CompositeType, 0, len(ctRows))
	for _, r := range ctRows {
		r.ct.Attributes = attrsByRelid[r.typrelid]
		compositeTypes = append(compositeTypes, r.ct)
	}

	return compositeTypes, nil
}

// listCompositeAttributes returns the attributes of the given composite types'
// relations, keyed by relation OID and in attnum order. One query serves every
// composite type.
func (c *Catalog) listCompositeAttributes(ctx context.Context, relids []uint32) (map[uint32][]*model.CompositeAttribute, error) {
	q := `
		SELECT
			a.attrelid,
			a.attname,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
			(SELECT quote_ident(cn.nspname) || '.' || quote_ident(coll.collname) FROM pg_catalog.pg_collation coll JOIN pg_catalog.pg_namespace cn ON cn.oid = coll.collnamespace WHERE coll.oid = a.attcollation AND a.attcollation <> 0 AND coll.collname <> 'default') AS collation,
			d.description
		FROM
			pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_description d ON d.objoid = a.attrelid
			AND d.classoid = 'pg_class'::regclass
			AND d.objsubid = a.attnum
		WHERE
			a.attrelid = ANY(@relids::oid[])
			AND a.attnum > 0
			AND NOT a.attisdropped
		ORDER BY
			a.attrelid,
			a.attnum
	`

	attrs := map[uint32][]*model.CompositeAttribute{}
	if len(relids) == 0 {
		return attrs, nil
	}

	args := pgx.NamedArgs{
		"relids": relids,
	}

	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get composite attributes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var a model.CompositeAttribute
		var relid uint32
		err := rows.Scan(&relid, &a.Name, &a.TypeName, &a.Collation, &a.Comment)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan composite attribute: %w", err)
		}
		attrs[relid] = append(attrs[relid], &a)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan composite attribute rows: %w", err)
	}

	return attrs, nil
}
