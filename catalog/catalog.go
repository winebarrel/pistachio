package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Catalog struct {
	conn    *pgx.Conn
	schemas []string
}

func NewCatalog(conn *pgx.Conn, schemas []string) (*Catalog, error) {
	if len(schemas) == 0 {
		return nil, errors.New("catalog: must specify schemas")
	}

	catalog := &Catalog{
		conn:    conn,
		schemas: schemas,
	}

	return catalog, nil
}

// collectRows runs q with args and returns what scan reads from each row.
// what names the query in an error.
func collectRows[T any](ctx context.Context, c *Catalog, what, q string, args pgx.NamedArgs, scan func(pgx.Row) (T, error)) ([]T, error) {
	rows, err := c.conn.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get %s: %w", what, err)
	}
	defer rows.Close()

	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("catalog: failed to scan %s: %w", what, err)
		}
		out = append(out, v)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan %s rows: %w", what, err)
	}

	return out, nil
}
