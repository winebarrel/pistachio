package catalog

import (
	"errors"

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
