package pistachio

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

// normalizeDesiredViewDefs normalizes desired view definitions by creating
// each view inside a transaction and reading it back via pg_get_viewdef.
// This ensures the desired definition goes through the same PostgreSQL
// normalization as the current definition from the catalog, preventing
// false diffs caused by implicit casts (e.g. 'active'::text) or
// table-qualified column names that pg_get_viewdef adds.
// Only views that already exist in current are normalized, so that new
// views preserve the user's original SQL in plan/apply output.
// The transaction is always rolled back so no changes are persisted.
// Views that cannot be created (e.g. missing tables) are silently skipped.
func normalizeDesiredViewDefs(ctx context.Context, conn *pgx.Conn, current, desired *orderedmap.Map[string, *model.View]) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for k, v := range desired.All() {
		if _, ok := current.GetOk(k); !ok {
			continue
		}

		sql := "CREATE OR REPLACE VIEW " + v.FQVN() + " AS " + v.Definition
		if _, err := tx.Exec(ctx, sql); err != nil {
			continue
		}

		var def string
		if err := tx.QueryRow(ctx, "SELECT pg_catalog.pg_get_viewdef($1::regclass, true)", v.FQVN()).Scan(&def); err != nil {
			continue
		}

		v.Definition = def
	}
}
