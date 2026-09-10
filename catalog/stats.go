package catalog

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/model"
)

// TableStat is what pg_class remembers about a table's size. Both numbers are
// the planner's estimates, written by VACUUM, ANALYZE and CREATE INDEX rather
// than counted on demand, so reading them costs nothing on the table itself.
// Rows is -1 for a table none of those has visited yet.
//
// StatsAt is how old the estimate is: the last time a VACUUM or an ANALYZE,
// by hand or by the autovacuum daemon, visited the table. It is the zero time
// when neither has, and also when the statistics have been reset, since the
// server remembers the times no longer than the counters. CREATE INDEX writes
// reltuples without a time of its own, so an estimate can be newer than this
// says.
type TableStat struct {
	Rows    int64
	Bytes   int64
	StatsAt time.Time
}

// TableStats reads the size estimate of every table in the managed schemas,
// keyed by the same schema-qualified name the Tables map uses. The TOAST
// relation's pages count toward the table's bytes, since a rewrite copies them
// too. A partitioned parent holds no rows of its own; the caller sums its
// partitions.
//
// The times come from the same functions pg_stat_all_tables calls, rather
// than from the view, which groups over every table in the database before a
// join can narrow it to the managed schemas. Each is a lookup in the shared
// statistics, made for the tables read here alone.
func (c *Catalog) TableStats(ctx context.Context) (map[string]TableStat, error) {
	q := `
		SELECT
			n.nspname,
			c.relname,
			c.reltuples::float8,
			GREATEST(c.relpages, 0) + GREATEST(COALESCE(t.relpages, 0), 0),
			pg_catalog.current_setting('block_size')::bigint,
			GREATEST(
				pg_catalog.pg_stat_get_last_vacuum_time(c.oid),
				pg_catalog.pg_stat_get_last_autovacuum_time(c.oid),
				pg_catalog.pg_stat_get_last_analyze_time(c.oid),
				pg_catalog.pg_stat_get_last_autoanalyze_time(c.oid)
			)
		FROM
			pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_catalog.pg_class t ON t.oid = c.reltoastrelid
		WHERE
			c.relkind IN ('r', 'p')
			AND n.nspname = ANY(@schemas)
	`

	rows, err := c.conn.Query(ctx, q, pgx.NamedArgs{"schemas": c.schemas})
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get table stats: %w", err)
	}
	defer rows.Close()

	stats := map[string]TableStat{}
	for rows.Next() {
		var schema, name string
		var tuples float64
		var pages, blockSize int64
		var statsAt *time.Time
		if err := rows.Scan(&schema, &name, &tuples, &pages, &blockSize, &statsAt); err != nil {
			return nil, fmt.Errorf("catalog: failed to scan table stats: %w", err)
		}
		st := TableStat{Rows: -1, Bytes: pages * blockSize}
		if tuples >= 0 {
			st.Rows = int64(math.Round(tuples))
		}
		if statsAt != nil {
			st.StatsAt = *statsAt
		}
		stats[model.Ident(schema, name)] = st
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan table stats rows: %w", err)
	}
	return stats, nil
}

// TypeChange names the two sides of a column type change, each as a type name
// without a type modifier or an array bound, spelled the way the catalog or
// the desired schema wrote it.
type TypeChange struct {
	Src string
	Dst string
}

// TypeChangeInfo is what the server says about a TypeChange. Known is false
// when either name does not resolve to a type. A domain stands for its base
// type in SameBase, BaseType and Binary, and Constrained says the destination
// is a domain that carries a NOT NULL or a CHECK, which PostgreSQL verifies by
// rewriting the table.
type TypeChangeInfo struct {
	Known       bool
	SameBase    bool
	BaseType    string
	Binary      bool
	Constrained bool
}

// TypeChanges resolves every change in one query. The names are resolved
// under the connection's search_path, which is the one the catalog printed
// them under, so an unqualified name reads back as the same type.
func (c *Catalog) TypeChanges(ctx context.Context, changes []TypeChange) (map[TypeChange]TypeChangeInfo, error) {
	infos := map[TypeChange]TypeChangeInfo{}
	if len(changes) == 0 {
		return infos, nil
	}

	srcs := make([]string, len(changes))
	dsts := make([]string, len(changes))
	for i, ch := range changes {
		srcs[i] = ch.Src
		dsts[i] = ch.Dst
	}

	q := `
		WITH
			pair AS (
				SELECT
					p.src,
					p.dst,
					pg_catalog.to_regtype(p.src)::oid AS soid,
					pg_catalog.to_regtype(p.dst)::oid AS doid
				FROM
					unnest(@srcs::text[], @dsts::text[]) AS p(src, dst)
			),
			resolved AS (
				SELECT
					p.src,
					p.dst,
					st.oid IS NOT NULL AND dt.oid IS NOT NULL AS known,
					COALESCE(NULLIF(st.typbasetype, 0), st.oid) AS sbase,
					COALESCE(NULLIF(dt.typbasetype, 0), dt.oid) AS dbase,
					dt.typtype = 'd' AND (
						dt.typnotnull
						OR EXISTS (SELECT 1 FROM pg_catalog.pg_constraint con WHERE con.contypid = dt.oid)
					) AS constrained
				FROM
					pair p
					LEFT JOIN pg_catalog.pg_type st ON st.oid = p.soid
					LEFT JOIN pg_catalog.pg_type dt ON dt.oid = p.doid
			)
		SELECT
			r.src,
			r.dst,
			r.known,
			COALESCE(r.sbase = r.dbase, false),
			COALESCE(pg_catalog.format_type(r.dbase, NULL), ''),
			COALESCE(r.constrained, false),
			EXISTS (
				SELECT 1
				FROM pg_catalog.pg_cast ca
				WHERE ca.castsource = r.sbase AND ca.casttarget = r.dbase AND ca.castmethod = 'b'
			)
		FROM
			resolved r
	`

	rows, err := c.conn.Query(ctx, q, pgx.NamedArgs{"srcs": srcs, "dsts": dsts})
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to resolve type changes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ch TypeChange
		var info TypeChangeInfo
		if err := rows.Scan(&ch.Src, &ch.Dst, &info.Known, &info.SameBase, &info.BaseType, &info.Constrained, &info.Binary); err != nil {
			return nil, fmt.Errorf("catalog: failed to scan type change: %w", err)
		}
		infos[ch] = info
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan type change rows: %w", err)
	}
	return infos, nil
}

// VolatileFunctions reports, for each function name, whether any function of
// that name is volatile. The name alone is matched, without its schema or its
// argument types: a default expression is read from the statement text, where
// the overload is not resolved, and one volatile overload is enough to make
// the answer useful.
func (c *Catalog) VolatileFunctions(ctx context.Context, names []string) (map[string]bool, error) {
	volatile := map[string]bool{}
	if len(names) == 0 {
		return volatile, nil
	}

	q := `
		SELECT
			p.proname,
			bool_or(p.provolatile = 'v')
		FROM
			pg_catalog.pg_proc p
		WHERE
			p.proname = ANY(@names)
		GROUP BY
			p.proname
	`

	rows, err := c.conn.Query(ctx, q, pgx.NamedArgs{"names": names})
	if err != nil {
		return nil, fmt.Errorf("catalog: failed to get function volatility: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var v bool
		if err := rows.Scan(&name, &v); err != nil {
			return nil, fmt.Errorf("catalog: failed to scan function volatility: %w", err)
		}
		volatile[name] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: failed to scan function volatility rows: %w", err)
	}
	return volatile, nil
}
