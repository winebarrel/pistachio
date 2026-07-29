# Performance

This page reports how pistachio scales as the number of tables grows. The
numbers come from measuring the `plan` and `dump` commands against synthetic
schemas of increasing size.

## What is measured

Each run parses the desired schema from SQL, reads the current schema from the
PostgreSQL system catalogs, and computes the DDL diff. All three costs grow with
the number of objects, so the table count is the main driver of runtime.

Four cases are measured:

- **create plan**: `plan` of the full schema against an empty database. Every
  table is a create, so this covers parsing and diff generation with no catalog
  to read.
- **noop plan**: `plan` of the full schema against a database that already
  matches it. This is the common case in CI. It runs the full path (parse, read
  the catalog, diff) and produces empty output.
- **modify plan**: `plan` of a changed schema against a matching database. Each
  table gets a new column, a column type change, and a new index, so the diff is
  three ALTER/CREATE statements per table. This measures diff generation and SQL
  output when there is actual work to emit.
- **dump**: `dump` of the full schema. This reads the catalog and serializes it
  back to SQL, with no parsing or diffing.

## Environment

- Apple M4 Pro (14 cores), 64 GB RAM
- PostgreSQL 15.18 (Docker, connected over `localhost`)
- pistachio built from `b1de372` (after v1.19.0)

The database runs on the same host, so client/server latency is negligible.
Times over a network connection will be higher because reading the catalog adds
round trips.

## Schema shape

Each table has 9 columns of mixed types (bigint identity primary key, text,
varchar, numeric, integer, boolean, and two timestamps), two secondary indexes,
and a foreign key to the previous table. A schema of N tables therefore has N
tables, N primary keys, 2N secondary indexes, and N-1 foreign keys.

## Results

Each value is the median of three runs, in seconds.

| Tables | create plan | noop plan | modify plan | dump  |
|-------:|------------:|----------:|------------:|------:|
|     10 |       0.035 |     0.044 |       0.047 | 0.040 |
|     50 |       0.039 |     0.082 |       0.081 | 0.083 |
|    100 |       0.052 |     0.135 |       0.136 | 0.121 |
|    250 |       0.079 |     0.280 |       0.294 | 0.248 |
|    500 |       0.132 |     0.533 |       0.559 | 0.467 |
|  1,000 |       0.248 |     0.829 |       1.074 | 0.646 |

The modify plan emits three DDL statements per table, so its output grows from
30 lines at 10 tables to 3,000 lines at 1,000 tables. The create plan output
grows the same way, from 142 to 14,002 lines.

## Analysis

Runtime scales close to linearly with the table count. In the 100 to 1,000
table range, doubling the number of tables roughly doubles the time, with a
slight sublinear trend at the top end. At small sizes a fixed overhead of about
30 ms (process startup and connecting to PostgreSQL) dominates, which is why the
smallest schemas do not get proportionally faster.

The create plan is much cheaper than the noop plan and dump because it never
reads the catalog. Reading the current schema from `pg_catalog` is the largest
cost, so runtime tracks the size of the existing database more than the size of
the desired SQL file.

The modify plan is the heaviest case. It reads the catalog like the noop plan,
then generates and deparses the diff SQL, which adds cost proportional to the
number of changes. At 1,000 tables it is the only case that passes one second
(1.07s) because it emits 3,000 DDL statements. The read-only commands all stay
under one second at that size, so pistachio is not a bottleneck for schemas of
typical size. Larger schemas were not measured.

## Reproducing

Generate a schema of N tables and time the commands against a local database:

```bash
# Generate N tables, each with columns, two indexes, and a foreign key.
gen() {
  for i in $(seq 1 "$1"); do
    printf 'CREATE TABLE t_%d (\n' "$i"
    printf '  id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,\n'
    printf '  name text NOT NULL,\n  code varchar(64) NOT NULL,\n'
    printf '  amount numeric(12,2) NOT NULL DEFAULT 0,\n  qty integer NOT NULL DEFAULT 0,\n'
    printf '  is_active boolean NOT NULL DEFAULT true,\n  note text,\n'
    printf '  created_at timestamptz NOT NULL DEFAULT now(),\n'
    printf '  updated_at timestamptz NOT NULL DEFAULT now()\n);\n'
    printf 'CREATE INDEX idx_t_%d_code ON t_%d (code);\n' "$i" "$i"
    printf 'CREATE INDEX idx_t_%d_created_at ON t_%d (created_at);\n' "$i" "$i"
    [ "$i" -gt 1 ] && printf 'ALTER TABLE t_%d ADD COLUMN parent_id bigint REFERENCES t_%d (id);\n' "$i" "$((i-1))"
  done
}

gen 1000 > schema.sql
psql -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'

time pista plan schema.sql   # create plan (empty database)
psql -f schema.sql
time pista dump              # dump (loaded database)
time pista plan schema.sql   # noop plan (matching database)

# modify plan: change every table (type change, new column, new index),
# then plan against the loaded database.
sed -e 's/qty integer/qty bigint/' \
    -e 's/note text,/note text,\n  extra text,/' \
    -e 's/\(CREATE INDEX idx_t_\([0-9]*\)_created_at ON t_\2 (created_at);\)/\1\nCREATE INDEX idx_t_\2_name ON t_\2 (name);/' \
    schema.sql > modified.sql
time pista plan modified.sql # modify plan (diff on every table)
```
