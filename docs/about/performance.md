# Performance

This page reports how pistachio scales as the number of tables grows. The
numbers come from measuring the `plan`, `dump` and `fmt` commands against
synthetic schemas of increasing size.

## What is measured

Each run parses the desired schema from SQL, reads the current schema from the
PostgreSQL system catalogs, and computes the DDL diff. All three costs grow with
the number of objects, so the table count is the main driver of runtime.

Five cases are measured:

- **create plan**: `plan` of the full schema against an empty database. Every
  table is a create, so this covers parsing and diff generation with no catalog
  to read.
- **noop plan**: `plan` of the full schema against a database that already
  matches it. This is the common case in CI. It runs the full path (parse, read
  the catalog, diff) and produces empty output.
- **modify plan**: `plan` of a changed schema against a matching database. Each
  table gets a new column, a column type change, and a new index, so the diff is
  three ALTER/CREATE statements per table. This measures diff generation and SQL
  output.
- **dump**: `dump` of the full schema. This reads the catalog and serializes it
  back to SQL, with no parsing or diffing. The output goes through the
  formatter, the same one `fmt` runs.
- **fmt**: `fmt` of the schema file, rewritten in place. This parses the file
  and lays it out again, and reads no database.

## Environment

- Apple M4 Pro (14 cores), 64 GB RAM
- PostgreSQL 15.18 (Docker, connected over `localhost`)
- pistachio built from `be2c79e` (after v1.46.0)

The database runs on the same host, so client/server latency is negligible.
Times over a network connection will be higher because reading the catalog adds
round trips.

## Schema shape

Each table has 9 columns of mixed types (bigint identity primary key, text,
varchar, numeric, integer, boolean, and two timestamps) and two secondary
indexes. Every table but the first also has a `parent_id` column with a foreign
key to the previous table. A schema of N tables therefore has N tables, N
primary keys, 2N secondary indexes, and N-1 foreign keys.

## Results

Each value is the median of three runs, in seconds.

| Tables | create plan | noop plan | modify plan | dump  |   fmt |
|-------:|------------:|----------:|------------:|------:|------:|
|     10 |       0.045 |     0.048 |       0.047 | 0.045 | 0.012 |
|     50 |       0.050 |     0.078 |       0.074 | 0.075 | 0.015 |
|    100 |       0.069 |     0.105 |       0.111 | 0.089 | 0.019 |
|    250 |       0.113 |     0.198 |       0.197 | 0.147 | 0.037 |
|    500 |       0.187 |     0.284 |       0.299 | 0.200 | 0.085 |
|  1,000 |       0.306 |     0.527 |       0.580 | 0.374 | 0.141 |

The modify plan emits three DDL statements per table, so its output grows from
30 lines at 10 tables to 3,000 lines at 1,000 tables. The create plan output
grows the same way, from 160 to 16,000 lines.

## Analysis

Runtime scales close to linearly with the table count. A fixed overhead of about
40 ms (process startup and connecting to PostgreSQL) sets the floor, which is
why the smallest schemas do not get proportionally faster. Above it, 10 times
the tables costs 8 to 10 times the time.

No single stage dominates. The create plan parses the SQL file and diffs it
without reading the catalog; dump reads the catalog and serializes it without
parsing. At 1,000 tables the two cost about the same, 0.31s and 0.37s, and the
noop plan, which does both, costs roughly their sum. The catalog read used to
cost a round trip per object and outweighed the rest; 1.39.0, which this build
includes, made it a fixed number of queries.

The modify plan is 53 ms slower than the noop plan at 1,000 tables. Reading the
catalog and parsing the desired schema are the bulk of the work, so emitting
3,000 DDL statements on top costs little. Every case stays under 0.6s at that
size, so pistachio is not a bottleneck for schemas of typical size. Larger
schemas were not measured.

`fmt` is the cheapest of the five, since it neither connects to the database nor
builds a model: it parses the file, lays the tokens out again, and checks the
result carries the same tokens. At 1,000 tables it takes 0.14s, and the work
itself is 0.10s of that.

The layout pass costs `dump` 0.11s at 1,000 tables, a little under a third of
its 0.37s: `dump --no-format`, which skips it, takes 0.26s. At 500 tables the
difference is 0.06s. Measured on its own against files of 100 to 2,000 tables,
the formatter runs in 16 ms to 205 ms, which is linear in the size of the
input.

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
    printf '  updated_at timestamptz NOT NULL DEFAULT now()'
    [ "$i" -gt 1 ] && printf ',\n  parent_id bigint REFERENCES t_%d (id)' "$((i-1))"
    printf '\n);\n'
    printf 'CREATE INDEX idx_t_%d_code ON t_%d (code);\n' "$i" "$i"
    printf 'CREATE INDEX idx_t_%d_created_at ON t_%d (created_at);\n' "$i" "$i"
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

# fmt: lay the schema file out again. Copy it first, since fmt rewrites the
# file it is given and a second run on the formatted file does no work.
cp schema.sql copy.sql
time pista fmt copy.sql      # fmt (no database)
```

The foreign key has to sit on the column. pistachio does not read a standalone
`ALTER TABLE ... ADD COLUMN ... REFERENCES`. Written that way the foreign keys
never reach the desired schema, and the noop plan proposes dropping them.

Dropping 1,000 tables in one statement needs more locks than the default allows,
so raise `max_locks_per_transaction` before resetting between runs. 4096 is
enough.
