# Sample Database Tests

pistachio is checked against real-world PostgreSQL schemas, not just the
hand-written fixtures in `testdata/`. Each sample database is downloaded from
its upstream source, loaded into an isolated database, and round-tripped
through `pista dump` and `pista plan`.

## Running

```bash
make test-samples
```

The target runs `test/samples/run.sh`, which builds `pista` and drives the
check. It needs a running PostgreSQL instance (`PGHOST=localhost`,
`PGUSER=postgres`, exported by the Makefile), plus `psql`, `curl`, and network
access to the upstream hosts. The same target runs in CI as the `samples` job.

To load every sample into one database for manual inspection instead of
checking them one at a time, use `make schema`.

## What the check does

For each sample, the runner:

1. Runs `make reset-db`, which drops every user schema so the previous sample
   cannot leak into the next one.
2. Runs the sample's loader target to download and load the schema.
3. Runs `pista dump -n <schemas>` to capture pistachio's model of the loaded
   schema as SQL.
4. Runs `pista plan -n <schemas> <dump>` and requires the output to be
   "No changes".

The two commands exercise opposite directions of the same model. `dump` goes
catalog reader -> model -> SQL; `plan` goes parser -> model -> diff against the
catalog. If the dump plans to anything other than "No changes", the catalog
reader and the parser disagree about the schema, which is a bug regardless of
which side is wrong.

Each sample reports `PASS`, `DRIFT` (the plan was not empty), or a `FAIL` with
the failing stage (`load`, `dump`, or `plan`). Failure output is printed
indented under the sample name, and the script exits non-zero if any sample
failed.

## Samples

The sample list lives in the `SAMPLES` variable in the Makefile, one record per
line: name, loader target, loader variables, and the schemas passed to
`pista -n` (blank means `public`). `make print-samples` prints it for shell
consumers, so the Makefile stays the single source of the list.

| Sample | Schemas | Source |
|---|---|---|
| chinook | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| dvdrental | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| happiness_index | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| lego | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| netflix | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| pagila | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| periodic_table | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| titanic | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| world | public | [pgFoundry dbsamples](https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/) |
| usda | public | [pgFoundry dbsamples](https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/) |
| dellstore2 | public | [pgFoundry dbsamples](https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/) |
| french_towns | public | [pgFoundry dbsamples](https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/) |
| iso_3166 | public | [pgFoundry dbsamples](https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/) |
| northwind | public | [pthom/northwind_psql](https://github.com/pthom/northwind_psql) |
| employees | employees | [h8/employees-database](https://github.com/h8/employees-database) |
| imdb | public | [gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark) |
| adventureworks | person, humanresources, production, purchasing, sales | [lorint/AdventureWorks-for-Postgres](https://github.com/lorint/AdventureWorks-for-Postgres) |
| clubdata | cd | [PostgreSQL Exercises](https://pgexercises.com/) |
| demodb | bookings | [postgrespro/demodb](https://github.com/postgrespro/demodb) |
| musicbrainz | musicbrainz | [metabrainz/musicbrainz-server](https://github.com/metabrainz/musicbrainz-server) |

## Coverage

Object counts of the loaded schemas, as of 2026-08-04 on PostgreSQL 15.18.
"Constraints" excludes foreign keys; "Types" counts enums and domains. All
counts are limited to the schemas the sample is checked with.

| Sample | Tables | Columns | Indexes | FKs | Constraints | Views | Types |
|---|---:|---:|---:|---:|---:|---:|---:|
| chinook | 11 | 64 | 21 | 11 | 11 | 0 | 0 |
| dvdrental | 15 | 86 | 32 | 18 | 16 | 7 | 2 |
| happiness_index | 1 | 9 | 1 | 0 | 1 | 0 | 0 |
| lego | 8 | 28 | 6 | 0 | 6 | 0 | 0 |
| netflix | 1 | 12 | 1 | 0 | 1 | 0 | 0 |
| pagila | 22 | 129 | 55 | 36 | 23 | 8 | 3 |
| periodic_table | 1 | 28 | 1 | 0 | 1 | 0 | 0 |
| titanic | 1 | 21 | 1 | 0 | 1 | 0 | 0 |
| world | 3 | 24 | 3 | 2 | 4 | 0 | 0 |
| usda | 10 | 67 | 15 | 10 | 9 | 0 | 0 |
| dellstore2 | 8 | 52 | 11 | 3 | 5 | 0 | 0 |
| french_towns | 3 | 14 | 9 | 2 | 9 | 0 | 0 |
| iso_3166 | 2 | 7 | 2 | 1 | 2 | 0 | 0 |
| northwind | 14 | 92 | 14 | 13 | 14 | 0 | 0 |
| employees | 6 | 24 | 9 | 6 | 6 | 0 | 1 |
| imdb | 21 | 108 | 44 | 0 | 21 | 0 | 0 |
| adventureworks | 68 | 456 | 71 | 90 | 157 | 21 | 0 |
| clubdata | 3 | 19 | 10 | 3 | 3 | 0 | 0 |
| demodb | 9 | 45 | 15 | 8 | 21 | 3 | 0 |
| musicbrainz | 374 | 2,469 | 907 | 770 | 1,032 | 10 | 7 |
| **Total** | **581** | **3,754** | **1,228** | **973** | **1,343** | **49** | **13** |

The 20 dumps come to about 11,000 lines of SQL. musicbrainz alone is roughly
two thirds of every count, and is the reason `reset-db` and `clean-schema` drop
one schema per statement: cascading through all of them in a single transaction
runs the server out of lock table space.

Beyond size, the samples bring in shapes the hand-written fixtures do not
always reach: partial and expression indexes and gin, gist, hash, and brin
methods (musicbrainz), exclusion constraints and unlogged tables (demodb, which
needs `btree_gist`), enums and domains (dvdrental, pagila, employees),
materialized views (adventureworks, pagila), tsvector columns (dvdrental,
pagila), a non-default collation (musicbrainz), and 90 foreign keys spread over
five schemas, 20 of which cross a schema boundary (adventureworks).

## Load-time adjustments

Some upstream dumps cannot be piped into `psql` as they are. The loader targets
strip only what is irrelevant to a schema round trip:

- **adventureworks**: `\copy` lines are dropped (the data lives in CSVs that are
  not fetched), along with the inline `Production.ProductReview` INSERT, whose
  foreign key targets would be missing.
- **clubdata**: the dump creates its own database and reconnects to it, which
  cannot be done mid-pipe. Those two lines are dropped; the rest creates the
  `cd` schema itself.
- **demodb**: `btree_gist` is created first for the `bookings.routes` exclusion
  constraint, and the `\copy` lines are dropped.
- **imdb**: the schema and its foreign key indexes ship as two files, so
  `schema.sql` and `fkindexes.sql` are concatenated.
- **musicbrainz**: the schema ships as one file per object kind and none of them
  create the schema, so `musicbrainz` is created up front and the files are
  concatenated in dependency order (extensions and collation, search
  configuration, types, tables, functions, then keys, indexes, constraints, and
  views).

None of these touch table, column, index, constraint, view, or type
definitions, so the round trip still covers the full schema.

## Adding a sample

1. Add a line to `SAMPLES` in the Makefile: name, loader target, loader
   variables, and the schemas for `pista -n`.
2. Reuse a loader target if the source fits one (`sample-db` for the Neon
   collection, `sample-db-tar` for a tarball, `sample-db-url` for a plain SQL
   URL). Otherwise add a target, and comment why the plain pipe does not work.
3. If the sample creates schemas other than `public`, add them to
   `SAMPLE_SCHEMAS` so `clean-schema` removes them.
4. Run `make test-samples` and confirm the new sample reports `PASS`.

A `DRIFT` result is the interesting outcome: it means pistachio reads or writes
that schema incorrectly. Fix the catalog reader, the parser, or the diff before
adding the sample, rather than trimming the schema to make it pass.
