# Amazon Aurora DSQL

`--engine dsql` supports DSQL. The user-facing description is
docs/guides/aurora-dsql.md. This file records what a live cluster answered,
so that a change to that support starts from evidence rather than from the
PostgreSQL manual.

The findings were verified against a live DSQL cluster (PostgreSQL 16 wire
protocol, ap-northeast-1) on 2026-07-23, and again on 2026-10-10
(`server_version` 16.15, `sys.dsql_major_version()` 1).

The features that DSQL does not support (foreign keys, triggers, PL/pgSQL,
and so on) are out of scope. A schema that is written for DSQL never
contains them. DSQL refuses them at apply.

Support policy:
- Work correctly within DSQL's supported feature set. Do not reproduce
  every PostgreSQL feature on DSQL.
- Refuse at plan the changes that DSQL has no path for: `DROP COLUMN`,
  `SET NOT NULL`, a column `TYPE` change, adding a NOT NULL column, adding
  a PK or CHECK constraint to an existing table, and `CONCURRENTLY`. Do not
  add recreation or back-fill machinery to force these through.
- Keep the DSQL paths opt-in and additive. Without `--engine dsql`, the
  behavior is exactly what it was before. The DSQL code sits in the `dsql`
  package and `dsql_options.go`, called from branches on the engine.

TestDSQL in dsql_test.go replays testdata/dsql/dsql.yaml with pgstub, so CI
needs no cluster. It covers only what a cluster has to answer; what is
decided before anything DSQL-specific is sent is tested against local
PostgreSQL. `make test-dsql` runs it, and `make test` skips it. A change to
any statement pistachio sends, catalog queries included, breaks the replay:
record it again with `DSQL_HOST=<endpoint> make record-dsql` on a cluster
whose public schema is empty. The script refuses a recording that names the
cluster or holds something that looks like a credential. OIDs and index
build job IDs are numbered in the recording, so recording again with no
change leaves the file as it was.

The `dsql` CI job runs `make test-dsql` and is a required check. A pull
request that changes a statement pistachio sends fails it until the
recording is made again. Without a cluster, leave the job failing and say so
in the pull request; a maintainer with a cluster records it.

The rest of this file is the evidence.

Connection:
- DSQL rejects the `default_transaction_read_only` startup parameter
  (`FATAL: setting configuration parameter "default_transaction_read_only"
  not supported`, SQLSTATE 0A000). `SET default_transaction_read_only = on`
  after connecting is rejected the same way. `SET SESSION CHARACTERISTICS
  AS TRANSACTION READ ONLY` works: `default_transaction_read_only` then reads
  `on`, and an autocommit `CREATE TABLE` fails with `cannot execute CREATE
  TABLE in a read-only transaction`. `--engine dsql` runs that statement
  after connecting, so `plan` and `dump` stay read-only (verified
  2026-10-10).

Catalog read layer (no incompatibility found):
- Every catalog dependency exists on DSQL. All 7 catalog functions that
  pistachio calls (`pg_get_constraintdef`, `pg_get_indexdef`,
  `pg_get_viewdef`, `pg_get_expr`, `pg_get_serial_sequence`,
  `pg_get_partkeydef`, `format_type`) and all 16 system catalogs that it
  queries (`pg_class`, `pg_namespace`, `pg_attribute`, `pg_attrdef`,
  `pg_type`, `pg_collation`, `pg_constraint`, `pg_index`, `pg_inherits`,
  `pg_depend`, `pg_description`, `pg_tablespace`, `pg_policy`, `pg_roles`,
  `pg_enum`, `pg_sequence`) resolve.
- The `dump` read paths were verified end to end on live objects. Tables,
  columns, PK and CHECK constraints, indexes, views, domains, and sequences
  all read back correctly. The catalog surface is compatible. The
  incompatibilities are in the returned values (see the drift items
  below), not in the queries.
- The partition and extension read paths (`pg_inherits`,
  `pg_get_partkeydef`, and the `pg_depend` subquery for extension
  ownership) also run without error. They return nothing, because DSQL
  cannot create those objects (see below). `dump` exits 0 with them
  present.

Objects that DSQL cannot create (out of scope, never in a DSQL desired
schema):
- Enums: `CREATE TYPE ... AS ENUM` -> `unsupported statement: CreateEnum`.
- Row-level security: `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` ->
  `unsupported ALTER TABLE ENABLE ROW SECURITY statement`; `CREATE POLICY`
  -> `unsupported statement: CreatePolicy`.
- Partitioned tables: `PARTITION BY` -> `PARTITION BY clause not supported
  for CREATE TABLE`; `PARTITION OF` -> `PARTITION OF clause not supported
  for CREATE TABLE`.
- Extensions: `CREATE EXTENSION` -> `unsupported statement:
  CreateExtension`. `pg_available_extensions` is empty, and `pg_extension`
  holds only the built-in `plpgsql`.
  The `pg_enum`, `pg_policy`, `pg_inherits` and `pg_depend` catalog reads
  still run, and they return nothing for these objects.

Sequences:
- `CREATE SEQUENCE` requires an explicit cache size. DSQL rejects a plain
  `CREATE SEQUENCE` with `CREATE SEQUENCE is not supported without an
  explicit cache size. please define CACHE greater than or equal to 65536
  or equal to 1`. pistachio always writes CACHE for a sequence, so this
  needs no change.
- An identity column requires an explicit cache size too
  (`identity column is not supported without an explicit cache size`),
  and must be `bigint`. The model leaves CACHE 1 out as the default, so
  `--engine dsql` writes it.

Tables and constraints:
- CREATE TABLE with an inline PRIMARY KEY applies successfully.
- Primary-key drift (a false positive): DSQL adds every non-key column as
  an `INCLUDE` column on the PK index, and it stores the access method as
  `btree_index`. So a table that was created from `PRIMARY KEY (id)`
  dumps back as `PRIMARY KEY (id) INCLUDE (name, email)`. Without the
  engine, pistachio treats this as a diff, and it re-plans
  `ALTER TABLE ... DROP CONSTRAINT ...; ALTER TABLE ... ADD CONSTRAINT ...`
  on every run. `--engine dsql` drops the `INCLUDE` from the current side.
- That generated fix cannot be applied either. DSQL rejects
  `ALTER TABLE ... DROP CONSTRAINT` on a primary key
  (`unsupported ALTER TABLE DROP CONSTRAINT statement`, SQLSTATE 0A000),
  and DSQL has no general `ALTER TABLE ... ADD CONSTRAINT`. The one
  exception, `USING INDEX`, is described below.

Column and constraint operations: reachable, or no path. For each ALTER
that pistachio emits, the question is not only whether the exact statement
errors. It is also whether another supported DSQL syntax can reach the
target state. Both categories were confirmed on the live cluster and
checked against the `ALTER TABLE` grammar. The grammar is exhaustive, so an
action that is absent from it has no alternative form.

Reachable through an alternative DSQL syntax (`--engine dsql` writes the
statements this way):
- Add a column with a DEFAULT. `ADD COLUMN col type DEFAULT expr` fails
  with `ALTER TABLE ADD COLUMN with constraint not supported`. But a plain
  `ADD COLUMN col type` followed by `ALTER COLUMN col SET DEFAULT expr`
  succeeds and yields the defaulted column. That is two statements instead
  of one. The rows already in the table keep NULL, since the column has no
  default when it is added.
- Add a UNIQUE constraint to an existing table. `ADD CONSTRAINT ... UNIQUE
  (col)` is not available. But this sequence succeeds and produces
  `UNIQUE (col)`: run `CREATE UNIQUE INDEX ASYNC`, wait for the build to
  reach VALID with `CALL sys.wait_for_job('<job_id>')`, then run
  `ALTER TABLE ... ADD CONSTRAINT name UNIQUE USING INDEX index_name`. This
  was confirmed. It requires the job wait between the two statements,
  which `--engine dsql` does after every `CREATE INDEX ASYNC`.

No alternative path (DSQL cannot do these to an existing table at all;
the actions are absent from the `ALTER TABLE` grammar):
- `DROP COLUMN` -> `unsupported ALTER TABLE DROP COLUMN statement`.
- `ALTER COLUMN ... SET NOT NULL` -> `unsupported ... SET NOT NULL
  statement`. `DROP NOT NULL` works, but there is no way to add NOT NULL
  to an existing column. There is no `SET NOT NULL`, and there is no
  `ADD CONSTRAINT CHECK` fallback, because ADD CONSTRAINT is limited to
  `UNIQUE USING INDEX`.
- `ALTER COLUMN ... TYPE` -> `unsupported ... SET DATA TYPE statement`.
- Add a NOT NULL column to an existing table. This is possible only at
  CREATE TABLE time. `ADD COLUMN` cannot carry NOT NULL, and there is no
  SET NOT NULL afterwards.
- Add a PRIMARY KEY or CHECK constraint to an existing table. The
  `USING INDEX` exception is for UNIQUE only. PK and CHECK are possible
  only at CREATE TABLE time.

Supported directly (no change needed): `ALTER COLUMN SET DEFAULT`,
`DROP DEFAULT`, `DROP NOT NULL`, and an inline PRIMARY KEY, UNIQUE or CHECK
at CREATE TABLE. A UNIQUE constraint round-trips cleanly, unlike a primary
key. Its `pg_get_constraintdef` is `UNIQUE (col)` with no INCLUDE.

Indexes (the original ASYNC question):
- pistachio emits `CREATE INDEX ... USING btree (col)`. DSQL rejects the
  access-method clause outright: `ERROR: USING not supported for CREATE
  INDEX`. So the statement fails before ASYNC matters. The
  `USING <method>` clause must be stripped.
- `CREATE INDEX ASYNC name ON t (col)`, with no USING, succeeds and
  returns a `job_id`. `sys.jobs` shows `INDEX_BUILD`. The build is
  asynchronous, so its completion must be awaited through `sys.jobs` or
  `sys.wait_for_job` before a dependent step runs.
- DSQL stores and reports the index access method as `btree_index`, not
  `btree`. pistachio's desired canonical form uses `btree`. So
  `equalIndexDef` reports perpetual drift even for an index that is
  already correct. This has the same root cause as the PK INCLUDE and
  method drift above.

Findings of 2026-10-10:
- Every `CREATE INDEX` needs ASYNC, even on an empty table
  (`unsupported mode. please use CREATE INDEX ASYNC.`). `CREATE INDEX
  ASYNC` returns one row, column `job_id` (text). `CALL
  sys.wait_for_job('<id>')` returns one row, column `succeeded` (bool).
  An index being built has `indisvalid = f`.
- An unquoted `async` right after `INDEX` is always the ASYNC keyword. An
  index named `async` has to be quoted.
- `CREATE INDEX CONCURRENTLY` -> `CONCURRENTLY not supported for CREATE
  INDEX`. `DROP INDEX CONCURRENTLY` fails even alone with `DROP INDEX
  CONCURRENTLY must be first action in transaction`. A plain `DROP INDEX`
  works.
- No advisory lock (`function pg_try_advisory_lock not supported`).
- One DDL statement per transaction (`multiple ddl statements not supported
  in a transaction`). A combined ALTER TABLE works, including `ADD COLUMN`
  with `SET DEFAULT` on the same column.
- A column added after the primary key is also added to the key's
  `INCLUDE`.
- Every text column has `attcompression = 'l'` (`default_toast_compression`
  is lz4). DSQL rejects both `SET COMPRESSION` and the `COMPRESSION`
  clause.
- `pg_class.reltuples` is -1 and `relpages` is 0.
- These work: renaming a table, column, constraint, index, view, sequence
  or domain; `SET SCHEMA`; `COMMENT ON`; `CREATE OR REPLACE VIEW`; view
  `SET`/`RESET`; dropping a CHECK or UNIQUE constraint; identity `SET
  GENERATED`, `SET INCREMENT BY`, `RESTART` and `DROP IDENTITY`; SQL
  functions; `CREATE DOMAIN` and a domain's DEFAULT.
- These fail: composite types (`unsupported statement: CompositeType`),
  materialized views (`unsupported statement: CreateTableAs`), `ALTER
  DOMAIN ... ADD CONSTRAINT`, `ALTER DOMAIN ... SET NOT NULL`, a column
  `COLLATE` (`COLLATE clause not supported`), and adding an identity column
  to an existing table (`ALTER TABLE ADD COLUMN with constraint not
  supported`).
- Limits: a transaction lasts at most 5 minutes and a connection at most
  60 minutes. Neither can be changed, and `sys.wait_for_job` takes no
  timeout.
