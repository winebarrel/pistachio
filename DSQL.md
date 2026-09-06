# Amazon Aurora DSQL

pistachio does not support DSQL. This file records what a live cluster
answered, so the next attempt starts from evidence rather than from the
PostgreSQL manual. It is not a commitment to implement it.

Verified against a live DSQL cluster (PostgreSQL 16 wire protocol,
ap-northeast-1). Adding ASYNC to CREATE INDEX is not enough. Within
DSQL's supported feature set, pistachio does not yet reach a stable
no-drift state.

Features DSQL does not support (foreign keys, triggers, PL/pgSQL, etc.)
are out of scope. A DSQL-targeted schema never contains them, so pistachio
never emits them. The findings below are cases where the target state is
within DSQL's supported set but pistachio's transition DDL or drift
comparison is wrong for it.

Support policy (if DSQL support is added):
- Work correctly within DSQL's supported feature set. Do not reproduce
  every PostgreSQL feature on DSQL.
- Leave unsupported diffs out of spec. When a diff needs an operation DSQL
  has no path for (`DROP COLUMN`, `SET NOT NULL`, column `TYPE` change,
  adding a NOT NULL column, adding a PK/CHECK constraint to an existing
  table), pistachio emits standard PostgreSQL DDL and DSQL rejects it at
  apply. That is acceptable, the same as any unsupported feature. Do not
  add recreation or back-fill machinery to force these through.
- Gate DSQL support behind an opt-in option (flag or env var). Without it,
  behavior stays exactly as today. There is no dialect layer now, so the
  DSQL paths must be additive and must not change the default PostgreSQL
  output.

The concrete work is listed under "Minimum a DSQL mode would require"
below. The rest of this section is the evidence.

Connection:
- DSQL rejects the `default_transaction_read_only` startup parameter
  (`FATAL: setting configuration parameter "default_transaction_read_only"
  not supported`, SQLSTATE 0A000). `plan` / `dump` must be run with
  `--no-read-only`. A DSQL mode would need to stop sending this parameter.

Catalog read layer (no incompatibility found):
- Every catalog dependency exists on DSQL. All 7 catalog functions
  (`pg_get_constraintdef`, `pg_get_indexdef`, `pg_get_viewdef`,
  `pg_get_expr`, `pg_get_serial_sequence`, `pg_get_partkeydef`,
  `format_type`) and all 16 system catalogs pistachio queries
  (`pg_class`, `pg_namespace`, `pg_attribute`, `pg_attrdef`, `pg_type`,
  `pg_collation`, `pg_constraint`, `pg_index`, `pg_inherits`, `pg_depend`,
  `pg_description`, `pg_tablespace`, `pg_policy`, `pg_roles`, `pg_enum`,
  `pg_sequence`) resolve.
- `dump` read paths verified end to end on live objects: tables, columns,
  PK/CHECK constraints, indexes, views, domains, and sequences all read
  back correctly. The catalog surface is compatible; the incompatibilities
  are in the returned values (see drift items below), not the queries.
- The partition and extension read paths (`pg_inherits`,
  `pg_get_partkeydef`, and the `pg_depend` extension-ownership subquery)
  also run without error; they simply return nothing because DSQL cannot
  create those objects (see below). `dump` exits 0 with them present.

Objects DSQL cannot create (out of scope, never in a DSQL desired schema):
- Enums: `CREATE TYPE ... AS ENUM` -> `unsupported statement: CreateEnum`.
- Row-level security: `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` ->
  `unsupported ALTER TABLE ENABLE ROW SECURITY statement`; `CREATE POLICY`
  -> `unsupported statement: CreatePolicy`.
- Partitioned tables: `PARTITION BY` -> `PARTITION BY clause not supported
  for CREATE TABLE`; `PARTITION OF` -> `PARTITION OF clause not supported
  for CREATE TABLE`.
- Extensions: `CREATE EXTENSION` -> `unsupported statement:
  CreateExtension`. `pg_available_extensions` is empty; `pg_extension`
  holds only the built-in `plpgsql`.
  The `pg_enum` / `pg_policy` / `pg_inherits` / `pg_depend` catalog reads
  still run and return empty for these.

Sequences:
- `CREATE SEQUENCE` requires an explicit cache size: DSQL rejects a plain
  `CREATE SEQUENCE` with `CREATE SEQUENCE is not supported without an
  explicit cache size. please define CACHE greater than or equal to 65536
  or equal to 1`. pistachio emits `CREATE SEQUENCE` without CACHE, so
  sequence apply fails; the read path (with a CACHE-qualified sequence)
  works.

Tables and constraints:
- CREATE TABLE with an inline PRIMARY KEY applies successfully.
- Primary-key drift (false positive): DSQL auto-adds all non-key columns
  as `INCLUDE` columns on the PK index, and stores the access method as
  `btree_index`. So a table created from `PRIMARY KEY (id)` dumps back as
  `PRIMARY KEY (id) INCLUDE (name, email)`. pistachio treats this as a
  diff and re-plans on every run with
  `ALTER TABLE ... DROP CONSTRAINT ...; ALTER TABLE ... ADD CONSTRAINT ...`.
- That generated fix is itself inapplicable: DSQL rejects
  `ALTER TABLE ... DROP CONSTRAINT` on a primary key
  (`unsupported ALTER TABLE DROP CONSTRAINT statement`, SQLSTATE 0A000),
  and DSQL has no general `ALTER TABLE ... ADD CONSTRAINT` (see below for
  the one `USING INDEX` exception).

Column / constraint operations: reachable vs. no path. For each ALTER
that pistachio emits, the question is not just "does the exact statement
error" but "can the target state be reached by another supported DSQL
syntax." Both categories were confirmed on the live cluster and checked
against the `ALTER TABLE` grammar (the grammar is exhaustive; an action
absent from it has no alternative form).

Reachable via alternative DSQL syntax (pistachio would need to emit
differently):
- Add a column with a DEFAULT. `ADD COLUMN col type DEFAULT expr` fails
  (`ALTER TABLE ADD COLUMN with constraint not supported`), but the plain
  `ADD COLUMN col type` followed by `ALTER COLUMN col SET DEFAULT expr`
  succeeds and yields the defaulted column. Two statements instead of one.
- Add a UNIQUE constraint to an existing table. `ADD CONSTRAINT ... UNIQUE
  (col)` is unavailable, but `CREATE UNIQUE INDEX ASYNC`, wait for the
  build to reach VALID (`CALL sys.wait_for_job('<job_id>')`), then
  `ALTER TABLE ... ADD CONSTRAINT name UNIQUE USING INDEX index_name`
  succeeds and produces `UNIQUE (col)`. Confirmed. Note this requires the
  job-wait step between the two statements, which pistachio's flat
  synchronous apply loop does not do today.

No alternative path (DSQL genuinely cannot do it to an existing table;
these actions are simply absent from the `ALTER TABLE` grammar):
- `DROP COLUMN` -> `unsupported ALTER TABLE DROP COLUMN statement`.
- `ALTER COLUMN ... SET NOT NULL` -> `unsupported ... SET NOT NULL
  statement`. `DROP NOT NULL` works, but there is no way to add NOT NULL
  to an existing column (no `SET NOT NULL`, and no `ADD CONSTRAINT CHECK`
  fallback since ADD CONSTRAINT is limited to `UNIQUE USING INDEX`).
- `ALTER COLUMN ... TYPE` -> `unsupported ... SET DATA TYPE statement`.
- Add a NOT NULL column to an existing table (only possible at CREATE
  TABLE time; `ADD COLUMN` cannot carry NOT NULL and there is no post-hoc
  SET NOT NULL).
- Add a PRIMARY KEY or CHECK constraint to an existing table (the
  `USING INDEX` exception is UNIQUE-only; PK/CHECK are CREATE TABLE-only).

Supported directly (no change needed): `ALTER COLUMN SET DEFAULT`,
`DROP DEFAULT`, `DROP NOT NULL`; inline PRIMARY KEY / UNIQUE / CHECK at
CREATE TABLE. A UNIQUE constraint round-trips cleanly (its
`pg_get_constraintdef` is `UNIQUE (col)` with no INCLUDE), unlike a
primary key.

Indexes (the original ASYNC question):
- pistachio emits `CREATE INDEX ... USING btree (col)`. DSQL rejects the
  access-method clause outright: `ERROR: USING not supported for CREATE
  INDEX`. So this fails before ASYNC even matters; the `USING <method>`
  clause must be stripped.
- `CREATE INDEX ASYNC name ON t (col)` (no USING) succeeds and returns a
  `job_id`; `sys.jobs` shows `INDEX_BUILD`. The build is asynchronous, so
  completion must be awaited via `sys.jobs` / `sys.wait_for_job` before
  dependent steps run.
- DSQL stores and reports the index access method as `btree_index`, not
  `btree`. pistachio's desired canonical form uses `btree`, so
  `equalIndexDef` reports perpetual drift even for an already-correct
  index (same root cause as the PK INCLUDE/method drift above).

Minimum a DSQL mode would require, per the above:
- Do not send `default_transaction_read_only`.
- Index generation: `CREATE INDEX` -> `CREATE INDEX ASYNC`, drop the
  `USING <method>` clause, and poll `job_id` to completion.
- Drift normalization: ignore DSQL's auto-`INCLUDE` on PK indexes and
  treat `btree` and `btree_index` as equivalent when comparing current
  vs desired.
- Sequence generation: emit an explicit `CACHE` (>= 65536 or = 1); DSQL
  rejects a plain `CREATE SEQUENCE`.
- Rewrite two reachable transitions into DSQL's alternative multi-step
  form instead of failing:
  - Column DEFAULT add -> plain `ADD COLUMN` then `SET DEFAULT`.
  - Existing-table UNIQUE add -> `CREATE UNIQUE INDEX ASYNC` + job-wait +
    `ADD CONSTRAINT ... UNIQUE USING INDEX`. This needs the same async
    job-wait plumbing as index creation.
- Detect-and-error (with a clear message) the transitions that have no
  DSQL path, rather than emitting DDL that fails at apply: `DROP COLUMN`,
  `SET NOT NULL`, column `TYPE` change, adding a NOT NULL column, and
  adding a PK/CHECK constraint to an existing table.

The catalog read layer needs no DSQL-specific work; the gaps are all on
the DDL-generation and drift-comparison side. The async job-wait (for both
index creation and the UNIQUE USING INDEX path) is the largest change,
since the apply loop currently sends each statement synchronously.

The codebase has no dialect layer today, so this is new work.

Origin: live DSQL investigation, 2026-07-23.
