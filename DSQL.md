# Amazon Aurora DSQL

pistachio does not support DSQL. Support looks feasible, but it is not
clear that anyone needs it, so the work has not been started. This file
records what a live cluster answered, so that an attempt starts from
evidence rather than from the PostgreSQL manual.

The findings were verified against a live DSQL cluster (PostgreSQL 16 wire
protocol, ap-northeast-1). Adding ASYNC to CREATE INDEX is not enough.
Within DSQL's supported feature set, pistachio does not yet reach a stable
state with no drift.

The features that DSQL does not support (foreign keys, triggers, PL/pgSQL,
and so on) are out of scope. A schema that is written for DSQL never
contains them, so pistachio never emits them. The findings below are the
cases where the target state is within DSQL's supported set, but
pistachio's transition DDL or its drift comparison is wrong for it.

Support policy, if DSQL support is added:
- Work correctly within DSQL's supported feature set. Do not reproduce
  every PostgreSQL feature on DSQL.
- Leave the diffs that DSQL cannot apply out of the specification. Some
  diffs need an operation that DSQL has no path for: `DROP COLUMN`,
  `SET NOT NULL`, a column `TYPE` change, adding a NOT NULL column, and
  adding a PK or CHECK constraint to an existing table. For these,
  pistachio emits standard PostgreSQL DDL, and DSQL rejects it at apply.
  That is acceptable, the same as for any unsupported feature. Do not add
  recreation or back-fill machinery to force these through.
- Gate DSQL support behind an opt-in option (a flag or an environment
  variable). Without it, the behavior stays exactly as it is today. There
  is no dialect layer now, so the DSQL paths must be additive and must not
  change the default PostgreSQL output.

The concrete work is listed under "Minimum a DSQL mode would require"
below. The rest of this file is the evidence.

Connection:
- DSQL rejects the `default_transaction_read_only` startup parameter
  (`FATAL: setting configuration parameter "default_transaction_read_only"
  not supported`, SQLSTATE 0A000). `plan` and `dump` must be run with
  `--no-read-only`. A DSQL mode would need to stop sending this parameter.

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
  or equal to 1`. pistachio emits `CREATE SEQUENCE` without CACHE, so
  applying a sequence fails. The read path works on a sequence that was
  created with CACHE.

Tables and constraints:
- CREATE TABLE with an inline PRIMARY KEY applies successfully.
- Primary-key drift (a false positive): DSQL adds every non-key column as
  an `INCLUDE` column on the PK index, and it stores the access method as
  `btree_index`. So a table that was created from `PRIMARY KEY (id)`
  dumps back as `PRIMARY KEY (id) INCLUDE (name, email)`. pistachio treats
  this as a diff, and it re-plans
  `ALTER TABLE ... DROP CONSTRAINT ...; ALTER TABLE ... ADD CONSTRAINT ...`
  on every run.
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

Reachable through an alternative DSQL syntax (pistachio would need to emit
the statements differently):
- Add a column with a DEFAULT. `ADD COLUMN col type DEFAULT expr` fails
  with `ALTER TABLE ADD COLUMN with constraint not supported`. But a plain
  `ADD COLUMN col type` followed by `ALTER COLUMN col SET DEFAULT expr`
  succeeds and yields the defaulted column. That is two statements instead
  of one.
- Add a UNIQUE constraint to an existing table. `ADD CONSTRAINT ... UNIQUE
  (col)` is not available. But this sequence succeeds and produces
  `UNIQUE (col)`: run `CREATE UNIQUE INDEX ASYNC`, wait for the build to
  reach VALID with `CALL sys.wait_for_job('<job_id>')`, then run
  `ALTER TABLE ... ADD CONSTRAINT name UNIQUE USING INDEX index_name`. This
  was confirmed. Note that it requires the job-wait step between the two
  statements. pistachio's flat, synchronous apply loop does not do that
  today.

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

Minimum a DSQL mode would require, per the above:
- Do not send `default_transaction_read_only`.
- Index generation: turn `CREATE INDEX` into `CREATE INDEX ASYNC`, drop
  the `USING <method>` clause, and poll the `job_id` to completion.
- Drift normalization: ignore the `INCLUDE` that DSQL adds to a PK index,
  and treat `btree` and `btree_index` as equivalent when comparing the
  current side with the desired side.
- Sequence generation: emit an explicit `CACHE` (>= 65536 or = 1),
  because DSQL rejects a plain `CREATE SEQUENCE`.
- Rewrite the two reachable transitions into DSQL's alternative multi-step
  form instead of failing:
  - Adding a column DEFAULT -> a plain `ADD COLUMN`, then `SET DEFAULT`.
  - Adding a UNIQUE constraint to an existing table ->
    `CREATE UNIQUE INDEX ASYNC`, a job wait, then
    `ADD CONSTRAINT ... UNIQUE USING INDEX`. This needs the same async
    job-wait plumbing as index creation.
- Detect the transitions that have no DSQL path, and fail with a clear
  message instead of emitting DDL that fails at apply: `DROP COLUMN`,
  `SET NOT NULL`, a column `TYPE` change, adding a NOT NULL column, and
  adding a PK or CHECK constraint to an existing table.

The catalog read layer needs no DSQL-specific work. The gaps are all on
the DDL-generation and drift-comparison side. The async job wait, for both
index creation and the UNIQUE USING INDEX path, is the largest change,
because the apply loop currently sends each statement synchronously.

The codebase has no dialect layer today, so this is new work.

Origin: live DSQL investigation, 2026-07-23.
