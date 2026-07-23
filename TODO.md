# TODO

Items intentionally deferred from prior PRs. Each entry notes the originating
PR for context.

## Auto-rewrite of column references in views and cross-table FKs

When a column is renamed via `-- pista:renamed-from`, the rewriter only
updates same-table dependents (indexes, constraints, FKs on the same
table). The following references are **not** rewritten and may produce a
redundant `DROP/CREATE` on the first plan (the second run after applying
the rename is clean):

- View / materialized view definitions that `SELECT` the renamed column.
- Foreign keys in *other* tables whose `REFERENCES this_table(renamed_col)`
  points at the renamed column (PkAttrs side).

Resolving these requires cross-object awareness in the diff phase.

Origin: [#123](https://github.com/winebarrel/pistachio/pull/123).

## Validation of column refs in GENERATED / DEFAULT expressions

`ValidateColumnRefs` checks index / constraint / FK definitions against
the desired column set. It does not currently walk:

- `GENERATED ALWAYS AS (<expr>) STORED` expressions on columns
- `DEFAULT <expr>` expressions on columns

A typo or stale rename in these expressions still surfaces only at apply
time. Adding a walk over `model.Column.Default` for both kinds (gated by
`Generated`) would close this gap.

Origin: [#124](https://github.com/winebarrel/pistachio/pull/124).

## INHERITS table plan / apply support

`model.Table.SQL` has an INHERITS branch that drops the child's own
columns and only emits constraints. As a result, plan / apply for an
`INHERITS (...)` child whose desired definition adds columns produces
incorrect DDL. The validator already special-cases INHERITS children
(skipped because the inherited column set isn't materialised on the
child), but the SQL emitter and diff don't handle the legacy partition
shape end-to-end.

Origin: [#125](https://github.com/winebarrel/pistachio/pull/125). Plan / apply fixtures were intentionally not added.

## Silent drift on `Table.TableSpace` / `Index.TableSpace` changes

The catalog and parser populate `Table.TableSpace` and `Index.TableSpace`,
but the diff layer never compares them. Changing a tablespace in desired
SQL after the object exists has no effect on the generated plan. Should
emit `ALTER TABLE ... SET TABLESPACE <new>` and
`ALTER INDEX ... SET TABLESPACE <new>`.

Origin: post-[#125](https://github.com/winebarrel/pistachio/pull/125) audit.

## Silent drift on `Table.Unlogged` toggle

`model.Table.Unlogged` is set on parse and used to emit `CREATE UNLOGGED TABLE`,
but transitions on existing tables (logged <-> unlogged) are not diffed.
PostgreSQL has `ALTER TABLE ... SET LOGGED` / `SET UNLOGGED`. The current
`no_diff_unlogged_table.yml` fixture only covers the unchanged case.

Origin: post-[#125](https://github.com/winebarrel/pistachio/pull/125) audit.

## `Column.StorageType` is dead code

`model.Column.StorageType` is declared but never read or written by
parser / catalog / diff. Either implement column storage diffs
(`ALTER COLUMN ... SET STORAGE PLAIN|EXTERNAL|EXTENDED|MAIN`) or drop
the field. Today the value is always the zero string.

Origin: post-[#125](https://github.com/winebarrel/pistachio/pull/125) audit.

## Constraint `Deferrable` / `Deferred` granularity

`Constraint.Deferrable` / `Constraint.Deferred` are read from the
catalog but not compared directly in the diff. Changes are still
detected because they end up in the `pg_get_constraintdef` Definition
string, which means a deferrable toggle currently triggers DROP+ADD
of the constraint. PostgreSQL supports
`ALTER TABLE ... ALTER CONSTRAINT ... [NOT] DEFERRABLE [INITIALLY ...]`
for in-place changes; using it would avoid the round-trip.

Origin: post-[#125](https://github.com/winebarrel/pistachio/pull/125) audit. Optimisation rather than a bug; current
behaviour is correct, just heavier than necessary.

## Table rename: cross-table dependents

`detectTableRenames` rewrites the renamed table's own indexes and FKs
in the adjusted current state, but other tables' `FOREIGN KEY ... REFERENCES old_table(...)`
and view definitions that reference the old table name are not rewritten.
PostgreSQL auto-updates these on RENAME, so a second plan/apply comes out
clean, but the first plan can emit redundant drop/recreate operations for
the dependent objects. Same scope as the existing column-rename TODO above
("Auto-rewrite of column references in views and cross-table FKs"),
extended to table-level renames.

Origin: pre-existing NOTE in `diff/rename.go:detectTableRenames`.

## Policy USING / WITH CHECK normalization for subquery column refs

`pg_get_expr` qualifies column references inside subqueries (e.g. emits
`SELECT allowed.id FROM myschema.allowed` for a USING that the user wrote
as `SELECT id FROM myschema.allowed`). The desired-side parser deparses
without that qualification, so even semantically-identical USING /
WITH CHECK expressions produce a spurious `ALTER POLICY` when subqueries
are involved.

`equalSelectExpr` (formerly `equalPolicyExpr`, renamed in #207 for shared
use across policy / generated-column expressions) reuses `normalizeCheckExpr`
from constraint diffs, which strips text-like casts and canonicalises
`= ANY(ARRAY[...])` -> `IN (...)`, but does not walk into subqueries and
rewrite ColumnRef qualifications.

Fix would be to walk `SubLink` / `RangeSubselect` nodes and strip column
qualifiers that match the FROM-clause table alias. The same approach
would also benefit constraint CHECK expressions if they ever contain
subqueries (uncommon; PostgreSQL discourages them).

Origin: post-RLS-support audit. Workaround: avoid subqueries in policy
expressions, or use a function that wraps the subquery.

## TO CURRENT_USER / SESSION_USER / CURRENT_ROLE in CREATE POLICY

PostgreSQL resolves these reserved role specs at policy creation time
and stores the resolved role OID in `pg_policy.polroles`. As a result,
desired SQL written as `TO current_user` cannot round-trip through the
catalog: subsequent plans see the resolved role name (e.g. `postgres`)
and emit a spurious `ALTER POLICY ... TO`.

Recommendation is to use literal role names in desired SQL. The parser
accepts the reserved specs for convenience but the limitation should be
documented prominently.

Origin: post-RLS-support audit. No fix planned. This is a PostgreSQL
behavior that affects the catalog round-trip, not a pistachio bug.

## Named NOT NULL constraints: name add/remove on existing columns

`Column.NotNullName` round-trips on PG18, and a name change between two
named NOT NULL constraints is emitted as `RENAME CONSTRAINT`. The
following transitions are no-ops in v1:

- nullable -> NOT NULL with an explicit desired name: emits `SET NOT NULL`
  (PG auto-generates a name) but does not apply the desired name.
- NOT NULL with explicit current name -> still NOT NULL but unnamed: keeps
  the current name in place.

Both require PG18's standalone `ALTER TABLE ... ADD CONSTRAINT name NOT NULL col`
syntax, which `pg_query_go` does not yet parse (libpg_query PR #317 is
still in draft as of 2026-05). Once that lands, the parser can accept
the standalone form and the diff can drop the no-op branches.

A second limitation: `catalog.ListColumnsByTable` strips any constraint
name with the `_not_null` suffix to mask PG18's auto-naming (which does
not follow column or table renames). An explicit user name that happens
to end in `_not_null` is therefore lost on round-trip. A more precise
heuristic would need to compare against the auto-name pattern at the
time of the most recent rename, which is not available from the
catalog alone.

A third limitation: on PG<18 the parser still captures the inline name,
but PostgreSQL silently drops it at apply time. The diff layer treats
the resulting "current has no name, desired has a name" mismatch as a
no-op (the same v1 behavior used for adding a name to an existing
NOT NULL on PG18), so no drift loop occurs; the explicit name is
simply not honored on PG<18. This is a PG18-only feature and should
be documented as such if it ever surfaces in user-facing docs.

Origin: [#157](https://github.com/winebarrel/pistachio/pull/157).

## CREATE OR REPLACE VIEW: type-only change on a same-named column

`canCreateOrReplaceView` (`diff/views.go`) decides between
`CREATE OR REPLACE VIEW` and `DROP`+`CREATE` by comparing the output
column *names* in order. When only a column's *type* changes but the name
stays (e.g. `SELECT n FROM t` -> `SELECT n::bigint AS n FROM t`), the names
still line up, so the plan emits `CREATE OR REPLACE VIEW`. PostgreSQL then
rejects it at apply time with `cannot change data type of view column`,
so a clean-looking plan fails on execution.

`pg_query` does not perform type inference, so the type change can't be
detected statically from the SELECT alone. Resolving it would require
either type resolution against the desired schema or always routing view
definition changes through `DROP`+`CREATE` (which costs dependent objects
and privileges). Workaround: adjust the source DDL or drop the view in a
pre-step.

Origin: known limitation documented inline at `diff/views.go`
(`canCreateOrReplaceView`).

## Plan-time error promotion: forgotten dependent reference

When desired SQL references the new column name in a dependent
definition but forgets to add `-- pista:renamed-from` on the column
itself, current behavior is to produce DDL that fails at apply time.
The `ValidateColumnRefs` pass added in [#124](https://github.com/winebarrel/pistachio/pull/124) already catches the
inverse case (renamed column with stale dependent reference). The
forgotten-rename direction could in principle also be caught (e.g. by
detecting "column X is in current but not desired AND a column with a
similar name exists in desired") but the heuristic has false positives
and is not pursued.

Origin: discussion during [#124](https://github.com/winebarrel/pistachio/pull/124). No current plan to implement.

## UNLOGGED sequence support

Standalone sequence management does not track the `UNLOGGED` attribute. An
`UNLOGGED SEQUENCE` is read and dumped as a plain `CREATE SEQUENCE` (as if
`LOGGED`), and a `LOGGED` <-> `UNLOGGED` change produces no diff. Closing
this would follow the existing table pattern: add an `Unlogged` field to
`model.Sequence`, read `pg_class.relpersistence = 'u'` in the catalog,
emit `CREATE UNLOGGED SEQUENCE`, and emit `ALTER SEQUENCE ... SET
LOGGED/UNLOGGED` for transitions. `TEMPORARY` sequences stay out of scope
(session-local, never dumped).

Origin: [#296](https://github.com/winebarrel/pistachio/pull/296).

## Amazon Aurora DSQL support: findings from live testing

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
- Put the behavior behind an explicit DSQL mode. There is no dialect layer
  today, and the default PostgreSQL output must not change.

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

The codebase has no dialect layer today, so this is new work. This section
records the verified gaps, not a commitment to implement DSQL support.

Origin: live DSQL investigation, 2026-07-23.
