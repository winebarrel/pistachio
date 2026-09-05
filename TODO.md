# TODO

Items intentionally deferred from prior PRs. Each entry notes the originating
PR for context.

An entry marked `Priority: low` is drift that a `pista dump` output fed back
as the desired schema does not hit. Only a desired schema written some other
way reaches it, and writing it the way `dump` does avoids it.

## Auto-rewrite of column references in views and cross-table FKs

When a column is renamed via `-- pista:renamed-from`, the rewriter only
updates same-table dependents (indexes, constraints, FKs, triggers,
policies and generated expressions on the same table). The following
references are **not** rewritten and may produce a redundant `DROP/CREATE`
on the first plan (the second run after applying the rename is clean):

- View / materialized view definitions that `SELECT` the renamed column.
- Foreign keys in *other* tables whose `REFERENCES this_table(renamed_col)`
  points at the renamed column (PkAttrs side).

Resolving these requires cross-object awareness in the diff phase.

The rewrite does not reach a partition child. `diffTable` takes a separate
branch for one, which returns before the rewrite block, and a `PARTITION OF`
child declares no columns, so its own rename map is empty. A trigger or policy
created directly on a child is in the model, since the catalog excludes only
the clones the parent pushes down, so a rename on the parent leaves a redundant
statement on such a child. Carrying the rename there needs the parent's map.

Origin: [#123](https://github.com/winebarrel/pistachio/pull/123).

## Perpetual failure on a qualified reference in a generated expression

PostgreSQL accepts a table- or schema-qualified column reference in a generated
expression, `total integer GENERATED ALWAYS AS (items.qty * 2) STORED`, and
stores it stripped: `pg_get_expr` reads back `(qty * 2)`. The desired side
keeps what the file wrote, so the two never compare equal, and because a
generated expression cannot be altered in place the run fails with
`cannot change GENERATED expression` on every plan rather than merely drifting.
Nothing changes in the schema for it to report.

`stripQualifications` does this for a view body. Reaching the same result here
needs the table's own name at the point the expression is normalized, which
`equalSelectExpr` is not given.

The column-reference validator does not read a qualified name either, so a
stale one produces the diff error above rather than a message naming the
column.

Workaround: write the reference unqualified, which is what `pista dump` emits.

## INHERITS table plan / apply support

`model.Table.SQL` has an INHERITS branch that drops the child's own
columns and only emits constraints. As a result, plan / apply for an
`INHERITS (...)` child whose desired definition adds columns produces
incorrect DDL. The validator already special-cases INHERITS children
(skipped because the inherited column set isn't materialised on the
child), but the SQL emitter and diff don't handle the legacy partition
shape end-to-end.

Origin: [#125](https://github.com/winebarrel/pistachio/pull/125). Plan / apply fixtures were intentionally not added.

## Silent drift on the partition shape

`Table.PartitionOf`, `Table.PartitionBound` and `Table.PartitionDef` are read
from the catalog and parsed from the desired schema, but the diff only tests
`PartitionOf` and `PartitionBound` against nil to pick a branch and never
compares a value. Each of these plans `-- No changes` on a table that already
exists, verified on 15:

- Turning `PARTITION BY RANGE (r)` into `PARTITION BY LIST (r)`.
- Moving a partition's `FOR VALUES` bound.
- Re-parenting a partition from one parent to another.
- A plain table gaining `PARTITION OF`, or a partition losing it.

None of it is a plain `ALTER` away. A partition key cannot be changed at all,
so reaching the desired state means recreating the table and moving the data.
The other three run through `ALTER TABLE ... DETACH PARTITION` and
`ATTACH PARTITION ... FOR VALUES`: detaching is cheap, and attaching scans the
table and fails on a row the bound does not cover. Erroring at plan time may
fit better than emitting any of it.

Origin: review of [#383](https://github.com/winebarrel/pistachio/pull/383).

## Silent drift on `Table.TableSpace` / `Index.TableSpace` changes

The catalog and parser populate `Table.TableSpace` and `Index.TableSpace`,
but the diff layer never compares them. Changing a tablespace in desired
SQL after the object exists has no effect on the generated plan. Should
emit `ALTER TABLE ... SET TABLESPACE <new>` and
`ALTER INDEX ... SET TABLESPACE <new>`.

Origin: post-[#125](https://github.com/winebarrel/pistachio/pull/125) audit.

## Persistence on a partitioned table is ignored

`ALTER TABLE ... SET LOGGED` on a partitioned table reports success and changes
neither the parent nor the partitions under it, verified on 15, 16 and 17.
Emitting it would replan forever, so `diffPersistence` skips a partitioned
table, and a desired schema that flips one there is silently ignored.

There is nothing to reach anyway. The parent holds no storage for the value to
describe, and it does not reach the children either: a partition created under
an unlogged parent is permanent unless it says `UNLOGGED` itself. PostgreSQL 18
removed the form for that reason, so the shape cannot arise there at all.
Erroring at plan time, the way a domain base-type change does, would fail a
whole run over a value that means nothing, on a form a dump of a 15 to 17
database carries routinely. Recorded as a deliberate choice, not a bug.

Origin: [#384](https://github.com/winebarrel/pistachio/pull/384).

## `SET COMPRESSION` does not reach the partitions that already exist

`ALTER TABLE ... ALTER COLUMN ... SET COMPRESSION` never recurses: `ATPrepCmd`
carries `/* This command never recurses */` for `AT_SetCompression`, while
`AT_SetStorage` next to it calls `ATSimpleRecursion`. A partition created later
copies both settings off the parent attribute, so only a table that already has
partitions is hit: the parent's method changes and theirs does not. A type
change goes the same way, since `SET DATA TYPE` recurses and resets the
compression of every partition while the re-emit that follows it names the
parent alone.

Nothing catches it on the next run. A partition child declares no columns of
its own, so `diffTable` returns before `diffColumns` and the plan reads clean.
Closing it means emitting the statement once per partition, which the column
diff has no reason to walk otherwise.

Origin: review of [#442](https://github.com/winebarrel/pistachio/pull/442).

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

Priority: low.

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

A second limitation: `catalog.ListColumnsByTables` strips any constraint
name with the `_not_null` suffix to mask PG18's auto-naming (which does
not follow column or table renames). An explicit user name that happens
to end in `_not_null` is therefore lost on round-trip. A more precise
heuristic would need to compare against the auto-name pattern at the
time of the most recent rename, which is not available from the
catalog alone. The same test also misses every name PostgreSQL
disambiguated: `ChooseConstraintName` checks the name against the whole
schema, so two tables whose names survive the cut identically give the
second one a `_not_null1` name, which the suffix does not match, and
`dump` then writes an explicit `CONSTRAINT ..._not_null1 NOT NULL` for a
column the file declared as a plain `NOT NULL`. The round trip holds,
since the parser reads the name back and the diff emits a rename only
when both sides are named, so this is drift in the output rather than a
plan that repeats.

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

The codebase has no dialect layer today, so this is new work. This section
records the verified gaps, not a commitment to implement DSQL support.

Origin: live DSQL investigation, 2026-07-23.

## Composite type: ALTER ATTRIBUTE ... TYPE while a table column uses the type

`ALTER TYPE ... ALTER ATTRIBUTE ... TYPE` fails at apply when a table column
references the composite type (`cannot alter type "x" because column "..."
uses it`, SQLSTATE 0A000). `CASCADE` does not lift the restriction, and
`ADD` / `DROP` / `RENAME ATTRIBUTE` are not affected. The plan looks valid
but apply rolls back, so nothing is destroyed. Domain base-type changes
already error at plan time; composite attribute type changes could do the
same, but that needs the diff to know which composite types are referenced by
a table column (cross-object awareness the composite diff does not have
today). For now the limitation is documented in the README.

Origin: [#331](https://github.com/winebarrel/pistachio/pull/331).

## Composite type: attribute reordering is not diffed

Attributes are matched by name, so a desired schema that only reorders
attributes produces no diff. This matches PostgreSQL, which has no operation
to reorder composite type attributes (`ADD ATTRIBUTE` always appends). A
mid-list `ADD` therefore appends the new attribute; the result is
order-independent and stable across plans. Recorded as a deliberate choice,
not a bug.

Origin: [#331](https://github.com/winebarrel/pistachio/pull/331).

## Silent drift on cross-schema user-type references

Priority: low.

A table column, or a composite type attribute, whose type is a user-defined
type (enum, domain, or composite) written schema-qualified in desired SQL
drifts on every plan when the type's schema differs from the container's own
schema. The catalog reads the type via `format_type`, which returns it
unqualified when it is in the search_path, while the desired parser keeps the
qualified form. The diff (`equalTypeName`) strips the container's own schema
from both sides, so `home public.addr` on a table in `public` no longer
drifts, but a type in a different search-path schema than its table or
composite type (e.g. a `shared` schema on the search_path) is still compared
qualified-vs-unqualified and emits a redundant `SET DATA TYPE`.

Closing it fully needs the target-schema list in the diff (to strip any
search-path schema, not just the container's own), which the diff does not
thread today. Workaround: write such a reference unqualified.

Origin: [#331](https://github.com/winebarrel/pistachio/pull/331).

## Sequence ownership transitions: `OWNED BY NONE` plans an unusable CREATE

Detaching a sequence from its column cannot be expressed. The parser reads
`ALTER SEQUENCE ... OWNED BY NONE` and clears the owner, so the statement is
accepted, but nothing emits the detaching DDL. A desired schema carrying it
for a sequence the database still owns plans a `CREATE SEQUENCE` for a
sequence that already exists, and apply fails with
`relation "..." already exists` (SQLSTATE 42P07).

`catalog.Sequences` drops every sequence with an owner, so the current side
never sees it. After `OWNED BY NONE` the desired side holds no owner, which
makes the sequence a managed standalone object, and the diff reads the
missing current entry as "not created yet". The opposite direction is
handled: `ALTER SEQUENCE ... OWNED BY <column>` marks the sequence unmanaged
on the desired side, matching the catalog, so an owned sequence no longer
replans forever.

Closing this means letting the catalog surface sequences that are merely
owned, kept apart from the serial and identity ones that stay column
attributes (the distinction `catalog.ListColumnsByTables` already draws by
checking that the column default draws from the sequence), and teaching the
diff to emit `ALTER SEQUENCE ... OWNED BY` / `OWNED BY NONE` for the
transitions. That widens the set of objects pistachio manages, so it is a
feature rather than a fix. Workaround: detach the sequence by hand.

Origin: bug audit, 2026-07-31.

## `COMMENT ON COLUMN` on an inherited column of an INHERITS child

A comment on a column an `INHERITS` child inherits from its parent is dropped
at parse time. `parseCommentStmt` needs the column to be present on the
table, and such a child declares only its own columns. A true partition child
takes a different path: it declares no columns at all, so the comment creates
the entry, and `diffTable` reaches that entry through a branch that never
diffs columns. An INHERITS child goes through the regular column diff, where
an entry holding only a name and a comment would be read as a new column and
emit `ADD COLUMN`, so the same trick does not carry over.

Closing this needs the inherited column set materialised on the child, which
is the same prerequisite as the INHERITS plan / apply entry above.

Origin: review of [#340](https://github.com/winebarrel/pistachio/pull/340).

## A dump of a partitioned table with an index on the parent does not reload

Priority: low.

`dump` writes each table followed by its own indexes, so an index on a
partitioned parent is created before the partitions under it. PostgreSQL then
creates the partition's copy itself, under the name the dump goes on to
declare, and psql stops on `relation "logs_2025_at_idx" already exists`.
pg_dump avoids it by writing every table first and every index after, with an
`ALTER INDEX ... ATTACH PARTITION` for each child.

Feeding the dump back to `pista plan` is clean, which is the contract, so this
only reaches someone loading a dump with psql. `test/fidelity` is where it
turned up, and its `partition.sql` leaves the parent index out with a note
pointing here. Putting it back is the regression test.

Emitting the indexes after every table, or following pg_dump with `ON ONLY`
plus `ATTACH`, are the two shapes. The first changes the order of every dump.

Origin: [#459](https://github.com/winebarrel/pistachio/pull/459).

## View and materialized view storage parameters are not read

Priority: low.

A view carries `security_barrier` and `check_option`, a materialized view the
autovacuum settings, and neither side reads them. `dump` drops the clause, and
a desired schema that writes it plans nothing, the way a table's parameters
behaved before they were read. Losing `security_barrier` from a dump is the
one that matters, since the view is a security boundary without it.

`pg_class.reloptions` holds them for both, next to the table's, and
`ALTER VIEW ... SET (...)` / `RESET (...)` and the `ALTER MATERIALIZED VIEW`
forms are what a change goes out as. The table work put the reading and the
rendering in `SortedStorageParams` and `Table.StorageParamsSQL`, which the view
model can use as they are, and `--manage-storage-param` is the gate they would
sit behind.

`dump` writes no clause and the parser reads none, so a dump fed back plans
clean and only a hand-written view reaches this.

Origin: review of [#458](https://github.com/winebarrel/pistachio/pull/458).

## A constraint's index storage parameters are not read

Priority: low.

`ALTER TABLE t ADD CONSTRAINT t_v_key UNIQUE (v) WITH (fillfactor = 70)` puts
the parameters on the index the constraint owns. `pg_get_constraintdef` does
not print them, so the current side reads the constraint as `UNIQUE (v)` and
the two never match: the plan is `DROP CONSTRAINT` + `ADD CONSTRAINT` on every
run, which rebuilds the index under an ACCESS EXCLUSIVE lock. `dump` drops the
clause for the same reason, so a table's parameters survive a dump and a
constraint's do not.

The parameters are on `pg_class` under `pg_constraint.conindid`, next to the
ones `ListIndexes` already reads, and appending them to the definition is what
would make both sides agree. `normalizeStorageParams` then folds the quoting
the way it does for a plain index.

`dump` writes the catalog form, so a dump fed back plans clean and only a
hand-written constraint reaches this.

Workaround: create the index separately, `CREATE UNIQUE INDEX ... WITH
(fillfactor = 70)`, where the parameters are read.

Origin: [#458](https://github.com/winebarrel/pistachio/pull/458).

## An EXCLUDE constraint is not normalized at all

Priority: low.

`equalConstraintDef` normalizes `RawExpr`, which is where a `CHECK` body sits.
An exclusion constraint puts its elements in `Constraint.Exclusions` and its
predicate in `Constraint.WhereClause`, and neither goes through
`normalizeCheckExpr`, so the two sides are compared as the parser left them.

Every normalization the walk performs is therefore missing there. A
schema-qualified call drifts, `EXCLUDE USING btree (public.lower_v(v) WITH =)`
being re-emitted as `DROP CONSTRAINT` + `ADD CONSTRAINT` on every plan, and so
does a cast the catalog adds and the file does not: `((v || 'x') WITH =)` is
stored as `((v || 'x'::text) WITH =)`. The folds in `diff/desugar.go` are
missing too, so the `BETWEEN` in
`EXCLUDE USING gist (r WITH &&) WHERE (a BETWEEN 1 AND 10)` drifts here alone.
This is wider than the schema question and predates the strip.

Passing `Exclusions` and `WhereClause` through the same walk is the obvious
shape, and the elements are `IndexElem` nodes, which `normalizeIndexStmt`
already knows how to handle.

`dump` writes the catalog form, so a dump fed back plans clean and only a
hand-written exclusion reaches this.

Workaround: write the exclusion the way `pg_get_constraintdef` prints it.

Origin: review of [#455](https://github.com/winebarrel/pistachio/pull/455).

## Perpetual drift on a typed literal the catalog re-prints

Priority: low.

A constant written with a type name is stored as the value that type's input
function produced, not as the text that produced it. `CHECK (a > timestamp
'2000-01-01')` comes back from `pg_get_constraintdef` as
`CHECK ((a > '2000-01-01 00:00:00'::timestamp without time zone))`, so the two
sides never compare equal and the `CHECK` is dropped and re-added on every
plan, revalidating the whole table. An index predicate, a view body, a policy,
a trigger `WHEN` and a domain `CHECK` drift the same way, and a generated
column fails the run, since it cannot be altered in place.

`a AT TIME ZONE 'UTC' > '2000-01-01'` is the same case: the right operand
resolves to `timestamp without time zone` and is re-printed in that type's
output form.

This is the one rewrite `diff/desugar.go` does not undo. The others are
syntactic, so the fold is a tree rewrite the grammar already defines. Matching
a literal means running the type's input and output functions over it, which
would put a query to the server in the middle of the comparison.

`dump` writes the catalog form, so a dump fed back plans clean and only a
hand-written literal reaches this.

Workaround: write the literal the way `pg_get_constraintdef` prints it, or
leave the type off.

Origin: expression normalization review, 2026-08-30.

## Perpetual drift on a schema-qualified sequence in a column DEFAULT

Priority: low.

A column `DEFAULT nextval('public.counter'::regclass)` is re-emitted as
`ALTER COLUMN ... SET DEFAULT` on every plan. Applying it succeeds and changes
nothing, so `plan --check` stays at exit code 2 forever. `pg_get_expr` returns
the sequence unqualified when its schema is on the search_path, while the
desired side keeps what the user wrote.

The call itself no longer drifts: `stripFuncSchema` (`diff/tables.go`) drops
the schema from a `FuncCall` name symmetrically, which reaches every site
`normalizeCheckExpr` covers. The sequence here is not in that name. It sits
inside a string literal argument, so reaching it means reading the regclass
literal, quoted identifiers included. `equalDefault` applies no schema
normalization of any kind beyond the walk.

A function moved between two schemas is the cost of the symmetric strip:
`a.f(v)` and `b.f(v)` compare equal, so the move produces no diff. This is the
tradeoff a view body's table reference already carries. Telling them apart
means the search_path-aware stripping described in the cross-schema user-type
entry above, which the diff cannot do today because it does not thread the
schema list.

Workaround: write the sequence unqualified.

Origin: found while adding `-- pista:execute-first`, 2026-07-31. The function
call half was closed later; the literal half is what remains.

## A pg_dump serial column loses its default

Priority: low.

`pg_dump` never writes `serial`. It splits the column into a plain `integer`, a
`CREATE SEQUENCE`, an `ALTER SEQUENCE ... OWNED BY`, and a separate
`SET DEFAULT`. The parser reads the first three and drops the fourth, since
`ALTER TABLE ... ALTER COLUMN ... SET DEFAULT` has no handler. It warns and
moves on.

Against the database the file came from, this plans clean. The catalog reports
such a column as `serial` with no default, the desired side holds `integer`
with no default, and `equalTypeName` treats the two type names as equal.

Against an empty database it does not reproduce the column. The plan is
`CREATE TABLE public.t (id integer NOT NULL)` alone: the sequence is unmanaged
because `OWNED BY` marks it so, and the default was dropped. What comes out is
a plain integer column.

Reading `AT_ColumnDefault` onto the column is the fix, next to the
`AT_SetStorage` and `AT_SetCompression` actions `applyAlterTableColumnStorage`
already reads. The `nextval` argument names the sequence, so the serial shape
is recoverable from it.

Origin: bug audit, 2026-07-31. Rewritten once the drift it described stopped
happening: the warn-and-drop of an unsupported `ALTER TABLE` action, added in
1.31.0, is what makes the two sides agree.

## Perpetual drift on a view defined with `SELECT *`

Priority: low, with the caveat that the consequence is heavier than the rest
of the class: with the view drop allowed, every apply drops and recreates the
view.

A view whose desired definition uses `SELECT *` or `t.*` is re-emitted on
every plan. `pg_get_viewdef` returns the star expanded into an explicit
column list, while the desired side keeps the star, so `equalViewDef`
(`diff/views.go`) never matches. `canCreateOrReplaceView` reports the output
columns as undeterminable for the same reason, which routes the change
through `DROP`+`CREATE`: under the default drop policy the plan prints
`-- skipped: DROP VIEW ...` and then `-- No changes`, and with the view drop
allowed every apply drops and recreates the view, taking its privileges and
dependent views with it. Applies to plain views, `t.*`, and a star over a
subquery alike.

Expanding the star on the desired side needs the column list of every FROM
item. `DiffViews` receives only the view maps, so the table columns would
have to be threaded in, and `model.View` carries a definition rather than
columns, so a star over another view has to be resolved recursively from
that definition. Since `equalViewDef` already strips table qualification,
the expansion only has to produce the column names in the right order, not
the qualified form the catalog prints.

What the star should mean is a separate decision. PostgreSQL expands it at
`CREATE` time and freezes the result, so adding a column to a base table
leaves the view alone. Expanding against the desired tables makes a column
addition re-emit `CREATE OR REPLACE VIEW` for every star view over that
table; expanding against the catalog matches PostgreSQL but never
propagates the new column.

Faithful expansion also has to reproduce PostgreSQL's name resolution:
`JOIN ... USING` and `NATURAL JOIN` yield the join column once,
`FROM (...) s(a, b)` renames the subquery's columns through the alias, and
function FROM items and `LATERAL` cannot be resolved from the desired
schema at all.

Workaround: write the column list explicitly. `pista dump` emits the
expanded form.

## SQL/JSON forms that still drift

Priority: low. A dump feeds back clean, so writing the file the way `pista
dump` emits it avoids all of this.

A SQL/JSON expression is compared as written, and the server rewrites most of
what it is handed, so a file written any other way re-emits its view or
constraint on every plan. Each form below was observed on every server that
has the syntax:

- The path is stored in canonical form. `'$.x'` comes back as `'$."x"'`,
  `'$.x ? (@ > 1)'` as `'$."x"?(@ > 1)'` and `'$.x + 1'` as `'($."x" + 1)'`.
  Every supported server, since the jsonpath type predates 15.
- `JSON_OBJECT`, `JSON_ARRAY`, `JSON_OBJECTAGG` and `JSON_ARRAYAGG` resolve
  `RETURNING json` or `RETURNING jsonb` from their argument types, and
  `pg_get_viewdef` prints the one the server picked. 16 and later.
- The query functions resolve a `RETURNING` type too, text for `JSON_VALUE`
  and `JSON_SERIALIZE` and jsonb for `JSON_QUERY`, and `JSON_QUERY` prints
  its wrapper and quote behaviour whether or not they hold the default, so a
  file leaving them off differs from `WITHOUT WRAPPER KEEP QUOTES`. 17 and
  later.
- `JSON_TABLE` is a FROM item rather than an expression, and the server adds
  a `LATERAL` and names the row pattern: `JSON_TABLE(t.a, '$.x' COLUMNS (k
  text PATH '$.k'))` is stored as `LATERAL JSON_TABLE(t.a, '$."x"' AS
  json_table_path_0 COLUMNS (k text PATH '$."k"'))`. 17 and later.

Closing any of it means reproducing what the server resolved. The path needs
a jsonpath expression parser, since pg_query hands the path over as a plain
string constant. The `RETURNING` type needs type inference over the
arguments, which pg_query does not do.

Origin: [#371](https://github.com/winebarrel/pistachio/pull/371).

## View drift on a target alias the catalog writes differently

Priority: low.

PostgreSQL names a sub-select's output column on its own and `pg_get_viewdef`
prints the name it chose, so a view whose body holds
`COALESCE((SELECT max(s.n) FROM t s WHERE s.a = t.a), 0)` comes back with
`SELECT max(s.n) AS max` inside. The written form carries no alias there, the
two `ResTarget` names disagree, and `CREATE OR REPLACE VIEW` is re-emitted on
every plan. Reproducing the name means reimplementing `FigureColname`, which
walks the expression to pick a function name, a column name or `?column?`.
`figureIndexColname` (`parser/parser.go`) already walks an expression this way
for an index element, so the shared half of the work is in the tree.

Workaround: write the alias the way `pista dump` emits it.

Origin: [#371](https://github.com/winebarrel/pistachio/pull/371).

## Deparsed SQL/JSON query functions carry stray spaces

libpg_query's deparser puts a space where a `JsonFuncExpr` needs none, in
three places:

- before the comma, `JSON_VALUE(t.a , '$."x"')`
- before the closing parenthesis of a `RETURNING` clause with nothing after
  it, `JSON_VALUE(t.a , '$."x"' RETURNING text )`
- twice between a `RETURNING` type and what follows it, `JSON_QUERY(t.a ,
  '$."x"' RETURNING jsonb  WITHOUT WRAPPER KEEP QUOTES)`

All of it is valid SQL and only shows up in emitted DDL, so it costs nothing
beyond looking wrong in a plan that re-emits a view holding one of the
PostgreSQL 17 query functions. The shapes are pinned by
testdata/plan/alter_json_query_clauses.yml. The fix belongs upstream.

Origin: [#371](https://github.com/winebarrel/pistachio/pull/371).

## Routine renaming is not supported

`-- pista:renamed-from` works on tables, views, enums, domains, composite
types, sequences, columns, constraints, foreign keys, indexes, policies and
triggers. It does not work on a function or a procedure: the directive on a
`CREATE FUNCTION` or `CREATE PROCEDURE` is an error rather than an
`ALTER FUNCTION ... RENAME TO`.

Dropping and recreating a routine loses no data, so a rename would mostly keep
the plan readable rather than protect anything. The identity carries the
argument types, so the directive would take a full signature
(`-- pista:renamed-from public.old_name(integer)`).

Origin: routine support.

## Routine create order ignores what the body reads

Routines are created after the types their signature names and before every
table, because a `CHECK` constraint, a `GENERATED` expression, an index
expression, a policy or a trigger can call one. The edge from a table to a
routine is drawn wholesale in `addRoutineDeps` rather than by reading the
expressions, since the answer is the same "routine first" either way.

The reverse direction is not modelled at all. A `LANGUAGE sql` routine whose
body reads a table created in the same run fails to apply, because PostgreSQL
parses a SQL body at creation time; so does one that calls another routine
defined later in the file. `plpgsql` is unaffected. The workaround is
`-- pista:ignore` plus `-- pista:execute`.

Fixing it means reading the body for the relations and routines it names and
ordering on that. Both directions would then live in one graph, which holds
only per pair, so the wholesale edge would have to go and every CHECK,
GENERATED, index, policy and trigger expression would have to be read instead.

Origin: routine support.

## Schema mapping does not rewrite a routine body

`-m old=new` rewrites the schema of a routine and the type names in its
signature, including a parameter default. It does not touch the body, which is
opaque text in whatever language the routine is written in. A body that
qualifies a table or a function with the old schema keeps the old name.

A view definition gets the same replacer because it is always SQL. A routine
body is not, so substituting a prefix in it would be a guess.

Priority: low.

Origin: routine support.

## SQL-standard routine bodies (BEGIN ATOMIC) are not managed

A routine written as `LANGUAGE sql BEGIN ATOMIC ... END` is skipped on both
sides of the diff: the catalog query filters on `prosqlbody IS NULL` and the
parser warns and drops it. So neither `dump` writes one nor `plan` proposes
dropping one.

pg_query parses and deparses the form. The obstacle is that PostgreSQL resolves
such a body at creation time and records real `pg_depend` entries on whatever it
reads, so the routine cannot be created ahead of the tables the way every other
routine is, and a referenced table cannot be dropped while it exists. Supporting
it needs the body-dependency work in the entry above.

`pg_get_functiondef` also re-deparses the stored parse tree, so the body comes
back with names resolved (`SELECT a FROM t` reads back as `SELECT t.a FROM t`),
the same drift views handle with `stripQualifications`.

Origin: routine support.

## Aggregates and window functions are not managed

`prokind` `'a'` and `'w'` are filtered out of the catalog query, and the parser
warns about a `CREATE FUNCTION ... WINDOW` and drops it, so the two sides stay
symmetric. `CREATE AGGREGATE` has a shape `model.Routine` does not cover.

Origin: routine support.

## `COMMENT ON INDEX` is not managed

Priority: low.

Comments are managed for tables, columns, views, materialized views, sequences,
enums, composite types, domains and routines. Indexes, constraints, triggers and
policies have no `Comment` field and no case in `parseCommentStmt`, so a
`COMMENT ON INDEX` in the desired schema is dropped without an error or a diff.
`pista dump` does not emit one either, so a dump feeds back clean. `pg_dump`
does emit them, and those lines are lost.

The work is a `Comment` field on `model.Index`, a `pg_description` join in
`catalog.ListIndexes`, an `OBJECT_INDEX` case in the parser, and emission from
`diffIndexes` and the create path, since recreating an index drops its comment.
`COMMENT ON INDEX` names only the schema and the index, so the parser has to
scan `Table.Indexes` and `View.Indexes` by name. Index names are unique per
schema, and constraint-backed indexes are already excluded from `ListIndexes`.

Better done together with constraint, trigger and policy comments. Shipping it
makes `dump` emit index comments, so a database that has one where the desired
schema does not will plan `COMMENT ON INDEX ... IS NULL;`.

Origin: discussion, 2026-08-25.

## An identity column's sequence name is not managed

The sequence behind an identity column is created as `<table>_<column>_seq` and
`pista dump` never writes the `SEQUENCE NAME` that `pg_dump` does, so a
sequence renamed by hand restores under the default name. The options the
sequence carries are managed; only the name is not.

Reading it is one more column in the identity read, and `CREATE TABLE` takes
`SEQUENCE NAME` inside the identity options, so `dump` is easy. The diff is
not: changing the name on an existing column is `ALTER SEQUENCE ... RENAME TO`,
and pistachio takes a rename from a `-- pista:renamed-from` directive rather
than inferring one, so a directive would have to reach a column's sequence.

Origin: identity sequence options, 2026-09-02.

## `WITH CHECK OPTION` on a view is not managed

Nothing reads `pg_class.reloptions` `check_option` or parses the trailing
`WITH [CASCADED | LOCAL] CHECK OPTION`, so the clause is dropped from a view in
the desired schema without an error and `pista dump` does not emit one. An
updatable view loses the constraint on restore.

The work is a field on `model.View`, a catalog read, the parser clause, and
emission from the create and replace paths. Related to the view storage
parameters entry above, which reads the same `reloptions` column.

Origin: the restore fidelity check, 2026-09-02.
`test/fidelity/schemas/view_columns.sql` notes what it leaves out.

## A NOT VALID check on an INHERITS child is restored validated

[#505](https://github.com/winebarrel/pistachio/pull/505) fixed this for a
plain and a partitioned table and left the INHERITS branch of `Table.SQL`
alone: a child's constraints are all written inside `CREATE TABLE`, so a
NOT VALID check declared directly on the child loses the clause, and the dump
fed back plans a `VALIDATE CONSTRAINT` for it. Emitting the ALTER the way a
plain table now does is not enough, because the child's constraint map also
holds the unvalidated clones a NOT VALID check on the parent pushes down, and
an ADD for one of those collides with the constraint the child already
inherits. Telling the two apart needs `pg_constraint.conislocal`, which the
catalog does not read.

Origin: review of [#505](https://github.com/winebarrel/pistachio/pull/505).
