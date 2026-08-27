# TODO

Items intentionally deferred from prior PRs. Each entry notes the originating
PR for context.

An entry marked `Priority: low` is drift that a `pista dump` output fed back
as the desired schema does not hit. Only a desired schema written some other
way reaches it, and writing it the way `dump` does avoids it.

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

## Trigger definitions are not rewritten on a column rename

When a column is renamed via `-- pista:renamed-from`, the rewriter updates the
same-table dependents it knows about (indexes, constraints, FKs). A trigger on
the same table is not among them, so a `UPDATE OF <col>` list or a `WHEN`
expression naming the old column reads as a definition change and the first
plan emits a redundant `CREATE OR REPLACE TRIGGER`. PostgreSQL applies
`RENAME COLUMN` to the trigger itself, so the second run after the rename is
clean.

Same class as the view and cross-table FK entry above, and the fix is the same
shape: run the column-rename rewriter over `Table.Triggers` and
`View.Triggers`.

Origin: trigger support.

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

A second limitation: `catalog.ListColumnsByTable` strips any constraint
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
attributes (the distinction `catalog.ListColumnsByTable` already draws by
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

## Perpetual drift on a schema-qualified function call in a CHECK constraint

Priority: low.

A CHECK constraint that calls a function schema-qualified, written as
`CHECK (public.lower_v(v) <> 'x')`, is re-emitted as `DROP CONSTRAINT` +
`ADD CONSTRAINT` on every plan. Applying it succeeds and changes nothing, so
`plan --check` stays at exit code 2 forever. `pg_get_constraintdef` returns
the call unqualified when the function's schema is on the search_path, while
the desired side keeps what the user wrote.

`normalizeCheckExpr` (`diff/tables.go`) strips text-like casts and
canonicalises `= ANY(ARRAY[...])` -> `IN (...)`, but never touches the
`FuncCall` name, so the two forms deparse differently. Index expressions
and index predicates run through `normalizeCheckExpr` as well
(`normalizeIndexStmt`), so the same drift is expected there. A view body
that calls a function qualified drifts the same way: `stripQualifications`
(`diff/views.go`) clears `RangeVar.Schemaname` and never looks at the call.

A column `DEFAULT nextval('public.counter'::regclass)` drifts too, but not
through the same node. The schema sits inside a string literal argument
rather than in a `FuncCall` name, so stripping the name symmetrically leaves
it; reaching it means reading the regclass literal. `equalDefault` applies no
schema normalization of any kind.

The codebase already normalizes schema qualification three different ways.
`stripQualifications` (`diff/views.go`) clears `RangeVar.Schemaname` and a
table-qualified `ColumnRef` unconditionally on both sides, without
consulting the search_path. `normalizeFKSchema` (`diff/tables.go`) goes the
other way and fills an empty schema in with the owning table's. And
`stripTypeSchema` (`diff/tables.go`) removes only the container's own
schema prefix.

The view approach is the cheapest fit here: strip the schema from a
`FuncCall` name symmetrically in `normalizeCheckExpr`. It carries the same
tradeoff views already accept, that two same-named functions in different
schemas compare equal. The stricter alternative is the search_path-aware
stripping described in the cross-schema user-type entry above, which the
diff cannot do today because it does not thread the schema list.

Workaround: write the call unqualified.

Origin: found while adding `-- pista:execute-first`, 2026-07-31.

## Perpetual drift on a serial column written as an explicit default

Priority: low.

A serial column written the way `pg_dump` writes it plans
`ALTER COLUMN id SET DEFAULT nextval(...)` on every run. Applying it succeeds
and changes nothing, so `plan --check` stays at exit code 2 forever. `pg_dump`
never writes `serial`; it splits the column into a plain `integer`, a
`CREATE SEQUENCE`, an `ALTER SEQUENCE ... OWNED BY`, and a separate
`SET DEFAULT`. The catalog reports such a column as `serial` with no default,
which is what lets `id serial` round-trip, while the desired side keeps the
explicit default. `equalTypeName` already treats `serial` and `integer` as
equal; the defaults are what differ.

`dump` does not produce this shape, so a schema kept through `pista dump`
never hits it. It shows up when a `pg_dump` output is used as the starting
desired file, which is a normal way to adopt the tool on an existing
database.

Comparing them needs the sequence the current column draws from, which the
model does not carry, since the catalog nulls the default for serial columns.
Suppressing the statement whenever the current column is serial and the
desired default is any `nextval` would also swallow a genuine change to a
different sequence.

Origin: bug audit, 2026-07-31.

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

## Sample database coverage has no Routines column

The table in `sample-db-test.md` counts tables, columns, indexes, FKs,
constraints, views, types, sequences and triggers per sample. The counts predate
`--manage-routine`, so routines have no column yet.

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
