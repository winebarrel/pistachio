# Known limitations

This document lists what pistachio does not handle, and what closing each one
would take. Each entry notes where it came from.

An entry marked `Priority: low` is drift that a `pista dump` output does not
hit. Writing the schema the way `dump` writes it avoids the drift.

## A rename is not carried into another object's reference

`-- pista:renamed-from` adjusts the current side so that the change is planned
as a rename rather than a drop and a create. The adjustment reaches only the
renamed object's own dependents: a column's indexes, constraints, foreign keys,
triggers, policies and generated expressions on the same table, a table's own
indexes, foreign keys and triggers, and a view's own triggers and indexes. Two
references are left as they were:

- A view or materialized view definition that names the renamed table or
  selects the renamed column.
- A foreign key in another table whose `REFERENCES` names the renamed table or
  column.

PostgreSQL updates such a reference itself on RENAME. So the second run is
clean, and only the first plan carries a redundant drop and create for the
dependent object. Closing this limitation needs cross-object awareness in the
diff phase.

A rename of an enum, domain, composite type or sequence is carried into the
type of a column, a composite attribute or a domain, and into a `nextval()`
default. It is not carried into a type that is named inside an expression, or
into a routine's arguments. The first plan drops and adds a CHECK constraint
whose expression names the type. It drops and creates an index whose
expression names the type. It replaces a view that names the type. It plans a
routine that takes the type as a new signature next to a drop of the old one.

A reference that is written without its schema is carried only when that
schema is on `--search-path`, because the catalog writes the reference that
way only then. `$user` in the path is not expanded. So a schema that is named
after the role counts only when the path names it. When two schemas on the
path hold a type of the same name, a bare reference is taken to mean the
renamed one, even where it resolves to the other.

A column rename does not reach a partition child either. `diffTable` takes a
separate branch for a partition child, and that branch returns before the
rewrite block. A `PARTITION OF` child declares no columns, so its own rename
map is empty. A trigger, policy or index that is created directly on a child
is in the model, because the catalog excludes only the clones that the parent
pushes down. So a rename on the parent leaves a redundant statement on such a
child. Carrying the rename there needs the parent's map.

Origin: [#123](https://github.com/winebarrel/pistachio/pull/123) for the column
half, a NOTE in `diff/rename.go:detectTableRenames` for the table half.

## Perpetual failure on a qualified reference in a generated expression

PostgreSQL accepts a table- or schema-qualified column reference in a generated
expression, such as `total integer GENERATED ALWAYS AS (items.qty * 2) STORED`.
It stores the expression stripped: `pg_get_expr` reads back `(qty * 2)`. The
desired side keeps what the file wrote, so the two never compare equal. A
generated expression cannot be altered in place. Therefore the run fails with
`cannot change GENERATED expression` on every plan rather than merely drifting.
Nothing changes in the schema for it to report.

`stripQualifications` does this for a view body. Reaching the same result here
needs the table's own name at the point where the expression is normalized.
`equalSelectExpr` is not given that name.

The column-reference validator does not read a qualified name either. So a
stale qualified name produces the diff error above rather than a message that
names the column.

Workaround: write the reference unqualified. That is what `pista dump` emits.

## Silent drift on the partition shape

`Table.PartitionOf`, `Table.PartitionBound` and `Table.PartitionDef` are read
from the catalog and parsed from the desired schema, but the diff reads none of
them. `Table.IsPartitionChild` tests the first two against nil to pick a
branch, and no value is ever compared. Each of the following changes plans
`-- No changes` on a table that already exists. This was verified on 15 and
18:

- Turning `PARTITION BY RANGE (r)` into `PARTITION BY LIST (r)`.
- Moving a partition's `FOR VALUES` bound.
- Re-parenting a partition from one parent to another.
- A plain table gaining `PARTITION OF`, or a partition losing it.

None of these changes is a plain `ALTER` away. A partition key cannot be
changed at all. So reaching the desired state means recreating the table and
moving the data. The other three run through `ALTER TABLE ... DETACH PARTITION`
and `ATTACH PARTITION ... FOR VALUES`. Detaching is cheap. Attaching scans the
table and fails on a row that the bound does not cover. An error at plan time
may fit better than emitting any of it.

Origin: review of [#383](https://github.com/winebarrel/pistachio/pull/383).

## A tablespace changes nothing on a table and re-creates an index

Priority: low. `dump` writes no tablespace on an index, so its output does not
hit the drift below. What `dump` leaves out is the heavier half: an index that
is loaded from that output lands in the default tablespace.

The catalog and the parser populate `Table.TableSpace` and `Index.TableSpace`.
The diff compares neither. The two sides go wrong differently.

A table's tablespace is silent drift. Changing it in the desired SQL after the
table exists produces no statement at all. `ALTER TABLE ... SET TABLESPACE
<new>` is what the change takes.

An index's tablespace is not silent. `pg_get_indexdef` never writes the
clause, while the parser leaves it in the definition that it deparses. So
`CREATE INDEX ... TABLESPACE ts1` never compares equal to what the catalog
hands back, even when the index already sits in `ts1`. Every plan drops the
index and creates it again. This was verified on 16:

```
DROP INDEX public.t_id_idx;
CREATE INDEX t_id_idx ON public.t USING btree (id) TABLESPACE ts1;
```

`dump` writes a table's `TABLESPACE` and not an index's. So a dump that is fed
back plans clean, and only a file that writes the clause on an index drifts.

Closing this limitation means comparing both fields and emitting
`ALTER TABLE ... SET TABLESPACE` and `ALTER INDEX ... SET TABLESPACE`. It also
means keeping the clause out of the definition that an index is compared by,
and writing the clause from `dump`.

Workaround: leave the clause off an index, and move the index with
`ALTER INDEX ... SET TABLESPACE` by hand.

Origin: post-[#125](https://github.com/winebarrel/pistachio/pull/125) audit.
The index half was found re-checking the entry, 2026-09-20.

## `SET COMPRESSION` does not reach the partitions that already exist

`ALTER TABLE ... ALTER COLUMN ... SET COMPRESSION` never recurses. `ATPrepCmd`
carries `/* This command never recurses */` for `AT_SetCompression`, while
`AT_SetStorage` next to it calls `ATSimpleRecursion`. A partition that is
created later copies both settings off the parent attribute. So only a table
that already has partitions is hit: the parent's method changes and the
partitions' method does not. A type change goes the same way. `SET DATA TYPE`
recurses and resets the compression of every partition, while the re-emit that
follows it names the parent alone.

Nothing catches this on the next run. A partition child declares no columns of
its own, so `diffTable` returns before `diffColumns`, and the plan reads clean.
Closing this limitation means emitting the statement once per partition. The
column diff has no other reason to walk the partitions.

Origin: review of [#442](https://github.com/winebarrel/pistachio/pull/442).

## An existing table's policy cannot read a relation the same run creates

A new table's policies are created after every table and view. An existing
table's policy changes stay with the table. Turning RLS on, adding a policy,
recreating one and `ALTER POLICY` run next to each other at the table's place
in the plan. So a live table is never left without its RLS or its policies.
Such a policy fails when its expression reads a table or view that the same
run creates. Workaround: create the relation first, and change the policy in a
later run.

Origin: moving `CREATE POLICY` after the views, 2026-09-23.

## Policy USING / WITH CHECK normalization for subquery column refs

Priority: low.

`pg_get_expr` qualifies the column references in a policy's sub-query. A
`USING (owner IN (SELECT id FROM allowed))` comes back as
`owner IN (SELECT allowed.id FROM allowed)`. A correlated reference to the
policy's own table is qualified too: `WHERE a.id = owner` comes back as
`WHERE a.id = docs.owner`. The catalog also prints `= ANY (SELECT ...)` as
`IN (SELECT ...)`. The desired side keeps what the file wrote. So a policy
with a sub-query plans an `ALTER POLICY` on every run.

`equalSelectExpr` compares policy expressions through `normalizeCheckExpr`.
The walk of `normalizeCheckExpr` reaches sub-queries, but none of its
normalizations strips a column qualifier. Closing this limitation means
dropping a qualifier that names the sub-query's own relation or alias, or the
policy's table. `normalizeCheckExpr` does not do that today.

Workaround: write the sub-query the way `pista dump` emits it, or wrap it in a
function.

Origin: post-RLS-support audit.

## Named NOT NULL constraints: name add/remove on existing columns

`Column.NotNullName` round-trips on PG18. A name change between two named NOT
NULL constraints is emitted as `RENAME CONSTRAINT`. Two transitions leave the
name alone:

- A nullable column that becomes NOT NULL with a desired name gets
  `SET NOT NULL`, and PostgreSQL gives the constraint its own name.
- A named NOT NULL that the desired side declares unnamed keeps its name.

Neither transition needs new syntax. A `RENAME CONSTRAINT` after the
`SET NOT NULL` gives the first transition the desired name. A
`RENAME CONSTRAINT` to the name that PostgreSQL would choose covers the second.
Reading PG18's standalone `ALTER TABLE ... ADD CONSTRAINT name NOT NULL col` in
a desired file waits on the parser: `pg_query_go` v6 embeds PostgreSQL 17.7 and
has no 18 release.

A second limitation: `catalog.ListColumnsByTables` strips any constraint name
that has the `_not_null` suffix. This masks PG18's auto-naming, which does not
follow column or table renames. Therefore an explicit user name that happens
to end in `_not_null` is lost on round-trip. A more precise heuristic would
need to compare the name against the auto-name pattern at the time of the most
recent rename. That time is not available from the catalog alone. The same
test also misses every name that PostgreSQL disambiguated.
`ChooseConstraintName` checks the name against the whole schema. So when two
tables have names that survive the cut identically, the second one gets a
`_not_null1` name, which the suffix does not match. `dump` then writes an
explicit `CONSTRAINT ..._not_null1 NOT NULL` for a column that the file
declared as a plain `NOT NULL`. The round trip holds, because the parser reads
the name back and the diff emits a rename only when both sides are named. So
this is drift in the output rather than a plan that repeats.

A third limitation: on PG<18 the parser still captures the inline name, but
PostgreSQL drops it at apply time. The diff treats the resulting mismatch,
where the current side has no name and the desired side has a name, as a
no-op. It treats the first transition above on PG18 the same way. So no drift
loop occurs. The name is not kept before PG18.

Origin: [#157](https://github.com/winebarrel/pistachio/pull/157).

## CREATE OR REPLACE VIEW: type-only change on a same-named column

`canCreateOrReplaceView` (`diff/views.go`) decides between
`CREATE OR REPLACE VIEW` and `DROP`+`CREATE` by comparing the output column
names in order. When only a column's type changes but the name stays (e.g.
`SELECT n FROM t` -> `SELECT n::bigint AS n FROM t`), the names still line up.
So the plan emits `CREATE OR REPLACE VIEW`. PostgreSQL then rejects the
statement at apply time with `cannot change data type of view column`. So a
plan that looks clean fails on execution.

`pg_query` does not perform type inference, so the type change cannot be
detected statically from the SELECT alone. Resolving this limitation would
require either type resolution against the desired schema or routing every
view definition change through `DROP`+`CREATE`. The second option costs
dependent objects and privileges. Workaround: adjust the source DDL, or drop
the view in a pre-step.

Origin: known limitation documented inline at `diff/views.go`
(`canCreateOrReplaceView`).

## A table or column drop is not checked for dependents

PostgreSQL refuses to drop a table or a column that another object reads. It
does not cascade. So a plan that holds such a drop fails at apply time.
`diffAll` checks `pg_depend` for the views that it drops, the keys and indexes
that it drops, the columns that it retypes and the routines that it recreates.
When one of these has a dependent, `diffAll` fails the plan instead. Two
statements are not checked:

- `DROP TABLE`
- `ALTER TABLE ... DROP COLUMN`

Neither statement fails unless the dependent stays in place. A dependent that
pistachio manages and that the plan drops goes before the drop. A dependent
stays in place when:

- The plan replaces a view or a `BEGIN ATOMIC` routine that reads the table or
  column with a definition that no longer reads it. `CREATE OR REPLACE` runs
  after the table changes. So the old definition still holds the dependency
  when the drop runs. Change the dependent in one run and drop in the next.

- `--include` / `--exclude` or a schema outside `-n` hides a view that reads
  the table or column, or a foreign key that references the table.
- `--allow-drop` names `table` but not `view` or `foreign_key`. The plan
  keeps a view that reads the table, or a foreign key on another table that
  references it. The plan still drops the table.
- A routine that pistachio does not manage takes the table's row type as an
  argument, or reads the table or column in a `BEGIN ATOMIC` body. Routines
  are unmanaged without `--manage-routine`, so this case needs no filter. A
  body that is written as a string records no dependency.

The check is left out for two reasons. These cases are rare, and a filter is
the user's choice. The check would also have to skip every dependent that the
plan drops or changes before the table, in the order the plan runs them. A
skip that it gets wrong fails a plan that applies cleanly. PostgreSQL's error
names the dependent, and `--with-tx` rolls the apply back.

Closing it means reporting the tables and columns that a diff drops, under
their current names when the plan also renames the table. It also means
reading the dependents of each table, column (`pg_depend.refobjsubid`) and row
type.

Origin: review of the view-dependent check, 2026-09-19. The routine and
foreign key cases were added 2026-09-26, and the replaced dependent
2026-09-28.

## Amazon Aurora DSQL is not supported

A schema that is written for DSQL never holds what DSQL cannot create. So the
obstacles are fewer than DSQL's list of unsupported features suggests. The
connection sends a parameter
that DSQL rejects. `CREATE INDEX` carries an access method that DSQL refuses,
and its build has to be awaited. A primary key reads back with columns that
pistachio did not write. Several transitions have a DSQL syntax that pistachio
does not emit.

A live cluster answered each of these questions. The answers are in
[DSQL.md](https://github.com/winebarrel/pistachio/blob/main/DSQL.md) rather
than here, because they are evidence for the work rather than the work itself.

Origin: live DSQL investigation, 2026-07-23.

## Composite type: ALTER ATTRIBUTE ... TYPE while a table column uses the type

`ALTER TYPE ... ALTER ATTRIBUTE ... TYPE` fails at apply when a table column
references the composite type (`cannot alter type "x" because column "..."
uses it`, SQLSTATE 0A000). `CASCADE` does not lift the restriction. `ADD` /
`DROP` / `RENAME ATTRIBUTE` are not affected. The plan looks valid, but apply
rolls back, so nothing is destroyed. A domain base-type change already errors
at plan time. A composite attribute type change could do the same. But that
needs the diff to know which composite types a table column references, and
the composite diff does not have that cross-object awareness today. The
supported objects page (`docs/reference/objects.md`) records it.

Origin: [#331](https://github.com/winebarrel/pistachio/pull/331).

## Silent drift on cross-schema user-type references

Priority: low.

A table column, or a composite type attribute, can have a user-defined type
(enum, domain, or composite) that the desired SQL writes schema-qualified.
Such a column or attribute drifts on every plan when the type's schema differs
from the container's own schema. The catalog reads the type through
`format_type`. `format_type` returns the type unqualified when its schema is on
`--search-path`. The desired parser keeps the qualified form. The diff
(`equalTypeName`) strips the container's own schema from both sides. So
`home public.addr` on a table in `public` no longer drifts. But a type whose
schema is on `--search-path` and differs from the schema of its table or
composite type is still compared qualified against unqualified, and the diff
emits a redundant `SET DATA TYPE`. With the default `--search-path public`,
that is a `public` type used from a table in another schema, such as
`a public.addr` on `app.t`.

Closing it fully needs the target-schema list in the diff, so that the diff can
strip any search-path schema and not only the container's own. The diff does
not thread that list today. Workaround: write such a reference unqualified.

Origin: [#331](https://github.com/winebarrel/pistachio/pull/331).

## A foreign key moved between same-named tables in two schemas is missed

A foreign key reference can be written without a schema on one side and with
it on the other. The diff matches such a reference in two ways: through the
schema that the key records for the referenced table, and through the owning
table's schema. The diff also tries the owning table's schema for the bare
name, because a hand-written bare name often means it. The second match hides
a real change when two schemas hold a table of the same name. With
`-n public,app` and both `public.base` and `app.base`:

- A key on `app.item` that references `public.base` reads back from the
  catalog as `REFERENCES base(id)`. A desired `REFERENCES app.base (id)`
  matches it through the owning schema. So moving the key to `app.base`
  plans nothing.
- A key that references `app.base` reads back qualified. A desired bare
  `REFERENCES base (id)`, which apply resolves to `public.base`, matches it
  the same way.

Matching the catalog side through its recorded schema alone would close the
first case. But `pista diff` reads the current side from a file. In a file,
the schema that is recorded for a bare name is only the parser's first target
schema. So the diff would then plan a change for a reference that only
switched between the bare and the qualified spelling.

Workaround: qualify a reference to a table outside the owning table's schema.
That avoids the second case. Drop and re-add by hand a key that moved between
same-named tables.

Origin: [#706](https://github.com/winebarrel/pistachio/pull/706).

## A sequence a column owns in the database plans an unusable CREATE

A desired standalone sequence can have the same name as a sequence that a
column owns in the database. The plan then holds a `CREATE SEQUENCE` for a
sequence that already exists, and apply fails with
`relation "..." already exists` (SQLSTATE 42P07). A plain `CREATE SEQUENCE s;`
does it. So does one followed by `ALTER SEQUENCE ... OWNED BY NONE`, which is
the way a file would detach the sequence: the parser reads `OWNED BY NONE` and
clears the owner, but nothing emits the detaching DDL.

`catalog.Sequences` drops every sequence that has an owner. So the current
side never sees it. A desired sequence with no owner is a managed standalone
object. So the diff reads the missing current entry as "not created yet". The
opposite direction is handled: `ALTER SEQUENCE ... OWNED BY <column>` marks
the sequence unmanaged on the desired side. That matches the catalog, so an
owned sequence no longer replans forever.

Closing this means two changes. The catalog must surface the sequences that
are merely owned, kept apart from the serial and identity ones that stay
column attributes. `catalog.ListColumnsByTables` already draws that
distinction by checking that the column default draws from the sequence. The
diff must emit `ALTER SEQUENCE ... OWNED BY` / `OWNED BY NONE` for the
transitions. That widens the set of objects that pistachio manages, so it is a
feature rather than a fix. Workaround: detach the sequence by hand.

Origin: bug audit, 2026-07-31.

## `COMMENT ON COLUMN` on an inherited column of an INHERITS child

Priority: low.

A comment on a column that an `INHERITS` child inherits from its parent is
dropped at parse time. `parseCommentStmt` needs the column to be present on
the table, and such a child declares only its own columns. A true partition
child takes a different path. It declares no columns at all, so the comment
creates the entry. `diffTable` reaches that entry through a branch that never
diffs columns. An INHERITS child goes through the regular column diff. There,
an entry that holds only a name and a comment would be read as a new column
and would emit `ADD COLUMN`. So the same trick does not carry over.

The catalog reads only the local columns of such a child. So the two sides
agree and nothing drifts. The comment is unmanaged rather than lost to a diff:
`dump` writes none. `pg_dump` writes it, and that line is dropped on the way
in. `test/fidelity/schemas/inherits.sql` notes what it leaves out.

Closing this needs the inherited column set materialized on the child. That
set must be kept apart from the local one, so that the column diff still sees
only what the child declares.

Origin: review of [#340](https://github.com/winebarrel/pistachio/pull/340).

## Redeclaring a column on an existing INHERITS child cannot be planned

Priority: low.

`attislocal` separates a column that the child declares from one that it only
inherits. A desired schema can only say "declared". So a flip in either
direction has no DDL that the diff can emit, and the plan emits the column
statement instead. Both shapes fail at apply and come back on every later
plan. This was verified on 15 and 18.

With `parent (id, n)` and a child created as `child (extra) INHERITS (parent)`,
a desired schema that redeclares the column:

```sql
CREATE TABLE public.child (n integer NOT NULL, extra text) INHERITS (public.parent);
```

plans `ALTER TABLE public.child ADD COLUMN n integer NOT NULL`. That fails with
`column "n" of relation "child" already exists`. The reverse holds too. A child
that redeclared `n` in the database while the desired schema omits it plans
`ALTER TABLE public.child DROP COLUMN n` under `--allow-drop column`. That
fails with `cannot drop inherited column "n"`. `ADD COLUMN` and `DROP COLUMN`
neither merge with nor split off an inherited column. Only `CREATE TABLE` and
a parent-side `ADD COLUMN` do.

A dump writes a redeclared column as declared. So a dump fed back plans clean,
and only a hand-written schema reaches this. Closing it needs the inherited
column set materialized on the child, kept apart from the local one. That is
the same prerequisite as the entry above. Erroring at plan time may fit better
than emitting DDL that cannot work.

Workaround: redeclare a column when the child is created, or leave it alone.

Origin: review of [#510](https://github.com/winebarrel/pistachio/pull/510).

## A child of more than one parent keeps only the first

Priority: low.

`model.Table.PartitionOf` holds one parent. So `catalog.ListTables` reads the
one at `pg_inherits.inhseqno = 1`, and the parser reads `cs.InhRelations[0]`.
Both drop the rest without a warning. `CREATE TABLE c (z integer) INHERITS (a,
b)` is dumped as `INHERITS (public.a)`. Reloading that gives a table without
`b` or its columns.

Both sides read the first parent alone. So a dump fed back plans clean. Only a
reload loses anything.

Closing it means a list rather than a single name. The parser, the dependency
graph and the diff all read that list. A declarative partition has exactly one
parent, so the field would carry a list for the INHERITS case alone.
`test/fidelity/schemas/inherits.sql` notes what it leaves out.

Origin: INHERITS local column support.

## A new partition's numbered copy of its parent's index

`CREATE TABLE ... PARTITION OF` copies the parent's indexes. The plan leaves
out a `CREATE INDEX` for a copy that the file writes. The plan recognizes the
copy by the name that PostgreSQL gives it. When the copies of two of the
parent's indexes would get the same name on the partition, PostgreSQL numbers
the second copy:

```sql
CREATE INDEX logs_at_idx ON public.logs USING btree (at);
CREATE INDEX logs_at_pos_idx ON public.logs USING btree (at) WHERE (id > 0);
```

A partition gets `logs_2026_at_idx` and `logs_2026_at_idx1`. The plan does not
predict the number. So a new partition that is written with
`logs_2026_at_idx1`, as `dump` writes it, still gets that `CREATE INDEX`. That
statement fails with `relation "logs_2026_at_idx1" already exists`.

Workaround: leave the copy out of the file.

Origin: review of [#866](https://github.com/winebarrel/pistachio/pull/866).

## Perpetual drift on a typed literal the catalog re-prints

Priority: low.

A constant is stored as the value that its type's input function produced. It
is not stored as the text that produced it. The catalog prints the value back
in the type's output form. A literal whose text differs from that form never
compares equal. This holds whether or not the literal names the type.
`CHECK (a > timestamp '2000-01-01')` and, on a `timestamp` column,
`CHECK (a > '2000-01-01')` both come back from `pg_get_constraintdef` as
`CHECK ((a > '2000-01-01 00:00:00'::timestamp without time zone))`. So the
`CHECK` is dropped and re-added on every plan, and the whole table is
revalidated. `timestamp '2000-01-01 00:00:00'` plans clean. An index
predicate, a view body, a policy, a trigger `WHEN` and a domain `CHECK` drift
the same way. A generated column fails the run, because it cannot be altered
in place.

`a AT TIME ZONE 'UTC' > '2000-01-01'` is the same case. The right operand
resolves to `timestamp without time zone`, and the catalog re-prints it in that
type's output form.

`diff/desugar.go` undoes syntactic rewrites. Matching a literal is different.
It means running the type's input and output functions over the literal, and
that would put a query to the server in the middle of the comparison.

`dump` writes the catalog form. So a dump fed back plans clean, and only a
hand-written literal reaches this.

Workaround: write the literal in the type's output form, the way
`pg_get_constraintdef` prints it.

Origin: expression normalization review, 2026-08-30.

## Perpetual drift on an `IN` list PostgreSQL expands

Priority: low.

A list reaches the catalog as `= ANY (ARRAY[...])` only when no item contains
a column reference and the left operand is not a row. `diff/desugar.go` folds
that form back. Two shapes are expanded into comparisons instead
(`transformAExprIn`, `src/backend/parser/parse_expr.c`):

- A row on the left. `(a, b) IN ((1, 2))` is stored as `(a = 1) AND (b = 2)`.
  That is one comparison per column, joined with AND. More than one row ORs
  those groups together: `(a, b) IN ((1, 2), (3, 4))` becomes
  `((a = 1) AND (b = 2)) OR ((a = 3) AND (b = 4))`.
- An item that contains a column reference. `a IN (b, c)` is stored as
  `(a = b) OR (a = c)`, and `a NOT IN (b, c)` as `(a <> b) AND (a <> c)`. A
  mixed list splits. The items that contain no column reference keep the
  array form: `a IN (b, 1, 2)` becomes `(a = ANY (ARRAY[1, 2])) OR (a = b)`.
  The test is the column reference, not the constant. So a call over literals
  stays whole: `a IN (length('xx'), length('yyy'))` keeps the array form.

In both cases the written list never matches what comes back. So its `CHECK`
is dropped and added again on every plan, and the whole table is revalidated.
An index predicate, a view body, a policy and a trigger `WHEN` drift the same
way. A generated column fails the run, because it cannot be altered in place.
A one-element list of a single column is the exception, for `IN` and `NOT IN`
alike: `a IN (b)` is stored as `a = b`, and `foldSingleElementIn` produces
that form. A one-row list of a row, `(a, b) IN ((1, 2))`, still drifts.

Closing this means writing those expansions out, which is more than rewriting
an operator. The row form needs a comparison per column and an OR per row. The
other form needs to tell an item that contains a column reference from one
that does not, and it needs to print the two halves in the order PostgreSQL
uses.

`dump` writes the expanded form. So a dump fed back plans clean, and only a
hand-written list reaches this.

Workaround: write the comparisons out, `a = 1 AND b = 2` for the row and
`a = b OR a = c` for the other.

Origin: expression normalization review, 2026-09-20.

## Perpetual drift on an array literal written with a cast

Priority: low.

Parse analysis moves a cast on an array constructor onto the elements. It
drops a cast that the elements already carry. `ARRAY[1]::integer[]` is stored
as `ARRAY[1]`, `ARRAY[1]::bigint[]` as `ARRAY[(1)::bigint]`, and
`ARRAY['2020-01-01']::date[]` as `ARRAY['2020-01-01'::date]`. The written cast
sits on the array. So it never matches what comes back, and the `CHECK` that
holds it is dropped and added again on every plan. An index predicate and a
view body drift the same way. A text-like cast is the exception, because
`normalizeCheckExpr` strips `::text[]` and `::varchar[]` from both sides. That
is the form `pg_dump` writes for a `varchar` column.

Matching the rest means knowing what each element's type already is. That type
decides whether the cast moves or goes. The tree alone does not say.

`dump` writes the stored form. So a dump fed back plans clean, and only a
hand-written cast reaches this.

Workaround: write the cast on the elements, `ARRAY[1::bigint]`, or leave it
off where the elements already have the type.

Origin: expression normalization review, 2026-09-20.

## Some casts in a DEFAULT are not compared

Priority: low.

The catalog writes a literal with its type, and it drops a cast that changes
nothing. So a cast on a literal is ignored. A cast on an expression that only
the desired side has is ignored too. A cast in either place that changes the
value goes unnoticed:

```sql
-- database
CREATE TABLE t (n numeric DEFAULT 1.5, s text DEFAULT now());
-- desired
CREATE TABLE t (n numeric DEFAULT 1.5::integer, s text DEFAULT now()::date);
```

The plan reports no changes. Telling the two kinds of cast apart needs the
type of the expression under the cast. Only the database knows that type.

Origin: default cast review, 2026-09-24.

## Perpetual drift on a schema-qualified sequence in a column DEFAULT

Priority: low.

A column `DEFAULT nextval('public.counter'::regclass)` is re-emitted as
`ALTER COLUMN ... SET DEFAULT` on every plan. Applying it succeeds and changes
nothing. So `plan --check` stays at exit code 2 forever. `pg_get_expr` returns
the sequence unqualified when its schema is on the search_path. The desired
side keeps what the user wrote.

The call itself no longer drifts. `stripFuncSchema` (`diff/tables.go`) drops
the schema from a `FuncCall` name symmetrically, and that reaches every site
that `normalizeCheckExpr` covers. The sequence here is not in that name. It
sits inside a string literal argument. So reaching it means reading the
regclass literal, quoted identifiers included. Beyond the walk, `equalDefault`
drops a schema only from the type of a top-level cast, never from a literal.

A function moved between two schemas is the cost of the symmetric strip.
`a.f(v)` and `b.f(v)` compare equal, so the move produces no diff. A view
body's table reference already carries this tradeoff. An exclusion element's
`OPERATOR(a.=)` carries it too since #507, and so does the type of a cast on
an expression in a DEFAULT. Telling them apart means the search_path-aware
stripping that the cross-schema user-type entry above describes. The diff
cannot do that today, because it does not thread the schema list.

Workaround: write the sequence unqualified.

Origin: found while adding `-- pista:execute-first`, 2026-07-31. The function
call half was closed later; the literal half is what remains.

## Perpetual drift on a view defined with `SELECT *`

Priority: low. The consequence is heavier than in the other entries of this
kind. With the view drop allowed, every apply drops and recreates the view.

A view whose desired definition uses `SELECT *` or `t.*` is re-emitted on
every plan. `pg_get_viewdef` returns the star expanded into an explicit column
list. The desired side keeps the star. So `equalViewDef` (`diff/views.go`)
never matches. For the same reason, `canCreateOrReplaceView` reports the
output columns as undeterminable, and that routes the change through
`DROP`+`CREATE`. Under the default drop policy the plan prints
`-- skipped: DROP VIEW ...` and then `-- No changes`. With the view drop
allowed, every apply drops and recreates the view, and takes its privileges
and dependent views with it. This applies to plain views, `t.*`, and a star
over a subquery alike.

Expanding the star on the desired side needs the column list of every FROM
item. `DiffViews` receives only the view maps, so the table columns would
have to be threaded in. `model.View` carries a definition rather than
columns, so a star over another view has to be resolved recursively from
that definition. `equalViewDef` already strips table qualification. So the
expansion only has to produce the column names in the right order, not the
qualified form that the catalog prints.

What the star should mean is a separate decision. PostgreSQL expands it at
`CREATE` time and freezes the result. So adding a column to a base table
leaves the view alone. Expanding against the desired tables makes a column
addition re-emit `CREATE OR REPLACE VIEW` for every star view over that
table. Expanding against the catalog matches PostgreSQL, but it never
propagates the new column.

Faithful expansion also has to reproduce PostgreSQL's name resolution.
`JOIN ... USING` and `NATURAL JOIN` yield the join column once.
`FROM (...) s(a, b)` renames the subquery's columns through the alias.
Function FROM items and `LATERAL` cannot be resolved from the desired schema
at all.

Workaround: write the column list explicitly. `pista dump` emits the
expanded form.

Origin: view definition comparison review, 2026-08-06.

## SQL/JSON forms that still drift

Priority: low. A dump feeds back clean. So writing the file the way
`pista dump` emits it avoids all of this.

A SQL/JSON expression is compared as written, and the server rewrites most of
what it is handed. So a file written any other way re-emits its view or
constraint on every plan. Each form below was observed on every server that
has the syntax:

- The path is stored in canonical form. `'$.x'` comes back as `'$."x"'`,
  `'$.x ? (@ > 1)'` as `'$."x"?(@ > 1)'` and `'$.x + 1'` as `'($."x" + 1)'`.
  This holds on every supported server, because the jsonpath type predates 15.
- `JSON_OBJECT`, `JSON_ARRAY`, `JSON_OBJECTAGG` and `JSON_ARRAYAGG` resolve
  `RETURNING json` or `RETURNING jsonb` from their argument types, and
  `pg_get_viewdef` prints the one that the server picked. 16 and later.
- The query functions resolve a `RETURNING` type too: text for `JSON_VALUE`
  and `JSON_SERIALIZE`, and jsonb for `JSON_QUERY`. `JSON_QUERY` also prints
  its wrapper and quote options explicitly, even when they use their
  defaults. So a file that leaves them off differs from
  `WITHOUT WRAPPER KEEP QUOTES`. 17 and later.
- `JSON_TABLE` is a FROM item rather than an expression. The server adds
  a `LATERAL` and names the row pattern: `JSON_TABLE(t.a, '$.x' COLUMNS (k
  text PATH '$.k'))` is stored as `LATERAL JSON_TABLE(t.a, '$."x"' AS
  json_table_path_0 COLUMNS (k text PATH '$."k"'))`. 17 and later.

Closing any of this means reproducing what the server resolved. The path needs
a jsonpath expression parser, because pg_query hands the path over as a plain
string constant. The `RETURNING` type needs type inference over the
arguments, and pg_query does not do that.

Origin: [#371](https://github.com/winebarrel/pistachio/pull/371).

## View drift on a target alias the catalog writes differently

Priority: low.

PostgreSQL names a sub-select's output column on its own, and `pg_get_viewdef`
prints the name it chose. So a view whose body holds
`COALESCE((SELECT max(s.n) FROM t s WHERE s.a = t.a), 0)` comes back with
`SELECT max(s.n) AS max` inside. The written form carries no alias there. The
two `ResTarget` names disagree, and `CREATE OR REPLACE VIEW` is re-emitted on
every plan. Reproducing the name means reimplementing `FigureColname`. That
function walks the expression to pick a function name, a column name or
`?column?`. `figureIndexColname` (`parser/parser.go`) already walks an
expression this way for an index element. So the shared half of the work is
in the tree.

Workaround: write the alias the way `pista dump` emits it.

Origin: [#371](https://github.com/winebarrel/pistachio/pull/371).

## View drift on a constant a set operation resolves to another type

Priority: low.

A bare string or `NULL` constant on a view's `SELECT` target resolves to
`text`. `pg_get_viewdef` prints `'x'::text` for it, and the diff treats that
as the bare constant. In a `UNION`, `INTERSECT` or `EXCEPT` the constant is
resolved against the other branches instead. `SELECT name::varchar FROM t UNION
SELECT 'x'` comes back with `'x'::character varying`, and `SELECT 1 UNION SELECT
NULL` with `NULL::integer`. Those casts are compared. So every plan drops and
re-creates the view, and under the default drop policy it prints
`-- skipped: DROP VIEW` and leaves the view as it was. Accepting the casts
would need the type of the leftmost branch's column, and the diff does not
have that type. For the same reason, one change is not seen: removing an
explicit `::text` from a branch that comes before a `varchar` one. That change
turns the output type from `text` into `character varying`.

Workaround: write the constant the way `pista dump` emits it.

Origin: [#710](https://github.com/winebarrel/pistachio/pull/710).

## A view change to a qualifier alone is not seen

`pg_get_viewdef` and a written view body qualify references differently. So
the view comparison drops the schema from every table reference and the table
from every `table.column` on both sides. A change to a qualifier alone plans
no change. An example is `o.created_at` to `u.created_at` in a join. Closing
this means dropping a qualifier only where one side leaves it out.

Workaround: run the `CREATE OR REPLACE VIEW` by hand.

Origin: bug survey, 2026-09-27.

## Routine renaming is not supported

`-- pista:renamed-from` works on tables, views, enums, enum values, domains,
composite types, composite attributes, sequences, columns, constraints, foreign
keys, indexes, policies and triggers. It does not work on a function or a
procedure. The directive on a `CREATE FUNCTION` or `CREATE PROCEDURE` is an
error rather than an `ALTER FUNCTION ... RENAME TO`.

Dropping and recreating a routine loses no data. So a rename would mostly keep
the plan readable rather than protect anything. The identity carries the
argument types. So the directive would take a full signature
(`-- pista:renamed-from public.old_name(integer)`).

Origin: routine support.

## Routine create order ignores what the body reads

Routines are created after the types that their signature names, and before
every table. The reason is that a `CHECK` constraint, a `GENERATED` expression,
an index expression, a policy or a trigger can call a routine. The edge from a
table to a routine is drawn wholesale in `addRoutineDeps`, not by reading the
expressions. The answer is the same "routine first" either way.

A routine whose signature names a table, as `RETURNS SETOF <table>` does,
follows that table instead. A table that the routine reaches through its
signature gets no edge to the routine, because that edge would close a cycle.
When two routines name two different tables, one of the routines comes after
both tables. A `CHECK`, `GENERATED`, index or policy expression on the other
table that calls that routine then fails to apply.

A routine whose signature names a view cannot be created in the same run as
the view. The plan creates every view after every routine, whatever the
dependency sort says.

The reverse direction is not modeled at all. A `LANGUAGE sql` routine whose
body reads a table that is created in the same run fails to apply, because
PostgreSQL parses a SQL body at creation time. A `LANGUAGE sql` routine that
calls another routine defined later in the file fails for the same reason. A
`plpgsql` routine fails too when its `DECLARE` uses a table's row type or
`%TYPE`. On older servers, a missing table in `%TYPE` shows up as a syntax
error.

Turning the check off works around both cases:

```sh
pista apply --pre-sql 'SET check_function_bodies = off' schema.sql
```

Errors in a body then show up when the routine is called. Types in the
signature are still checked.

One fix is to apply with the check off and to run each routine's validator at
the end. The other fix is to read the body and to order the routine on what the
body names. That fix puts both directions in one graph. So the wholesale edge
would have to go, and every CHECK, GENERATED, index, policy and trigger
expression would have to be read.

Origin: routine support.

## A bare builtin type name resolves to a managed object of that name

`toposort.resolveTypeDep` models the search path as the object's own schema
and then `public`. PostgreSQL puts `pg_catalog` ahead of both. So a bare
`text`, `json` or `date` in a column type or in a domain's base type means the
builtin type, whatever else carries the name. pistachio can look only in the
objects that it manages. So a table, view or sequence that is named after a
builtin takes the edge instead:

```sql
CREATE DOMAIN public.d1 AS text;
CREATE TABLE public.text (v public.d1);
```

The domain takes an edge to the table, and the table takes an edge to the
domain. The pair closes a cycle. The whole plan, not just the pair, then falls
back to ordering by category. That breaks creates elsewhere that need the
dependency order. For example, put `CREATE TABLE public.x`, a composite type
with an attribute of type `public.x` and a function that returns
`SETOF public.x` next to the pair above. The three are planned with the table
last, and apply fails with `type "public.x" does not exist`. It takes both
references to close the cycle. The table alone does nothing: its own `text`
column resolves to the table itself and is skipped. One other object that is
written `text`, such as another table's column or a domain's base type, takes
the spurious edge and orders the table first. That changes nothing else.

Closing this needs the set of builtin type names in the resolver, so that a
bare name in the set resolves to nothing. That set is about a hundred names,
one per `pg_catalog` type, and a new PostgreSQL release adds to it. pg_query
cannot stand in for the set. It qualifies the spellings that its grammar maps,
so `json` parses to `pg_catalog.json`, but it leaves `text` bare. `dump` has no
parse tree to read at all. An object that is named after a builtin is rare
enough that the list has not been worth carrying.

Resolving a type position among enums, domains and composite types alone also
closes the cycle, with no list to keep. The cost is the edge that a column
typed with another table's row type takes today. That case is rarer still than
the collision. So the two are worth weighing together whenever this is taken
up.

Workaround: do not name a relation after a builtin type. Qualifying the type
in the schema file does not help. The catalog reports the type bare, so the
two spellings would drift on every run.

Origin: review of [#676](https://github.com/winebarrel/pistachio/pull/676).

## Schema mapping does not rewrite a routine body

`-m old=new` rewrites the schema of a routine and the type names in its
signature, including a parameter default. It does not touch the body. The body
is opaque text in whatever language the routine is written in. A body that
qualifies a table or a function with the old schema keeps the old name.

A view definition gets the same replacer, because a view definition is always
SQL. A routine body is not always SQL, so substituting a prefix in it would be
a guess.

Priority: low.

Origin: routine support.

## A SQL-standard routine body is created after every table

A routine that is written as `BEGIN ATOMIC ... END` or `RETURN <expr>` is
created with the views, after every table. A `CHECK` constraint, column
default, generated column, index expression or trigger `WHEN` condition that
calls such a routine fails to apply when both are created in the same run.
Policies are created last, so they are not affected.

Fixing this needs the body-dependency work in "Routine create order ignores
what the body reads".

An overload set that mixes a string body and a SQL-standard body shares one
node in the dependency sort. It is ordered as a string body.

Priority: low.

Origin: BEGIN ATOMIC support.

## A SQL-standard routine body is compared as text

PostgreSQL stores the body as a parse tree. `pg_get_functiondef` deparses it
with names qualified, so `SELECT a FROM t` comes back as `SELECT t.a FROM t`.
The body is compared without normalization. So a body that is written
differently from what `dump` writes is replaced on every plan. Views avoid the
same drift with `stripQualifications`.

Priority: low. Writing the body as `dump` writes it avoids the drift.

Origin: BEGIN ATOMIC support.

## Aggregates and window functions are not managed

`prokind` `'a'` and `'w'` are filtered out of the catalog query. The parser
warns about a `CREATE FUNCTION ... WINDOW` and drops it. So the two sides stay
symmetric. `CREATE AGGREGATE` has a shape that `model.Routine` does not cover.

Origin: routine support.

## Rules are not managed

`CREATE RULE` in a schema file is skipped with the unsupported-statement
warning. `dump` does not write a rule, and `plan` does not drop a rule that the
database holds. A database restored from `pista dump` loses its rules.

Workaround: write the rule as `CREATE OR REPLACE RULE` under
`-- pista:execute`. The statement runs on every apply, and a plain
`CREATE RULE` fails the second time.

Origin: pg_dump fidelity comparison of the sample databases, 2026-09-23.

## An identity column's sequence name is not managed

Priority: low.

The sequence behind an identity column is created as `<table>_<column>_seq`.
`pista dump` never writes the `SEQUENCE NAME` that `pg_dump` writes. So a
sequence that was renamed by hand restores under the default name. The options
that the sequence carries are managed; only the name is not.

Reading the name is one more column in the identity read, and `CREATE TABLE`
takes `SEQUENCE NAME` inside the identity options. So `dump` is easy. The diff
is not easy. Changing the name on an existing column is
`ALTER SEQUENCE ... RENAME TO`. pistachio takes a rename from a
`-- pista:renamed-from` directive rather than inferring one. So a directive
would have to reach a column's sequence.

Managing the name also means that `dump` writes `SEQUENCE NAME` on every
identity column. There is no plan to close this.

Origin: identity sequence options, 2026-09-02.

## A `nextval` default is dropped from a column a sequence cannot type

A column is read as a serial when it owns a sequence and its default draws from
that sequence. The type is not part of the test, but the rendering depends on
the type. The type name reads back as `serial` only for `integer`, `bigint` and
`smallint`, and the default is dropped either way. Therefore a column of any
other type that owns its sequence loses its default:

```sql
CREATE TABLE public.t (id serial);
ALTER TABLE public.t ALTER COLUMN id SET DATA TYPE text;
```

`pista dump` writes `id text NOT NULL`, though the column still carries
`nextval('t_id_seq'::regclass)`. The plan of that dump reports no changes. So
nothing says that the default was lost. Loading the dump gives a table without
the default.

The same happens to a column whose sequence was attached by hand. There the
sequence goes too, because an owned sequence is left to the column that owns
it:

```sql
CREATE SEQUENCE public.s;
CREATE TABLE public.t (id numeric DEFAULT nextval('public.s'));
ALTER SEQUENCE public.s OWNED BY public.t.id;
```

`pista dump` writes `id numeric` and no `CREATE SEQUENCE`.

Closing this means gating the serial test on the type. Then a column that a
sequence cannot type keeps its default, and its sequence surfaces as a
standalone sequence.

Origin: review of the serial retype fix, 2026-09-08.

## A sequence a column owns loses its name in `dump`

Priority: low.

A column that owns a sequence and draws its default from it is read as a
serial, whatever the sequence is called:

```sql
CREATE SEQUENCE public.custom_user_id_seq;
CREATE TABLE public.users (id bigint DEFAULT nextval('custom_user_id_seq') NOT NULL);
ALTER SEQUENCE public.custom_user_id_seq OWNED BY public.users.id;
```

`pista dump` writes `id bigserial NOT NULL` and no `CREATE SEQUENCE`. The plan
of that dump is clean. But loading the dump into an empty database creates
`users_id_seq`, and the options that were set on the sequence go back to their
defaults. Use `pg_dump -s` to copy such a database.

Closing this means reading a column as a serial only when its sequence has the
name and the options that serial gives it. Any other column would be written
as the sequence, the `nextval` default and the `OWNED BY`. The plan of that
dump also needs a desired `OWNED BY` to keep the sequence managed, which it
does not do today.

Origin: sequence ownership review, 2026-09-23.

## Perpetual drift on an array written with dimensions or a bound

Priority: low.

PostgreSQL accepts `integer[3]` and `integer[][]` for SQL standard
compatibility, and it enforces neither. A column that is declared `integer[3]`
takes an array of any length. A column that is declared `integer[][]` takes an
array of any number of dimensions. `format_type` prints both as `integer[]`.
The desired side keeps what the file wrote. So the two never compare equal,
and the column is retyped on every plan. The statement is harmless to the
data, and it takes an ACCESS EXCLUSIVE lock. `plan --check` stays non-zero.

Folding every `[...]` to a single `[]` closes this. But the spelling means
nothing to PostgreSQL either, so a schema that carries one has already lost
what it was trying to say. `dump` writes `integer[]`, and none of the sample
schemas declares a column this way.

Workaround: write `integer[]`, which is what `pista dump` emits.

Origin: review of the column type canonicalization, 2026-09-08.

## `PRIMARY KEY USING INDEX` does not mark its columns NOT NULL

Priority: low.

A primary key can take its columns from an existing unique index rather than
from a column list:

```sql
CREATE TABLE public.t (id integer);
CREATE UNIQUE INDEX t_idx ON public.t (id);
ALTER TABLE public.t ADD CONSTRAINT t_pkey PRIMARY KEY USING INDEX t_idx;
```

The constraint and the index converge as of 1.50.0. The index that the
constraint takes over is left where it is rather than planned as a drop. A
desired constraint that names an index compares equal to the constraint that
PostgreSQL created from that index. What the form still does not carry is the
NOT NULL that a primary key marks its columns with. There is no column list to
read it from. So the desired side has the column nullable where the database
has it not null, and every plan emits

```
ALTER TABLE public.t ALTER COLUMN id DROP NOT NULL;
```

That statement fails with `column "id" is in a primary key`. So the run does
not converge. This was verified on 15 and 18.

Closing this means resolving the constraint's columns through the index that
the desired schema declares next to the constraint. That index is where the
column list lives.

`pista dump` writes the key inline, as `CONSTRAINT t_pkey PRIMARY KEY (id)`,
with the column declared `NOT NULL` and no separate index. That output plans
clean. Only a file that writes `USING INDEX` reaches this.

Workaround: declare the key's columns `NOT NULL` in the file. That leaves
nothing for the plan to emit.

Origin: review of the primary key NOT NULL fix, 2026-09-09. Narrowed once
`USING INDEX` constraints were read in 1.50.0.

## A subscripted ARRAY constructor loses its parentheses

PostgreSQL needs parentheses to subscript an array constructor, and
`pg_get_expr` writes the expression back that way. A column that is declared
`DEFAULT (ARRAY[1,2,3])[1]` reads out of the catalog as
`(ARRAY[1, 2, 3])[1]`. The desired side runs the expression through
libpg_query's deparse. The deparse drops the pair and returns
`ARRAY[1, 2, 3][1]`. The two never compare equal. So the column is
redefaulted on every plan, and the statement that the plan emits is a syntax
error:

```
ALTER TABLE public.t ALTER COLUMN e SET DEFAULT (ARRAY[1, 2, 3][1]);
```

A `pista dump` round trip is broken, not only drifting. So this is not
`Priority: low`.

The deparse drops the pair only for an `A_ArrayExpr` under an
`A_Indirection`. `(c).x`, `(ROW(1,2)).f1`, `(f(x)).a`, `(a[1])[2]` and
`(x::int[])[1]` all keep their parentheses. Closing this means putting the
pair back around a constructor that the deparse subscripts, on each path that
renders an expression. A column default, a CHECK constraint, an index
expression and a policy qualifier reach the deparse separately.

An array constructor is a constant, so `(ARRAY[1,2,3])[1]` is a longer way
to write `1`. No sample schema declares one.

Origin: review of the `ARRAY[...]` layout fix, 2026-09-15.

## Perpetual drift on a default operator class or collation written out

Priority: low.

`pg_get_indexdef` and `pg_get_constraintdef` name an index element's operator
class only when it is not the default for the column's type. They name its
collation only when it is not the collation that the column carries. The
desired side keeps what the file wrote. So an index that is created as
`(id int4_ops)` never compares equal to the bare column that the catalog hands
back. The index is dropped and created on every plan, and an exclusion
constraint is dropped and added.

Deciding that a written class is the default means reading
`pg_opclass.opcdefault` for the column's type under the index's access method.
The diff does not thread that information, and an expression element has no
column type to look it up by. Matching the name against the classes that are
a default for some type answers a different question. `bpchar_ops` on a `text`
column is legal and is not that column's default, but it would fold away. The
collation needs the column's own collation. Two cases are folded: the collation
that the column declares with `COLLATE`, and `COLLATE "default"` on an index
column of type `text`, `varchar` or `char`, or an array of one of them, with
no collation of its own. Any other collation that is written out still drifts.
That includes the collation that a column takes from its type, such as `"C"`
on a `name` column or on a domain that declares it, and a collation on an
expression element, on a materialized view's index, or in an exclusion
constraint.

An element that carries operator class options is not affected. ruleutils
(`src/backend/utils/adt/ruleutils.c`) passes `InvalidOid` for the column type
there. So the class is written either way, and both sides name it.

Workaround: leave both out, which is what `pista dump` and `pg_dump` write.

Origin: review of the index element canonicalization, 2026-09-17.
