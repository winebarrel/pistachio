# Known limitations

What pistachio does not handle, and what closing each one would take. Each
entry notes where it came from.

An entry marked `Priority: low` is drift a `pista dump` output does not hit.
Writing the schema the way `dump` writes it avoids it.

## A rename is not carried into another object's reference

`-- pista:renamed-from` adjusts the current side so the change is planned as a
rename rather than a drop and a create. The adjustment reaches the renamed
object's own dependents alone: a column's indexes, constraints, foreign keys,
triggers, policies and generated expressions on the same table, and a table's
own indexes and foreign keys. Two references are left as they were:

- A view or materialized view definition that names the renamed table or
  selects the renamed column.
- A foreign key in another table whose `REFERENCES` names the renamed table or
  column.

PostgreSQL updates such a reference itself on RENAME, so the second run is
clean and only the first plan carries a redundant drop and create for the
dependent object. Closing it needs cross-object awareness in the diff phase.

A column rename does not reach a partition child either. `diffTable` takes a
separate branch for one, which returns before the rewrite block, and a
`PARTITION OF` child declares no columns, so its own rename map is empty. A
trigger or policy created directly on a child is in the model, since the
catalog excludes only the clones the parent pushes down, so a rename on the
parent leaves a redundant statement on such a child. Carrying the rename there
needs the parent's map.

Origin: [#123](https://github.com/winebarrel/pistachio/pull/123) for the column
half, a NOTE in `diff/rename.go:detectTableRenames` for the table half.

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

## Named NOT NULL constraints: name add/remove on existing columns

`Column.NotNullName` round-trips on PG18, and a name change between two
named NOT NULL constraints is emitted as `RENAME CONSTRAINT`. The
following transitions are no-ops in v1:

- nullable -> NOT NULL with an explicit desired name: emits `SET NOT NULL`
  (PG auto-generates a name) but does not apply the desired name.
- NOT NULL with explicit current name -> still NOT NULL but unnamed: keeps
  the current name in place.

Both require PG18's standalone `ALTER TABLE ... ADD CONSTRAINT name NOT NULL col`
syntax, which the parser cannot read: `pg_query_go` v6 embeds PostgreSQL 17.7
and has no 18 release. libpg_query, the C library under it, shipped 18 on
2026-05-21, so the Go binding is what is waited on. Once it lands, the parser
can accept the standalone form and the diff can drop the no-op branches.

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

## Amazon Aurora DSQL is not supported

A DSQL-targeted schema never holds what DSQL cannot create, so what stands in
the way is narrower than the feature list: the connection sends a parameter
DSQL rejects, `CREATE INDEX` carries an access method it refuses and a build
that has to be awaited, a primary key reads back with columns pistachio did
not write, and several transitions have a DSQL syntax pistachio does not emit.

Each of these was answered by a live cluster, and the answers are in
[DSQL.md](https://github.com/winebarrel/pistachio/blob/main/DSQL.md) rather
than here, since they are evidence for the work rather than the work itself.

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

Priority: low.

A comment on a column an `INHERITS` child inherits from its parent is dropped
at parse time. `parseCommentStmt` needs the column to be present on the
table, and such a child declares only its own columns. A true partition child
takes a different path: it declares no columns at all, so the comment creates
the entry, and `diffTable` reaches that entry through a branch that never
diffs columns. An INHERITS child goes through the regular column diff, where
an entry holding only a name and a comment would be read as a new column and
emit `ADD COLUMN`, so the same trick does not carry over.

The catalog reads such a child's local columns alone, so the two sides agree
and nothing drifts. The comment is unmanaged rather than lost to a diff:
`dump` writes none. `pg_dump` does, and that line is dropped on the way in.
`test/fidelity/schemas/inherits.sql` notes what it leaves out.

Closing this needs the inherited column set materialised on the child, kept
apart from the local one so the column diff still sees only what the child
declares.

Origin: review of [#340](https://github.com/winebarrel/pistachio/pull/340).

## Redeclaring a column on an existing INHERITS child cannot be planned

Priority: low.

`attislocal` separates a column the child declares from one it only inherits,
but a desired schema can only say "declared", so a flip in either direction has
no DDL the diff can emit and the plan emits the column statement instead. Both
shapes fail at apply and come back on every later plan, verified on 15.

With `parent (id, n)` and a child created as `child (extra) INHERITS (parent)`,
a desired schema that redeclares the column:

```sql
CREATE TABLE public.child (n integer NOT NULL, extra text) INHERITS (public.parent);
```

plans `ALTER TABLE public.child ADD COLUMN n integer NOT NULL`, which fails with
`column "n" of relation "child" already exists`. The reverse holds too: a child
that redeclared `n` in the database while the desired schema omits it plans
`ALTER TABLE public.child DROP COLUMN n` under `--allow-drop column`, which
fails with `cannot drop inherited column "n"`. `ADD COLUMN` and `DROP COLUMN`
neither merge with nor split off an inherited column; only `CREATE TABLE` and a
parent-side `ADD COLUMN` do.

A dump writes a redeclared column as declared, so a dump fed back plans clean
and only a hand-written schema reaches this. Closing it needs the inherited
column set materialised on the child, kept apart from the local one, which is
the same prerequisite as the entry above. Erroring at plan time may fit better
than emitting DDL that cannot work.

Workaround: redeclare a column when the child is created, or leave it alone.

Origin: review of [#510](https://github.com/winebarrel/pistachio/pull/510).

## A child of more than one parent keeps only the first

Priority: low.

`model.Table.PartitionOf` holds one parent, so `catalog.ListTables` reads the
one at `pg_inherits.inhseqno = 1` and the parser reads `cs.InhRelations[0]`,
dropping the rest without a warning. `CREATE TABLE c (z integer) INHERITS (a,
b)` is dumped as `INHERITS (public.a)`, and reloading that gives a table
without `b` or its columns.

Both sides read the first parent alone, so a dump fed back plans clean. Only a
reload loses anything.

Closing it means a list rather than a single name, which the parser, the
dependency graph and the diff all read. A declarative partition has exactly one
parent, so the field would carry a list for the INHERITS case alone.
`test/fidelity/schemas/inherits.sql` notes what it leaves out.

Origin: INHERITS local column support.

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
tradeoff a view body's table reference already carries, and an exclusion
element's `OPERATOR(a.=)` carries it too since #507. Telling them apart
means the search_path-aware stripping described in the cross-schema user-type
entry above, which the diff cannot do today because it does not thread the
schema list.

Workaround: write the sequence unqualified.

Origin: found while adding `-- pista:execute-first`, 2026-07-31. The function
call half was closed later; the literal half is what remains.

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

## A comment on a constraint, a trigger or a policy is not managed

Priority: low.

Comments are managed for tables, columns, views, materialized views, indexes,
sequences, enums, composite types, domains and routines. `model.Constraint`,
`model.Trigger` and `model.Policy` have no `Comment` field and no case in
`parseCommentStmt`, so a `COMMENT ON CONSTRAINT`, `COMMENT ON TRIGGER` or
`COMMENT ON POLICY` in the desired schema warns and is dropped. `pista dump`
writes none either, so a dump feeds back clean. `pg_dump` does write them, and
those lines are lost.

The work is the same shape the index comment took: a `Comment` field on the
model, a `pg_description` join in the catalog read, a case in the parser, and
emission from the diff and the create path, since an object that is dropped
and recreated loses its comment. Each of the three names its relation, so the parser finds the
owner without scanning. The index a `PRIMARY KEY`, `UNIQUE` or `EXCLUDE`
constraint owns carries its comment as the constraint's, which is why a
`COMMENT ON INDEX` naming one is dropped today.

Origin: discussion, 2026-08-25. Narrowed once index comments shipped.

## An identity column's sequence name is not managed

Priority: low.

The sequence behind an identity column is created as `<table>_<column>_seq` and
`pista dump` never writes the `SEQUENCE NAME` that `pg_dump` does, so a
sequence renamed by hand restores under the default name. The options the
sequence carries are managed; only the name is not.

Reading it is one more column in the identity read, and `CREATE TABLE` takes
`SEQUENCE NAME` inside the identity options, so `dump` is easy. The diff is
not: changing the name on an existing column is `ALTER SEQUENCE ... RENAME TO`,
and pistachio takes a rename from a `-- pista:renamed-from` directive rather
than inferring one, so a directive would have to reach a column's sequence.

Managing the name also means `dump` writes `SEQUENCE NAME` on every identity
column. No plan to close this.

Origin: identity sequence options, 2026-09-02.
