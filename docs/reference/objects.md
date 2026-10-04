# Supported objects

pistachio manages the objects below. Each section says what the parser reads from a schema file, what pistachio compares against the database, and what DDL a change becomes.

| Object | Read from | Opt-in | `--allow-drop` type | `--enable` / `--disable` type |
|---|---|---|---|---|
| [Tables](#tables) | `CREATE TABLE`, part of `ALTER TABLE` | | `table` | `table` |
| [Columns](#columns) | the column list of `CREATE TABLE` | | `column` | `table` |
| [Constraints](#constraints) | column and table constraints, `ALTER TABLE ... ADD CONSTRAINT` | | `constraint` | `table` |
| [Foreign keys](#foreign-keys) | `REFERENCES`, `FOREIGN KEY`, `ALTER TABLE ... ADD CONSTRAINT` | | `foreign_key` | `table` |
| [Indexes](#indexes) | `CREATE INDEX` | | `index` | `table` or `view` |
| [Views](#views) | `CREATE VIEW`, `CREATE MATERIALIZED VIEW` | | `view` | `view` |
| [Enum types](#enum-types) | `CREATE TYPE ... AS ENUM` | | `enum` | `enum` |
| [Domains](#domains) | `CREATE DOMAIN`, `ALTER DOMAIN ... ADD CONSTRAINT` | | `domain` | `domain` |
| [Composite types](#composite-types) | `CREATE TYPE ... AS (...)` | | `composite_type` | `composite_type` |
| [Sequences](#sequences) | `CREATE SEQUENCE`, `ALTER SEQUENCE ... OWNED BY` | | `sequence` | `sequence` |
| [Routines](#routines) | `CREATE FUNCTION`, `CREATE PROCEDURE` | `--manage-routine` | `routine` | `routine` |
| [Triggers](#triggers) | `CREATE TRIGGER`, `ALTER TABLE ... ENABLE/DISABLE TRIGGER` | | `trigger` | `table` or `view` |
| [Policies](#policies-and-row-level-security) | `CREATE POLICY`, `ALTER TABLE ... ROW LEVEL SECURITY` | | `policy` | `table` |
| [Comments](#comments) | `COMMENT ON` | | | the object's type |

`--enable` and `--disable` select object types. `--enable` keeps only the named types, and `--disable` leaves them out. A column, constraint, index, trigger or policy goes with the table it is on, so `table` covers it. An index on a materialized view and a trigger on a view go with the view, so `view` covers them. A comment goes with the object it is on. See [Filtering what is managed](../guides/filtering.md#by-object-type).

`--allow-drop` controls a pure removal: an object that the desired schema no longer contains. Without the object's type in `--allow-drop`, the plan writes the drop as a `-- skipped:` comment and nothing runs. The drop that is part of a constraint's or an index's definition change does not require `--allow-drop`. A recreate of a view, a routine or a trigger does. See [Controlling drops](../guides/drops.md).

To rename any of these objects, an enum value or a composite attribute, use the [`-- pista:renamed-from`](directives.md#-pistarenamed-from) directive. Routines cannot be renamed.

## Statements that are not read

pistachio reads only the statements that the sections below name. It ignores every other statement in a schema file with a warning on standard error:

```
pista: schema.sql:12:1: ignored unsupported statement: GRANT select ON public.users TO app
```

This includes:

- `SET`, `GRANT`, `CREATE EXTENSION`, `CREATE RULE` and `CREATE AGGREGATE`
- every `DROP`
- every `ALTER` that the sections below do not list

The desired state is what `CREATE` says. The same warning covers a part of a statement that pistachio does not read, such as the `ALTER TABLE ... ADD COLUMN` that a `pg_dump` file includes. The warning for `BEGIN` or `COMMIT` points to `--with-tx` and `--try-tx`.

To keep such a statement in the file and run it during `apply`, mark it with [`-- pista:execute`](directives.md#-pistaexecute). The directive also silences the warning.

A statement that names another object must come after that object's `CREATE`, in the same file or an earlier one. `ALTER TABLE`, `CREATE INDEX`, `CREATE TRIGGER` and `CREATE POLICY` are such statements. A statement that comes before the `CREATE` is an error:

```
pista: error: ALTER TABLE public.users: table public.users is not declared before it
 --> schema.sql:3:1
  |
3 | ALTER TABLE public.users ADD CONSTRAINT users_id_check CHECK (id > 0);
  | ^
```

Two objects that PostgreSQL keeps in one namespace cannot share a name in one schema. Two relations or two types are such a pair. A routine is identified by its name and argument types, so overloads are separate objects.

## Order of statements

A plan runs in this order. Within a group, pistachio sorts objects by their dependencies. A type comes before the table that uses it. A table comes before a view that reads it. A partition comes after its parent.

1. Foreign key drops.
2. View drops and `BEGIN ATOMIC` routine drops, dependents first.
3. Creates and alters: enum types, domains, composite types, sequences, routines, then tables. A table's statements include its constraints, indexes, triggers and row-level security. For a table that already exists, they also include its policies.
4. Indexes on partitioned tables.
5. `SET LOGGED` and `SET UNLOGGED`.
6. Drops, dependents first: tables, sequences, routines, composite types, domains, enum types.
7. Foreign key adds, renames, `ALTER CONSTRAINT` and `VALIDATE CONSTRAINT`.
8. View creates and `BEGIN ATOMIC` routine creates.
9. The policies of new tables.

## Tables

### What is read

The parser reads `CREATE TABLE` with these clauses:

- `UNLOGGED`
- `PARTITION BY`
- `PARTITION OF ... FOR VALUES`
- `INHERITS`
- `TABLESPACE`
- `WITH (...)`

`LIKE` is an error. List the columns instead. A table marked [`-- pista:ignore`](directives.md#-pistaignore) or a statement marked [`-- pista:execute`](directives.md#-pistaexecute) is not read, so it may use `LIKE`.

Of `ALTER TABLE`, the parser reads only these actions:

- `ADD CONSTRAINT`
- the row-level security toggles
- the trigger states
- `ALTER COLUMN ... SET STORAGE` / `SET COMPRESSION`

Any other action, `ADD COLUMN` included, is ignored with a warning. The desired columns are the ones that `CREATE TABLE` lists.

### Changes

A new table is one `CREATE TABLE`, followed by:

- its `NOT VALID` constraints
- its indexes
- `ENABLE` / `FORCE ROW LEVEL SECURITY`
- its triggers
- its comments

Its foreign keys and policies come at the end of the plan.

A removed table is `DROP TABLE`, which requires `--allow-drop table`. It comes after the `DROP CONSTRAINT` of each foreign key that the table has. pistachio does not check the drop for dependents. If a view reads the table and is not dropped, the apply fails.

Adding or removing `UNLOGGED` is emitted as `ALTER TABLE ... SET UNLOGGED` or `SET LOGGED`. The statement rewrites the table. PostgreSQL refuses a logged table that references an unlogged one. So pistachio orders the statements by the foreign keys between the tables that change. A partitioned table is skipped, because the statement changes nothing there.

A table's `TABLESPACE` is written on `CREATE TABLE`. It is not compared afterwards.

### Storage parameters

A table's storage parameters, the `WITH (...)` clause, are managed only with `--manage-storage-param`. They are off by default because the autovacuum settings for a table are usually set on the database, not written in the schema file. A materialized view has the same parameters, and the same flag controls them.

Without the flag, the parameters are removed from both sides of the diff. Nothing is planned. The clause that a schema file writes is left off the `CREATE`, and `dump` does not write one.

With the flag, the schema file states every parameter that the relation should have. A change is emitted as `ALTER TABLE ... SET (...)`, with a `RESET (...)` for the parameters that the file no longer names. Neither statement rewrites the relation. A `toast.` parameter belongs to the TOAST relation. PostgreSQL creates that relation only for a table with a toastable column, and discards the setting when there is none. So on such a table, the parameter appears in the plan on every run. A partitioned table has no parameter, and a partition does not inherit the parent's.

An index's parameters are part of its definition and are managed with or without the flag. The same is true of a plain view's `WITH (...)`, which contains only `security_barrier` and `security_invoker`. See [Views](#views).

### Partitions

A partition is declared as `CREATE TABLE ... PARTITION OF parent FOR VALUES ...` after its parent. Its columns and constraints come from the parent. They are neither written nor compared. On a partition that exists, pistachio compares:

- indexes
- foreign keys
- triggers
- policies
- row-level security
- comments
- storage parameters

It does not compare the partition's bound, its parent or the parent's partition key. Changing one of them produces no changes. PostgreSQL has no `ALTER` for a partition key. Moving a bound or a parent requires `DETACH` and `ATTACH`, which pistachio does not emit.

PostgreSQL clones a trigger on a partitioned table onto every partition. pistachio reads and writes only the parent's trigger, and PostgreSQL keeps the clones in step. A foreign key on the parent is copied the same way. See [Foreign keys](#foreign-keys). An index on the parent is created after every table statement. See [Indexes](#indexes).

`--skip-partition-child` leaves every partition out of both sides. It is for a schema whose partitions another tool creates. See [Skipping partition children](../guides/filtering.md#skipping-partition-children).

### Inheritance

pistachio manages an `INHERITS (...)` child as the columns and constraints that the child declares itself. That is what its `CREATE TABLE` writes, and what `pg_dump` writes for it.

```sql
CREATE TABLE public.shapes (id integer NOT NULL, name text);
CREATE TABLE public.boxes (w integer NOT NULL) INHERITS (public.shapes);
```

`boxes` is managed as `w` alone. `id` and `name` belong to `shapes`, and pistachio reads them from neither side. A change to them is emitted against the parent, and PostgreSQL passes it down to the child.

A column that the child redeclares is its own, and PostgreSQL merges it with the inherited one. That happens at `CREATE TABLE` only. No DDL can add the redeclaration to a child that already exists, or remove it. `ADD COLUMN` on an inherited column fails with `column ... already exists`, and `DROP COLUMN` fails with `cannot drop inherited column`. The plan emits one of the two anyway, because the desired side cannot tell a redeclared column from an inherited one. Redeclare a column when the child is created, or leave it alone.

A comment on an inherited column is not managed. `dump` writes none, and pistachio ignores one that a schema file includes, so nothing drifts. `pg_dump` does write it, and that line is lost.

pistachio reads only the first parent, so `dump` writes `INHERITS (a, b)` as `INHERITS (a)`. Both sides read it the same way, so a dump fed back produces no changes. But loading that dump into a database gives a table that does not inherit `b`.

`--skip-partition-child` does not apply to an `INHERITS` child. It applies to a declarative partition, which has a bound.

## Columns

### What is read

The parser reads the column definitions of `CREATE TABLE`:

- the type
- `COLLATE`
- `DEFAULT`
- `NOT NULL`, with an optional constraint name
- `GENERATED ... AS IDENTITY`, with its sequence options
- `GENERATED ALWAYS AS (...) STORED`
- `STORAGE` and `COMPRESSION`

It also reads `ALTER TABLE ... ALTER COLUMN ... SET STORAGE` / `SET COMPRESSION` after the table. That is how `pg_dump` writes them.

The parser reads a type in the form that the catalog reports. So an alias in the file compares equal to the name that the database has: `int` and `int4` are `integer`, `varchar` is `character varying`, and `timestamptz` is `timestamp with time zone`. `serial`, `bigserial` and `smallserial` are `integer`, `bigint` and `smallint` with a `nextval` default. A column written either way compares equal. A primary key column is `NOT NULL` whether or not the file says so. `COLLATE "default"` is what a column has implicitly, and the parser reads it as no collation. Any built-in type is accepted, arrays included. An identifier may be quoted.

### Changes

pistachio matches columns by name. It does not compare their order. A new column is appended.

| Change | DDL |
|---|---|
| Added | `ALTER TABLE ... ADD COLUMN`, with its default, identity, generated expression and `NOT NULL`, then `SET STORAGE` / `SET COMPRESSION` |
| Removed | `ALTER TABLE ... DROP COLUMN`, requires `--allow-drop column` |
| Type or collation | `ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE` |
| Default | `SET DEFAULT` or `DROP DEFAULT` |
| `NOT NULL` | `SET NOT NULL` or `DROP NOT NULL` |
| Identity | `ADD GENERATED ... AS IDENTITY`, `DROP IDENTITY`, or `SET GENERATED` with the sequence options that changed |
| Storage or compression | `SET STORAGE` or `SET COMPRESSION` |
| Generated expression | The change is an error. See below. |

A type or collation change is the first statement for the column. PostgreSQL refuses it while one of these depends on the column:

- a view or materialized view
- a rule
- a trigger
- a policy
- a `BEGIN ATOMIC` routine
- a generated column

`plan` fails first and names the dependents:

```
pista: error: cannot change the type of public.t.n: view public.v depends on it
```

A trigger blocks the change even when the column is only in its `UPDATE OF` list. The change also applies to the table's partitions and `INHERITS` children, so a dependent on their copy of the column blocks it too. A view or a `BEGIN ATOMIC` routine that the same plan drops does not block it. A trigger, a policy or a generated column that the plan drops still blocks it, because those drops run after the type change. Indexes and constraints do not block it, because PostgreSQL rebuilds them. `SET DATA TYPE` resets storage and compression. So when the file names them, both are written again after it.

`DROP NOT NULL` runs after the table's constraint statements, so a primary key dropped in the same plan comes first. `DROP COLUMN` runs after the table's constraint, index, trigger, policy and comment statements.

An identity column's sequence options, the `( ... )` after `AS IDENTITY`, are managed. pistachio plans no `RESTART`. So a change that puts the current value outside the new range fails at apply with the server's error.

A column that changes from `serial` to identity gets a new sequence from `ADD IDENTITY`. pistachio drops the old sequence unless the desired schema declares it. It does the same for a `serial` column given another default. The drop requires `--allow-drop sequence`.

A column cannot become generated or plain in place, and a generated column's expression cannot change in place. Either is an error at plan time:

```
pista: error: column public.t.total: cannot change GENERATED expression; DROP COLUMN + ADD COLUMN is required
```

A column that names no `STORAGE` or `COMPRESSION` asks for the default. So a column that the database has at `EXTERNAL` is planned back to its type's own strategy. Neither statement rewrites the table.

pistachio compares a `DEFAULT` as an expression, not as text. So a cast that the catalog adds is not a change, and neither is an `IN` list that the catalog stores as `= ANY (ARRAY[...])`. A rename with `-- pista:renamed-from` rewrites the column's references in the same table's indexes, constraints, foreign keys, triggers, policies and generated expressions. So one `RENAME COLUMN` is emitted. See [Renaming objects](../guides/renaming.md).

## Constraints

This section covers primary keys, unique constraints, check constraints and exclusion constraints. Foreign keys have a [section of their own](#foreign-keys).

### What is read

The parser reads a constraint written on a column, in the table's constraint list, or as `ALTER TABLE ... ADD CONSTRAINT` after the table. `NOT VALID` is read too. A `DEFERRABLE` or `INITIALLY DEFERRED` on a column constraint belongs to the constraint. A constraint without a name gets the name that PostgreSQL would give it. See [Names](#names).

### Changes

PostgreSQL has no `ALTER` for a constraint's definition, so a change to one is `DROP CONSTRAINT` and `ADD CONSTRAINT`. That pair does not require `--allow-drop`. A pure removal requires `--allow-drop constraint`. pistachio compares a definition as an expression. So a cast that the catalog adds, or an `IN` list that it rewrites, is not a change.

| Change | DDL |
|---|---|
| Added | `ALTER TABLE ... ADD CONSTRAINT`, with `NOT VALID` when the file writes it |
| Removed | `ALTER TABLE ... DROP CONSTRAINT`, requires `--allow-drop constraint` |
| Definition | `DROP CONSTRAINT` and `ADD CONSTRAINT` |
| `NOT VALID` to validated | `ALTER TABLE ... VALIDATE CONSTRAINT` |
| Validated to `NOT VALID` | `DROP CONSTRAINT` and `ADD CONSTRAINT ... NOT VALID` |
| Deferral clause | `DROP CONSTRAINT` and `ADD CONSTRAINT`. Only a foreign key uses `ALTER CONSTRAINT`. |
| Renamed | `ALTER TABLE ... RENAME CONSTRAINT` |

The drop and add rebuild a key's index. PostgreSQL refuses the drop while a foreign key references the key, or while a view groups by the primary key. `plan` fails first and names them:

```
pista: error: cannot drop constraint t_pkey on public.t: foreign key r_a_fkey on public.r depends on it
```

A foreign key or a view that the same plan drops does not block the drop. To change a referenced key, change the foreign key in the same run, or drop it and add it back in a later run.

`--assume-validated` treats every constraint as validated on both sides, so pistachio writes no `NOT VALID` and no `VALIDATE CONSTRAINT`. Without the flag, a validated constraint that the desired schema writes as `NOT VALID` is dropped and added back, because nothing removes the validated flag.

A `NOT VALID` check on a new table is added after the `CREATE TABLE`, as `ALTER TABLE ONLY ... ADD CONSTRAINT ... NOT VALID`. On a partitioned table, the statement has no `ONLY`. A constraint added to a table that exists has no `ONLY` either.

## Foreign keys

### What is read

The parser reads `REFERENCES` on a column, `FOREIGN KEY` in the table's constraint list, and `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY` after the table. `NOT VALID` is read too. A `REFERENCES` that names no columns refers to the referenced table's primary key, when that table is declared. A referenced table written without a schema is in the default schema.

### Changes

pistachio drops foreign keys at the start of a plan and adds them at the end. So a table can be created, changed or dropped between the two.

| Change | DDL |
|---|---|
| Added | `ALTER TABLE ONLY ... ADD CONSTRAINT ... FOREIGN KEY`, with `NOT VALID` when the file writes it |
| Removed | `ALTER TABLE ... DROP CONSTRAINT`, requires `--allow-drop foreign_key` |
| Definition | `DROP CONSTRAINT` and `ADD CONSTRAINT` |
| Deferral clause alone | `ALTER TABLE ... ALTER CONSTRAINT ... DEFERRABLE INITIALLY DEFERRED` |
| `NOT VALID` to validated | `ALTER TABLE ... VALIDATE CONSTRAINT` |
| Renamed | `ALTER TABLE ... RENAME CONSTRAINT` |

`ALTER CONSTRAINT` rewrites a catalog row. It does not rescan the table. A key that is also being validated gets a `VALIDATE CONSTRAINT` next to it. A foreign key dropped because its table is dropped follows `--allow-drop table`, not `--allow-drop foreign_key`.

A foreign key on a partitioned table is added without `ONLY`, because PostgreSQL rejects `ONLY` there. Nothing inherits a foreign key, so on a plain table the word changes nothing either. pistachio keeps it there because that is what `pg_dump` writes.

A partition has a copy of every foreign key that its parent declares. The copy gets no statement of its own. The parent's `DROP` removes the copy, its `ADD` creates a new copy, and its `ALTER CONSTRAINT` and `VALIDATE CONSTRAINT` recurse into the copy. `dump` leaves the copy out for the same reason, as `pg_dump` does. A rename is the one statement that does not reach the copy. The copy keeps its old name, while a partition attached later gets the new name. The copy is neither written nor compared, so nothing drifts. A key that the partition declares itself is managed like any other.

## Indexes

### What is read

The parser reads `CREATE INDEX` after the table or materialized view that the index is on. It reads:

- unique, partial, expression and multi-column indexes
- `INCLUDE`
- operator classes
- collations
- `WITH (...)`
- the access method

`CREATE INDEX CONCURRENTLY` and the [`-- pista:concurrently`](directives.md#-pistaconcurrently) directive mean the same thing. An index without a name gets the name that PostgreSQL would give it. See [Names](#names). `CREATE INDEX` on a plain view is an error.

### Changes

pistachio compares two definitions the way `pg_get_indexdef` writes them:

- `ASC` and the default `NULLS` order are implied
- an operator class or a collation is named without its schema
- `ONLY` on a partitioned table is ignored
- `COLLATE "default"` on a text column is removed

PostgreSQL has no `ALTER INDEX` for a definition, so a change is `DROP INDEX` and `CREATE INDEX`. That pair does not require `--allow-drop`. A change to the `WITH (...)` parameters counts as a definition change.

| Change | DDL |
|---|---|
| Added | `CREATE INDEX`, or `CREATE INDEX CONCURRENTLY` when opted in |
| Removed | `DROP INDEX`, requires `--allow-drop index`; `DROP INDEX CONCURRENTLY` under `--force-index-concurrently`, or under `diff` when the current file opts the index in |
| Definition | `DROP INDEX` and `CREATE INDEX` |
| Renamed | `ALTER INDEX ... RENAME TO` |

Recreating an index removes its comment, so the comment is written again after the `CREATE INDEX`. PostgreSQL refuses to drop an index that a constraint depends on. `plan` fails first and names the dependent, as it does for a constraint.

A `TABLESPACE` written on an index remains in its definition. The catalog never reports it, so such an index is recreated on every plan. `dump` writes none. See [Known limitations](../about/limitations.md).

### On a partitioned table

`pg_get_indexdef` writes `ON ONLY` for every index on a partitioned table, so pistachio ignores `ONLY` when it compares two definitions. `ONLY` matters only to `CREATE INDEX`. Without it, PostgreSQL also creates an index on each partition and attaches it. The `CREATE INDEX` on a partitioned table runs after the creates and alters of every table, `DROP COLUMN` included. It runs deepest level first, so the partitions exist when it runs. See [Order of statements](#order-of-statements).

An index attached to the parent's index is dropped with it. PostgreSQL rejects a `DROP INDEX` on an attached index, so pistachio never emits that statement. `CONCURRENTLY` cannot be used on a partitioned table. An index on a partitioned table that opts into `CONCURRENTLY` is an error at plan time.

## Views

This section covers plain views and materialized views.

### What is read

The parser reads `CREATE VIEW` and `CREATE MATERIALIZED VIEW` with:

- the query
- `WITH (...)`
- a plain view's `WITH [LOCAL | CASCADED] CHECK OPTION`
- a materialized view's `WITH NO DATA`

`ALTER VIEW` and `REFRESH MATERIALIZED VIEW` in a schema file are ignored with a warning.

A plain view's `WITH (...)` contains `security_barrier` and `security_invoker`. These decide what the view means, not how it is stored, so the clause is managed with or without `--manage-storage-param`. The clause comes before `AS`:

```sql
CREATE VIEW public.my_accounts WITH (security_barrier = true, security_invoker = true) AS
  SELECT id, balance FROM public.accounts WHERE owner = CURRENT_USER;
```

A materialized view's `WITH (...)` is managed with `--manage-storage-param`. See [Storage parameters](#storage-parameters).

### Changes

pistachio compares a query as PostgreSQL stores it. These are not changes:

- a schema on a table
- a table prefix on a column
- a cast that the catalog adds
- an `IN` list that the catalog rewrites

A materialized view written with `WITH NO DATA` is created with it, so its query does not run until a `REFRESH MATERIALIZED VIEW`. Whether a view is populated is data state, not schema. pistachio does not compare it, adding or removing `WITH NO DATA` produces no changes on its own, and `dump` does not write it.

| Change | DDL |
|---|---|
| Added | `CREATE OR REPLACE VIEW` or `CREATE MATERIALIZED VIEW`, then its triggers and indexes |
| Removed | `DROP VIEW` or `DROP MATERIALIZED VIEW`, requires `--allow-drop view` |
| Query, columns kept or appended | `CREATE OR REPLACE VIEW` |
| Query, otherwise | `DROP VIEW` and `CREATE OR REPLACE VIEW`, requires `--allow-drop view` |
| Query of a materialized view | `DROP MATERIALIZED VIEW` and `CREATE MATERIALIZED VIEW`, requires `--allow-drop view` |
| Plain to materialized, or back | drop and create, requires `--allow-drop view` |
| Check option | `ALTER VIEW ... SET (check_option = ...)` or `RESET (check_option)` |
| `WITH (...)` parameters | `ALTER VIEW ... SET (...)` or `RESET (...)`, or `ALTER MATERIALIZED VIEW ...` |
| Indexes of a materialized view | They are handled as on a table. |
| Renamed | `ALTER VIEW ... RENAME TO` or `ALTER MATERIALIZED VIEW ... RENAME TO` |

pistachio uses `CREATE OR REPLACE VIEW` where PostgreSQL accepts it: when the new query keeps the output column names in order, with new ones only at the end. A change that removes, renames or reorders a column is a drop and a create instead. So is a query whose column names cannot be determined from the text, such as `SELECT *`. Such a view also appears in the plan on every run. See [Known limitations](../about/limitations.md). pistachio compares only the names. So a query that changes a column's type and keeps its name is emitted as `CREATE OR REPLACE VIEW`, and fails at apply with `cannot change data type of view column`.

A recreate requires `--allow-drop view`. Without it, the plan writes the `DROP` as `-- skipped:` and no `CREATE`. A definition change includes the `WITH (...)` clause and the check option on its `CREATE`. They replace the view's options as a whole. A recreated view gets its comments and triggers again.

PostgreSQL refuses to drop a view that another object reads. It does not cascade. So such a plan would fail partway through `apply`. `plan` fails first and names what reads the view:

```
pista: error: cannot drop public.staff: materialized view public.staff_count, view public.eng_staff depend on it
```

Drops run deepest first, so a dependent that the same plan drops does not block. A chain of views whose columns all change is handled in one plan. So does a view whose dependent the desired schema no longer contains. Any other dependent has to be handled in a run of its own.

pistachio finds dependents in the catalog, not in the schema file. A view that `--include` / `--exclude` hides, or one outside `-n`, blocks the drop just the same. So do a rule, a policy and a routine, each named the way PostgreSQL names it. A routine counts when it reads the view in a `BEGIN ATOMIC` body, or when it returns the view's row type. A routine whose body is a string literal records no dependency and does not block anything.

View drops run before the table statements, and view creates run after them. So in one plan, a view can be dropped, its tables changed, and the view created again.

## Enum types

### What is read

The parser reads `CREATE TYPE ... AS ENUM` with its values in order. `ALTER TYPE ... ADD VALUE` in a schema file is ignored with a warning. The desired values are the ones that `CREATE TYPE` lists.

### Changes

| Change | DDL |
|---|---|
| Added | `CREATE TYPE ... AS ENUM` |
| Removed | `DROP TYPE`, requires `--allow-drop enum` |
| Value added | `ALTER TYPE ... ADD VALUE`, with `AFTER` or `BEFORE` to keep the file's order |
| Value renamed | `ALTER TYPE ... RENAME VALUE`, via the directive on the value |
| Renamed | `ALTER TYPE ... RENAME TO` |

PostgreSQL cannot remove a value from an enum or reorder its values. Either is an error at plan time:

```
pista: error: cannot remove enum value 'inactive' from public.status: PostgreSQL does not support removing enum values
```

pistachio applies a rename of the type to the columns, composite attributes and domains that use it.

## Domains

### What is read

The parser reads `CREATE DOMAIN` with:

- the base type
- `COLLATE`
- `DEFAULT`
- `NOT NULL`
- `CHECK` constraints

It also reads `ALTER DOMAIN ... ADD CONSTRAINT ... CHECK` after the `CREATE DOMAIN`. That is how `dump` writes a `NOT VALID` constraint. Every other `ALTER DOMAIN` is ignored with a warning. A `CHECK` without a name is named `<domain>_check`.

### Changes

| Change | DDL |
|---|---|
| Added | `CREATE DOMAIN`, then `ALTER DOMAIN ... ADD CONSTRAINT ... NOT VALID` for each such constraint |
| Removed | `DROP DOMAIN`, requires `--allow-drop domain` |
| Default | `ALTER DOMAIN ... SET DEFAULT` or `DROP DEFAULT` |
| `NOT NULL` | `ALTER DOMAIN ... SET NOT NULL` or `DROP NOT NULL` |
| Constraint added or changed | `ALTER DOMAIN ... ADD CONSTRAINT`, after a `DROP CONSTRAINT` for a changed one |
| Constraint removed | `ALTER DOMAIN ... DROP CONSTRAINT`, does not require `--allow-drop` |
| `NOT VALID` to validated | `ALTER DOMAIN ... VALIDATE CONSTRAINT` |
| Renamed | `ALTER DOMAIN ... RENAME TO` |

PostgreSQL cannot change a domain's base type or collation. Either is an error at plan time:

```
pista: error: cannot change base type of domain public.email from text to character varying: PostgreSQL does not support this
```

pistachio does not rename a domain constraint, so a renamed one is dropped and added. `--assume-validated` applies to domain constraints as it does to table constraints.

## Composite types

### What is read

The parser reads `CREATE TYPE ... AS (...)` with each attribute's name, type and `COLLATE`. `ALTER TYPE ... ADD / DROP / ALTER / RENAME ATTRIBUTE` in a schema file is ignored with a warning.

### Changes

pistachio matches attributes by name. PostgreSQL cannot reorder them, so their order is not compared.

| Change | DDL |
|---|---|
| Added | `CREATE TYPE ... AS (...)` |
| Removed | `DROP TYPE`, requires `--allow-drop composite_type` |
| Attribute added | `ALTER TYPE ... ADD ATTRIBUTE` |
| Attribute removed | `ALTER TYPE ... DROP ATTRIBUTE`, requires `--allow-drop composite_type` |
| Attribute type or collation | `ALTER TYPE ... ALTER ATTRIBUTE ... TYPE` |
| Attribute renamed | `ALTER TYPE ... RENAME ATTRIBUTE`, via the directive on the attribute |
| Renamed | `ALTER TYPE ... RENAME TO` |

`ALTER ATTRIBUTE ... TYPE` fails at apply while a table column uses the type. PostgreSQL does not allow it, and `CASCADE` does not help.

## Sequences

pistachio manages every sequence except the two kinds that belong to a column:

- The sequence of an identity column.
- The sequence of a `serial` column. A sequence is one when a `smallint`, `integer` or `bigint` column owns it, takes its default from it, and the sequence is still named `<table>_<column>_seq`. `dump` writes such a column as `serial`, `bigserial` or `smallserial`.

Any other sequence that a column owns is managed together with its owner. Examples are a sequence attached with `OWNED BY`, and a `serial` sequence whose table was renamed. `dump` writes the sequence, then the column with its type and `nextval` default, then `ALTER SEQUENCE ... OWNED BY` after the tables.

The desired schema does not have to declare an owned sequence when the column is written as a `serial` type, or when the column keeps its `nextval` default or has no default. The sequence then belongs to the column and is not dropped. As with a `serial` column, a default left out of the desired schema is not dropped.

When the owning table or column is dropped, PostgreSQL drops the sequence too, so pistachio plans no `DROP SEQUENCE` for it.

### What is read

The parser reads `CREATE SEQUENCE` and `CREATE UNLOGGED SEQUENCE` with:

- `AS`
- `INCREMENT`
- `MINVALUE`
- `MAXVALUE`
- `START`
- `CACHE`
- `CYCLE`
- `OWNED BY`

An option that is left out means the default that PostgreSQL gives it, not an unmanaged option. Removing `CACHE 20` from the file produces `CACHE 1` in the plan. Of `ALTER SEQUENCE`, the parser reads only `OWNED BY`. Any other option is ignored with a warning.

### Changes

| Change | DDL |
|---|---|
| Added | `CREATE SEQUENCE` with every option written out |
| Removed | `DROP SEQUENCE`, requires `--allow-drop sequence` |
| Options | one `ALTER SEQUENCE` naming the options that changed |
| `UNLOGGED` | `ALTER SEQUENCE ... SET UNLOGGED` or `SET LOGGED` |
| Owner set or changed | `ALTER SEQUENCE ... OWNED BY`, after the table statements |
| Owner removed | `ALTER SEQUENCE ... OWNED BY NONE`, before the table statements |
| Renamed | `ALTER SEQUENCE ... RENAME TO` |

pistachio plans no `RESTART`. So a change that puts the current value outside the new range fails at apply with the server's error. A sequence that a column default reads through `nextval` is created before that table and dropped after it. A rename of the sequence is applied to the default.

## Routines

Functions and procedures are managed only when `--manage-routine` is passed. Without it, pistachio does not read routines. So a schema maintained with `-- pista:execute` keeps working, and plan output is unchanged.

```bash
pista dump --manage-routine
pista plan --manage-routine schema.sql
pista apply --manage-routine schema.sql
```

### What is read

The parser reads `CREATE FUNCTION` and `CREATE PROCEDURE`:

- the parameters, with their modes, names, types and defaults
- the return type
- the language
- the body
- the attributes `IMMUTABLE` / `STABLE` / `VOLATILE`, `STRICT`, `SECURITY DEFINER`, `LEAKPROOF`, `PARALLEL`, `COST`, `ROWS` and `SET`

A body is a string, a `BEGIN ATOMIC ... END` or `RETURN <expr>` block, or the object file and symbol of a C function.

A routine is identified by its name and its argument types, so an overload set is several independent objects:

```sql
CREATE FUNCTION public.normalize(e text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT lower(e) $$;

CREATE FUNCTION public.normalize(e text, keep_case boolean) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT CASE WHEN keep_case THEN e ELSE lower(e) END $$;
```

An attribute left at its default is not written back. PostgreSQL reports `VOLATILE`, `PARALLEL UNSAFE` and the default `COST` as absent. So a desired schema may write them out or leave them off. Argument and return types are reported without their schema when `search_path` reaches them, like any other name that pistachio reads back. A desired schema may write a type in the routine's own schema with or without the schema name.

### Changes

pistachio compares the body, the language and the attributes. A change is applied with `CREATE OR REPLACE`. The changes that PostgreSQL refuses in `CREATE OR REPLACE` run as `DROP` and `CREATE`, which requires `--allow-drop routine`:

- changing the return type, including the columns of a `RETURNS TABLE`
- renaming a parameter, or adding or removing an `OUT` parameter
- removing a parameter default
- turning a function into a procedure, or back

Without the flag, the plan writes the `DROP` as `-- skipped:` and leaves the routine unchanged.

Adding or removing an input parameter, `IN`, `INOUT` or `VARIADIC`, is not a change to the routine. The input types are its identity. So the new signature is a new routine, and the old one is dropped, which also requires `--allow-drop routine`. Without the flag, the new routine is still created, and the `DROP` of the old one is written as `-- skipped:`. The database then has both overloads.

PostgreSQL refuses the `DROP` of a recreate while anything calls the routine:

- a `CHECK` constraint
- a column default
- a generated column
- an index
- a view
- a policy
- a trigger
- a domain constraint
- a `BEGIN ATOMIC` routine

A body written as a string records no dependency and does not block. `plan` fails and names the callers:

```
pista: error: cannot drop function public.f(integer): table constraint t_check on public.t depends on it
```

The routine is recreated before the tables change, so changing the dependent in the same plan does not help. Change the dependent in one run and the routine in the next. A view or a `BEGIN ATOMIC` routine that the plan drops does not block, because both are dropped first. A pure removal is not checked. It fails at apply when something still calls the routine.

### Order

A routine is created after the types that its signature names, and before every table. This is because a `CHECK` constraint, a `GENERATED` expression, an index expression, a policy or a trigger can call a routine. A signature can name a table instead of a type, as `RETURNS SETOF <table>` does. Such a routine comes after that table. It comes before every other table where that does not form a cycle. Drops run the other way, dependents first.

Because of that order, a `LANGUAGE sql` routine whose body reads a table created in the same run fails to apply. PostgreSQL parses a SQL body at creation time. The same is true for a routine that calls a routine defined later, and for a `plpgsql` routine whose `DECLARE` uses a table's row type or `%TYPE`. Applying with `--pre-sql 'SET check_function_bodies = off'` skips that check. Marking the routine `-- pista:ignore` and creating it with `-- pista:execute` also works. So does writing the body as `BEGIN ATOMIC`.

A body written as `BEGIN ATOMIC ... END` or `RETURN <expr>` is managed too. PostgreSQL parses such a body when it creates the routine, so pistachio orders the routine like a view. It is created after the tables, views and routines that its body names, and dropped before them. `dump` writes it after the views, with the body as PostgreSQL returns it:

```sql
CREATE OR REPLACE FUNCTION public.total()
    RETURNS bigint
    LANGUAGE sql
    STABLE
    BEGIN ATOMIC
        SELECT count(*) AS count FROM v;
    END;
```

PostgreSQL qualifies the names in the body, so `SELECT a FROM t` comes back as `SELECT t.a FROM t`. A body written differently from the dump is replaced on every plan. A `CHECK` constraint, column default, generated column, index expression or trigger `WHEN` condition that calls such a routine cannot be created in the same run as the routine. A policy on a new table can, because those policies run last. A policy on a table that exists cannot.

### Not managed

The following are left out on both sides, so `dump` does not write them and `plan` does not propose dropping them:

- aggregates and window functions
- a routine that an extension owns, and the constructors of a range type
- a routine with an option that pistachio does not read, such as `SUPPORT` or `TRANSFORM FOR TYPE`

A schema file that declares one of these gets the unsupported statement warning. `-- pista:renamed-from` on a routine is an error.

`SET <parameter> FROM CURRENT` is handled differently. It captures the session value at creation time, so the statement has nothing to compare. But the database reports the resolved value, and pistachio reads the routine back like any other. Such a routine is treated as `-- pista:ignore`. It is neither created, altered nor dropped, and its signature is listed under `-- ignored:`.

## Triggers

### What is read

The parser reads `CREATE TRIGGER` and `CREATE CONSTRAINT TRIGGER` on a table or view declared earlier. It reads the enable state from `ALTER TABLE ... ENABLE / DISABLE / ENABLE ALWAYS / ENABLE REPLICA TRIGGER <name>` after the trigger. `CREATE TRIGGER` has no syntax for a disabled trigger, so a desired schema asks for one the way `dump` writes it:

```sql
ALTER TABLE public.events DISABLE TRIGGER events_stamp;
```

PostgreSQL rejects the statement on a view, so a view's triggers have no state to set. The `ALL` and `USER` forms name no single trigger, and are ignored with a warning. Event triggers belong to the database, not to a schema, and are not managed. A trigger that an extension owns is not managed either.

The function that a trigger calls is managed only with `--manage-routine`. See [Routines](#routines). Without that flag, write the function with `-- pista:execute-first`. Then it exists before the `CREATE TRIGGER` that references it runs:

```sql
-- pista:execute-first
CREATE OR REPLACE FUNCTION public.stamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE public.events (
    id integer NOT NULL,
    updated_at timestamptz,
    CONSTRAINT events_pkey PRIMARY KEY (id)
);

CREATE TRIGGER events_stamp BEFORE UPDATE ON public.events
    FOR EACH ROW EXECUTE FUNCTION public.stamp();
```

### Changes

| Change | DDL |
|---|---|
| Added | `CREATE TRIGGER`, then its state when not `ENABLE` |
| Removed | `DROP TRIGGER`, requires `--allow-drop trigger` |
| Definition | `CREATE OR REPLACE TRIGGER` |
| Definition, constraint trigger on either side | `DROP TRIGGER` and `CREATE TRIGGER`, requires `--allow-drop trigger` |
| State | `ALTER TABLE ... ENABLE / DISABLE ... TRIGGER` |
| Renamed | `ALTER TRIGGER ... ON ... RENAME TO` |

`CREATE OR REPLACE TRIGGER` needs a lighter lock than a drop and a create. PostgreSQL has no `CREATE OR REPLACE CONSTRAINT TRIGGER`. So a change with a constraint trigger on either side is a drop and a create. Without `--allow-drop trigger`, the trigger keeps its current definition. Both forms leave a trigger enabled, so pistachio applies any other state again right after.

pistachio compares the function by name alone. The catalog reports it without its schema when `search_path` reaches it. So `public.stamp()` and `stamp()` are one function, and so are two functions with the same name in different schemas.

PostgreSQL clones a trigger on a partitioned table onto every partition. pistachio reads and writes only the parent's trigger, and PostgreSQL keeps the clones in step. A trigger created on a partition itself is managed like any other.

## Policies and row-level security

### What is read

The parser reads `ALTER TABLE ... ENABLE / DISABLE / FORCE / NO FORCE ROW LEVEL SECURITY` and `CREATE POLICY` after the table. Of `CREATE POLICY`, it reads:

- `AS PERMISSIVE / RESTRICTIVE`
- `FOR`
- `TO`
- `USING`
- `WITH CHECK`

`ALTER POLICY` and `DROP POLICY` in a schema file are ignored with a warning.

### Changes

pistachio compares `USING` and `WITH CHECK` as expressions. So a cast that the catalog adds, or an `IN` list that it rewrites, is not a change. It compares the role list as a set, and `TO PUBLIC` is the same as no list.

| Change | DDL |
|---|---|
| Row-level security | `ALTER TABLE ... ENABLE / DISABLE / FORCE / NO FORCE ROW LEVEL SECURITY` |
| Policy added | `CREATE POLICY` |
| Policy removed | `DROP POLICY`, requires `--allow-drop policy` |
| Roles, `USING` or `WITH CHECK` changed or added | `ALTER POLICY ... TO ... USING (...) WITH CHECK (...)`, naming the parts that changed |
| Command or permissiveness changed, or a clause removed | `DROP POLICY` and `CREATE POLICY` |
| Renamed | `ALTER POLICY ... ON ... RENAME TO` |

`ALTER POLICY` cannot change a policy's command or permissiveness, and it cannot remove a `USING` or `WITH CHECK` clause. So those changes recreate the policy. That pair does not require `--allow-drop`.

The row-level security statements come before the policy statements of their table. The policies of a new table run at the end of the plan, after the views. So such a policy can read a view created in the same run. The policies of a table that exists run with the table's own statements, and cannot.

## Comments

The parser reads `COMMENT ON` for:

- tables, views and materialized views, and columns of all three
- composite attributes
- indexes
- constraints and foreign keys
- triggers
- policies
- enum and composite types
- domains and domain constraints
- sequences
- functions and procedures

Any other target, such as `SCHEMA`, is ignored with a warning.

`dump` writes a comment after the object that it belongs to. A comment that the schema file no longer contains is cleared with `COMMENT ON ... IS NULL`. `IS ''` in a file means the same as `IS NULL`.

A comment on a constraint, a foreign key, a trigger or a policy names the table that the object is on. A comment on a domain constraint names the domain. `COMMENT ON INDEX` names the index alone, because PostgreSQL keeps index names unique within a schema:

```sql
COMMENT ON CONSTRAINT users_email_key ON public.users IS 'One account per address';
COMMENT ON TRIGGER users_touch ON public.users IS 'Sets updated_at';
COMMENT ON POLICY users_own ON public.users IS 'Own rows only';
COMMENT ON CONSTRAINT email_check ON DOMAIN public.email IS 'Has an at sign';
COMMENT ON INDEX public.users_email_idx IS 'Lookup by email';
COMMENT ON FUNCTION public.normalize(text) IS 'v1';
```

A `COMMENT ON FUNCTION` written without an argument list matches when only one routine has the name. When an object is dropped and created again, the plan writes its comment again. These keep the comment:

- a rename
- `CREATE OR REPLACE VIEW`
- `CREATE OR REPLACE TRIGGER`
- `ALTER POLICY`
- `ALTER CONSTRAINT`
- `VALIDATE CONSTRAINT`

A comment on an object that the schema file does not declare before it is ignored without a warning. That includes:

- a comment written above the object's `CREATE`
- a comment on an inherited column of an `INHERITS` child
- a comment on the index that a `PRIMARY KEY`, `UNIQUE` or `EXCLUDE` constraint owns; write it with `COMMENT ON CONSTRAINT` instead
- a comment on a `NOT NULL` constraint, which PostgreSQL 18 `pg_dump` writes and pistachio does not read as a constraint

## Names

An unnamed constraint, such as `id integer PRIMARY KEY`, `name text UNIQUE` or `col integer REFERENCES other(id)`, gets the name that PostgreSQL would give it. A primary key is `{table}_pkey`. A unique, foreign key or exclusion constraint is `{table}_{col}..._key`, `{table}_{col}..._fkey` or `{table}_{col}..._excl`, joining every key column. A `UNIQUE` or `EXCLUDE` joins its `INCLUDE` columns as well. An `EXCLUDE` element written as an expression is named the way an index element is, described below. A `CHECK` becomes `{table}_{col}_check` when its expression references one column, and `{table}_check` when it references none or several. PostgreSQL does the same, even for a constraint written on a column.

An index written without a name, such as `CREATE INDEX ON users (name)`, is named `{table}_{col}..._idx`, joining every index element including the `INCLUDE` list. An element written as an expression gets the name that PostgreSQL gives it:

- the function that it calls, so `(lower(name))` gives `users_lower_idx`
- the field of a field selection
- the column under a subscript
- the type of a cast that has nothing under it
- `expr` when none of these applies

A name that does not fit in 63 bytes is shortened the way PostgreSQL shortens it. The table and column parts are trimmed, and the trailing label is kept.

A generated name can collide with a name already in use, whether generated or explicit. PostgreSQL then appends a number with no separator, `users_id_check1` or `users_name_idx1`. pistachio cannot predict that number, so it rejects such a file as a duplicate name. Write explicit `CONSTRAINT <name>` clauses and index names where that happens.
