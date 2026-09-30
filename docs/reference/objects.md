# Supported objects

pistachio manages the objects below. Each section says what the parser reads from a schema file, what is compared against the database, and what DDL a change becomes.

| Object | Read from | Opt-in | `--allow-drop` type |
|---|---|---|---|
| [Tables](#tables) | `CREATE TABLE`, part of `ALTER TABLE` | | `table` |
| [Columns](#columns) | the column list of `CREATE TABLE` | | `column` |
| [Constraints](#constraints) | column and table constraints, `ALTER TABLE ... ADD CONSTRAINT` | | `constraint` |
| [Foreign keys](#foreign-keys) | `REFERENCES`, `FOREIGN KEY`, `ALTER TABLE ... ADD CONSTRAINT` | | `foreign_key` |
| [Indexes](#indexes) | `CREATE INDEX` | | `index` |
| [Views](#views) | `CREATE VIEW`, `CREATE MATERIALIZED VIEW` | | `view` |
| [Enum types](#enum-types) | `CREATE TYPE ... AS ENUM` | | `enum` |
| [Domains](#domains) | `CREATE DOMAIN`, `ALTER DOMAIN ... ADD CONSTRAINT` | | `domain` |
| [Composite types](#composite-types) | `CREATE TYPE ... AS (...)` | | `composite_type` |
| [Sequences](#sequences) | `CREATE SEQUENCE`, `ALTER SEQUENCE ... OWNED BY` | | `sequence` |
| [Routines](#routines) | `CREATE FUNCTION`, `CREATE PROCEDURE` | `--manage-routine` | `routine` |
| [Triggers](#triggers) | `CREATE TRIGGER`, `ALTER TABLE ... ENABLE/DISABLE TRIGGER` | | `trigger` |
| [Policies](#policies-and-row-level-security) | `CREATE POLICY`, `ALTER TABLE ... ROW LEVEL SECURITY` | | `policy` |
| [Comments](#comments) | `COMMENT ON` | | |

`--allow-drop` gates a pure removal: an object that the desired schema no longer holds. Without the type, the drop is written as a `-- skipped:` comment and nothing runs. The drop half of a definition change, a constraint or an index that is dropped to be added back, is not gated. A view, routine or trigger recreate is gated. See [Controlling drops](../guides/drops.md).

Renaming any of these objects, and enum values and composite attributes, goes through the [`-- pista:renamed-from`](directives.md#-pistarenamed-from) directive. Routines cannot be renamed.

## Statements that are not read

pistachio reads only the statements that the sections below name. Every other statement in a schema file is dropped with a warning on standard error:

```
pista: schema.sql:12:1: ignored unsupported statement: GRANT select ON public.users TO app
```

That covers `SET`, `GRANT`, `CREATE EXTENSION`, `CREATE RULE`, `CREATE AGGREGATE`, every `DROP`, and every `ALTER` that the sections below do not list. The desired state is what `CREATE` says. The same warning covers the part of a statement that pistachio does not read: the `ALTER TABLE ... ADD COLUMN` that a `pg_dump` file carries, or a `LIKE` clause in `CREATE TABLE`. A `BEGIN` or `COMMIT` warning points at `--with-tx` and `--try-tx`.

To keep such a statement in the file and run it during `apply`, mark it with [`-- pista:execute`](directives.md#-pistaexecute), which also silences the warning.

A statement that names another object, `ALTER TABLE`, `CREATE INDEX`, `CREATE TRIGGER` or `CREATE POLICY` for one, has to come after that object's `CREATE`, in the same file or an earlier one. One that does not is an error:

```
pista: error: ALTER TABLE public.users: table public.users is not declared before it
 --> schema.sql:3:1
  |
3 | ALTER TABLE public.users ADD CONSTRAINT users_id_check CHECK (id > 0);
  | ^
```

Two objects that PostgreSQL keeps in one namespace, two relations or two types, cannot share a name in one schema. A routine is identified by its name and argument types, so overloads are separate objects.

## Order of statements

A plan runs in this order. Within a group, objects are sorted by their dependencies: a type comes before the table that uses it, a table before a view that reads it, and a partition after its parent.

1. Foreign key drops.
2. View drops and `BEGIN ATOMIC` routine drops, dependents first.
3. Creates and alters: enum types, domains, composite types, sequences, routines, then tables. A table's statements include its constraints, indexes, triggers, row-level security and, for a table that already exists, its policies.
4. Indexes on partitioned tables.
5. `SET LOGGED` and `SET UNLOGGED`.
6. Drops, dependents first: tables, sequences, routines, composite types, domains, enum types.
7. Foreign key adds, renames, `ALTER CONSTRAINT` and `VALIDATE CONSTRAINT`.
8. View creates and `BEGIN ATOMIC` routine creates.
9. The policies of new tables.

## Tables

### What is read

`CREATE TABLE` is read, with `UNLOGGED`, `PARTITION BY`, `PARTITION OF ... FOR VALUES`, `INHERITS`, `TABLESPACE` and `WITH (...)`. Of `ALTER TABLE`, only `ADD CONSTRAINT`, the row-level security toggles, the trigger states and `ALTER COLUMN ... SET STORAGE` / `SET COMPRESSION` are read. Any other action is warned about and dropped, `ADD COLUMN` included: the desired columns are the ones that `CREATE TABLE` lists.

### Changes

A new table is one `CREATE TABLE`, followed by its `NOT VALID` constraints, indexes, `ENABLE` / `FORCE ROW LEVEL SECURITY`, triggers and comments. Its foreign keys and policies come at the end of the plan.

A removed table is `DROP TABLE`, gated by `table`, after the `DROP CONSTRAINT` of each foreign key that it holds. The drop is not checked for dependents: a view that reads the table and stays makes it fail at apply.

`UNLOGGED` that is added or removed goes out as `ALTER TABLE ... SET UNLOGGED` or `SET LOGGED`, which rewrites the table. PostgreSQL refuses a logged table that references an unlogged one, so the statements are ordered by the foreign keys between the tables that change. A partitioned table is skipped, since the statement changes nothing there.

A table's `TABLESPACE` is written on `CREATE TABLE` and not compared afterwards.

### Storage parameters

A table's storage parameters, the `WITH (...)` clause, are managed only with `--manage-storage-param`. They are off by default because the autovacuum settings that a table is tuned with are usually set on the database, not written in the schema file. A materialized view takes the same parameters and sits behind the same flag.

Without the flag the parameters are dropped from both sides of the diff: nothing is planned, the clause that a schema file writes is left off the `CREATE`, and `dump` does not write one.

With it the schema file states every parameter that the relation is to have. A change goes out as `ALTER TABLE ... SET (...)`, with a `RESET (...)` for the parameters that the file no longer names. Neither rewrites the relation. A `toast.` parameter belongs to the TOAST relation. PostgreSQL creates that relation only for a table with a toastable column and discards the setting when there is none, so such a table re-plans it on every run. A partitioned table holds no parameter, and a partition does not inherit the parent's.

An index's parameters are part of its definition and are managed either way. So is a plain view's `WITH (...)`, which holds only `security_barrier` and `security_invoker`. See [Views](#views).

### Partitions

A partition is declared as `CREATE TABLE ... PARTITION OF parent FOR VALUES ...` after its parent. Its columns and constraints come from the parent and are neither written nor compared. On a partition that exists, indexes, foreign keys, triggers, policies, row-level security, comments and storage parameters are compared. Its bound, its parent and the parent's partition key are not compared: changing one plans nothing. PostgreSQL has no `ALTER` for a partition key, and moving a bound or a parent goes through `DETACH` and `ATTACH`, which pistachio does not emit.

A trigger on a partitioned table is cloned onto every partition. Only the parent's trigger is read and written, and PostgreSQL keeps the clones in step. A foreign key on the parent is copied the same way. See [Foreign keys](#foreign-keys). An index on the parent is created after every table statement. See [Indexes](#indexes).

`--skip-partition-child` leaves every partition out of both sides, for a schema whose partitions another tool creates. See [Skipping partition children](../guides/filtering.md#skipping-partition-children).

### Inheritance

An `INHERITS (...)` child is managed as the columns and constraints that it declares itself. That is what its `CREATE TABLE` writes, and what `pg_dump` writes for it.

```sql
CREATE TABLE public.shapes (id integer NOT NULL, name text);
CREATE TABLE public.boxes (w integer NOT NULL) INHERITS (public.shapes);
```

`boxes` is managed as `w` alone. `id` and `name` belong to `shapes` and are read from neither side, so a change to them goes out against the parent and PostgreSQL carries it down.

A column that the child redeclares is its own, and PostgreSQL merges it with the inherited one. That happens at `CREATE TABLE` only. Adding the redeclaration to a child that already exists, or taking it away, has no DDL behind it: `ADD COLUMN` on an inherited column fails with `column ... already exists`, and `DROP COLUMN` with `cannot drop inherited column`. The plan emits one of the two anyway, since the desired side cannot tell a redeclared column from an inherited one. Redeclare a column when the child is created, or leave it alone.

A comment on an inherited column is not managed. `dump` writes none, and one that a schema file carries is ignored, so nothing drifts. `pg_dump` does write it, and that line is lost.

Only the first parent is read, so `INHERITS (a, b)` is dumped as `INHERITS (a)`. Both sides read it the same way, so a dump fed back plans clean, but reloading one gives a table that does not inherit `b`.

`--skip-partition-child` does not reach an `INHERITS` child. It applies to a declarative partition, which carries a bound.

## Columns

### What is read

The column definitions of `CREATE TABLE` are read: the type, `COLLATE`, `DEFAULT`, `NOT NULL` with an optional constraint name, `GENERATED ... AS IDENTITY` with its sequence options, `GENERATED ALWAYS AS (...) STORED`, `STORAGE` and `COMPRESSION`. `ALTER TABLE ... ALTER COLUMN ... SET STORAGE` / `SET COMPRESSION` after the table are read too, which is how `pg_dump` writes them.

A type is read in the form that the catalog reports, so an alias in the file compares equal to the name that the database holds: `int` and `int4` are `integer`, `varchar` is `character varying`, `timestamptz` is `timestamp with time zone`. `serial`, `bigserial` and `smallserial` are `integer`, `bigint` and `smallint` with a `nextval` default, and a column written either way compares equal. A primary key column is `NOT NULL` whether or not the file says so. `COLLATE "default"` is what a column has implicitly and is read as no collation. Any built-in type is accepted, arrays included, and an identifier may be quoted.

### Changes

Columns are matched by name. Order is not compared, and a new column is appended.

| Change | DDL |
|---|---|
| Added | `ALTER TABLE ... ADD COLUMN`, with its default, identity, generated expression and `NOT NULL`, then `SET STORAGE` / `SET COMPRESSION` |
| Removed | `ALTER TABLE ... DROP COLUMN`, gated by `column` |
| Type or collation | `ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE` |
| Default | `SET DEFAULT` or `DROP DEFAULT` |
| `NOT NULL` | `SET NOT NULL` or `DROP NOT NULL` |
| Identity | `ADD GENERATED ... AS IDENTITY`, `DROP IDENTITY`, or `SET GENERATED` with the sequence options that changed |
| Storage or compression | `SET STORAGE` or `SET COMPRESSION` |
| Generated expression | The change is an error. See below. |

A type or collation change is the first statement for the column. PostgreSQL refuses it while a view or materialized view, a rule, a trigger, a policy, a `BEGIN ATOMIC` routine or a generated column depends on the column. `plan` fails first and names the dependents:

```
pista: error: cannot change the type of public.t.n: view public.v depends on it
```

A trigger blocks even when the column is only in its `UPDATE OF` list. The change reaches the table's partitions and `INHERITS` children, so a dependent on their copy of the column blocks too. A view or a `BEGIN ATOMIC` routine that the same plan drops does not block. A trigger, a policy or a generated column that the plan drops still does, since those drops run after the type change. Indexes and constraints do not block, since PostgreSQL rebuilds them. `SET DATA TYPE` resets storage and compression, so both are written again after it when the file names them.

`DROP NOT NULL` runs after the table's constraint statements, so a primary key dropped in the same plan goes first. `DROP COLUMN` runs after the table's constraint, index, trigger, policy and comment statements.

An identity column's sequence options, the `( ... )` after `AS IDENTITY`, are managed. No `RESTART` is planned, so a change that puts the current value outside the new range fails at apply with the server's error.

A generated column cannot change in place. Turning a column generated or plain, or changing its expression, is an error at plan time:

```
pista: error: column public.t.total: cannot change GENERATED expression; DROP COLUMN + ADD COLUMN is required
```

A column that names no `STORAGE` or `COMPRESSION` asks for the default, so a column that the database holds at `EXTERNAL` plans back to its type's own strategy. Neither statement rewrites the table.

A `DEFAULT` is compared as an expression, not as text, so a cast that the catalog adds or an `IN` list that it stores as `= ANY (ARRAY[...])` is not a change. A rename via `-- pista:renamed-from` rewrites the column's references in the same table's indexes, constraints, foreign keys, triggers, policies and generated expressions, so one `RENAME COLUMN` goes out. See [Renaming objects](../guides/renaming.md).

## Constraints

This section covers primary keys, unique constraints, check constraints and exclusion constraints. Foreign keys have a [section of their own](#foreign-keys).

### What is read

A constraint is read whether it is written on a column, in the table's constraint list, or as `ALTER TABLE ... ADD CONSTRAINT` after the table, `NOT VALID` included. A `DEFERRABLE` or `INITIALLY DEFERRED` on a column constraint belongs to the constraint. A constraint without a name takes the one that PostgreSQL would give it. See [Names](#names).

### Changes

PostgreSQL has no `ALTER` for a constraint's definition, so a change to one is `DROP CONSTRAINT` and `ADD CONSTRAINT`. That pair is not gated. A pure removal is gated by `constraint`. A definition is compared as an expression, so a cast that the catalog adds or an `IN` list that it rewrites is not a change.

| Change | DDL |
|---|---|
| Added | `ALTER TABLE ... ADD CONSTRAINT`, with `NOT VALID` when the file writes it |
| Removed | `ALTER TABLE ... DROP CONSTRAINT`, gated by `constraint` |
| Definition | `DROP CONSTRAINT` and `ADD CONSTRAINT` |
| `NOT VALID` to validated | `ALTER TABLE ... VALIDATE CONSTRAINT` |
| Validated to `NOT VALID` | `DROP CONSTRAINT` and `ADD CONSTRAINT ... NOT VALID` |
| Deferral clause | `DROP CONSTRAINT` and `ADD CONSTRAINT`. Only a foreign key takes `ALTER CONSTRAINT`. |
| Renamed | `ALTER TABLE ... RENAME CONSTRAINT` |

A key's index is rebuilt by the drop and add. PostgreSQL refuses the drop while a foreign key references the key or a view groups by the primary key. `plan` fails first and names them:

```
pista: error: cannot drop constraint t_pkey on public.t: foreign key r_a_fkey on public.r depends on it
```

A foreign key or a view that the same plan drops does not block. To change a referenced key, change the foreign key in the same run, or drop it and add it back in a later run.

`--assume-validated` treats every constraint as validated on both sides, so no `NOT VALID` and no `VALIDATE CONSTRAINT` is written. A validated constraint that the desired schema writes as `NOT VALID` is otherwise added back, because nothing takes the flag away.

A `NOT VALID` check on a new table is added after the `CREATE TABLE`, as `ALTER TABLE ONLY ... ADD CONSTRAINT ... NOT VALID`, without `ONLY` on a partitioned table. A constraint added to a table that exists takes no `ONLY`.

## Foreign keys

### What is read

`REFERENCES` on a column, `FOREIGN KEY` in the table's constraint list, and `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY` after the table are read, `NOT VALID` included. A `REFERENCES` that names no columns takes the referenced table's primary key when that table is declared. A referenced table written without a schema is in the default schema.

### Changes

Foreign keys are dropped at the start of a plan and added at the end, so a table can be created, changed or dropped between the two.

| Change | DDL |
|---|---|
| Added | `ALTER TABLE ONLY ... ADD CONSTRAINT ... FOREIGN KEY`, with `NOT VALID` when the file writes it |
| Removed | `ALTER TABLE ... DROP CONSTRAINT`, gated by `foreign_key` |
| Definition | `DROP CONSTRAINT` and `ADD CONSTRAINT` |
| Deferral clause alone | `ALTER TABLE ... ALTER CONSTRAINT ... DEFERRABLE INITIALLY DEFERRED` |
| `NOT VALID` to validated | `ALTER TABLE ... VALIDATE CONSTRAINT` |
| Renamed | `ALTER TABLE ... RENAME CONSTRAINT` |

`ALTER CONSTRAINT` rewrites a catalog row rather than rescanning the table. A key that is also being validated takes `VALIDATE CONSTRAINT` next to it. A foreign key dropped because its table is dropped follows the `table` policy, not `foreign_key`.

A foreign key on a partitioned table is added without `ONLY`, which PostgreSQL rejects there. Nothing inherits a foreign key, so the word decides nothing on a plain table either. It is kept there because that is what `pg_dump` writes.

A partition holds a copy of every foreign key that its parent declares, and the copy takes no statement of its own. The parent's `DROP` takes the copy with it, its `ADD` puts a new copy back, and its `ALTER CONSTRAINT` and `VALIDATE CONSTRAINT` recurse. `dump` leaves the copy out for the same reason, as `pg_dump` does. A rename is the one statement that does not reach the copy: it keeps its old name while a partition attached later takes the new one, and since the copy is neither written nor compared, nothing drifts. A key that the partition declares itself is managed like any other.

## Indexes

### What is read

`CREATE INDEX` after the table or materialized view that it is on is read: unique, partial, expression, multi-column, `INCLUDE`, operator classes, collations, `WITH (...)` and the access method. `CREATE INDEX CONCURRENTLY` and the [`-- pista:concurrently`](directives.md#-pistaconcurrently) directive mean the same thing. An index without a name takes the one that PostgreSQL would give it. See [Names](#names). `CREATE INDEX` on a plain view is an error.

### Changes

Two definitions are compared the way `pg_get_indexdef` writes them: `ASC` and the default `NULLS` order are implied, an operator class or a collation is named without its schema, `ONLY` on a partitioned table is ignored, and `COLLATE "default"` on a text column is dropped. PostgreSQL has no `ALTER INDEX` for a definition, so a change is `DROP INDEX` and `CREATE INDEX`, and that pair is not gated. A change to the `WITH (...)` parameters counts as a definition change.

| Change | DDL |
|---|---|
| Added | `CREATE INDEX`, or `CREATE INDEX CONCURRENTLY` when opted in |
| Removed | `DROP INDEX`, gated by `index`; `DROP INDEX CONCURRENTLY` under `--force-index-concurrently`, or under `diff` when the current file opts the index in |
| Definition | `DROP INDEX` and `CREATE INDEX` |
| Renamed | `ALTER INDEX ... RENAME TO` |

Recreating an index drops its comment, so the comment is written again after the `CREATE INDEX`. PostgreSQL refuses to drop an index that a constraint depends on. `plan` fails first and names it, as it does for a constraint.

A `TABLESPACE` written on an index stays in its definition, which the catalog never reports, so such an index is recreated on every plan. `dump` writes none. See [Known limitations](../about/limitations.md).

### On a partitioned table

`pg_get_indexdef` writes `ON ONLY` for every index on a partitioned table, so `ONLY` is ignored when two definitions are compared. It matters only to `CREATE INDEX`: without it PostgreSQL also creates an index on each partition and attaches it. The `CREATE INDEX` on a partitioned table runs after the creates and alters of every table, `DROP COLUMN` included, deepest level first, so the partitions exist when it runs. See [Order of statements](#order-of-statements).

An index attached to the parent's is dropped with it, and PostgreSQL rejects a `DROP INDEX` on one, so pistachio never emits that statement. `CONCURRENTLY` cannot be used on a partitioned table, and an index opted in there is an error at plan time.

## Views

This section covers plain views and materialized views.

### What is read

`CREATE VIEW` and `CREATE MATERIALIZED VIEW` are read, with the query, `WITH (...)`, a plain view's `WITH [LOCAL | CASCADED] CHECK OPTION`, and a materialized view's `WITH NO DATA`. `ALTER VIEW` and `REFRESH MATERIALIZED VIEW` in a schema file are warned about and dropped.

A plain view's `WITH (...)` holds `security_barrier` and `security_invoker`, which decide what the view means rather than how it is stored, and is managed either way. The clause precedes `AS`:

```sql
CREATE VIEW public.my_accounts WITH (security_barrier = true, security_invoker = true) AS
  SELECT id, balance FROM public.accounts WHERE owner = CURRENT_USER;
```

A materialized view's `WITH (...)` is managed with `--manage-storage-param`. See [Storage parameters](#storage-parameters).

### Changes

A query is compared as PostgreSQL stores it: a schema on a table, a table prefix on a column, a cast that the catalog adds and an `IN` list that it rewrites are not changes. A materialized view written with `WITH NO DATA` is created with it, so its query does not run until a `REFRESH MATERIALIZED VIEW`. Whether a view is populated is data state, not schema: it is not compared, adding or removing `WITH NO DATA` plans nothing on its own, and `dump` does not write it.

| Change | DDL |
|---|---|
| Added | `CREATE OR REPLACE VIEW` or `CREATE MATERIALIZED VIEW`, then its triggers and indexes |
| Removed | `DROP VIEW` or `DROP MATERIALIZED VIEW`, gated by `view` |
| Query, columns kept or appended | `CREATE OR REPLACE VIEW` |
| Query, otherwise | `DROP VIEW` and `CREATE OR REPLACE VIEW`, gated by `view` |
| Query of a materialized view | `DROP MATERIALIZED VIEW` and `CREATE MATERIALIZED VIEW`, gated by `view` |
| Plain to materialized, or back | drop and create, gated by `view` |
| Check option | `ALTER VIEW ... SET (check_option = ...)` or `RESET (check_option)` |
| `WITH (...)` parameters | `ALTER VIEW ... SET (...)` or `RESET (...)`, or `ALTER MATERIALIZED VIEW ...` |
| Indexes of a materialized view | They are handled as on a table. |
| Renamed | `ALTER VIEW ... RENAME TO` or `ALTER MATERIALIZED VIEW ... RENAME TO` |

`CREATE OR REPLACE VIEW` is used where PostgreSQL accepts it: when the new query keeps the output column names in order, with new ones only at the end. A change that removes, renames or reorders a column is a drop and a create instead, and so is a query whose column names cannot be told from the text, a `SELECT *` for one, which also re-plans on every run. See [Known limitations](../about/limitations.md). Only the names are compared, so a query that changes a column's type and keeps its name goes out as `CREATE OR REPLACE VIEW` and fails at apply with `cannot change data type of view column`.

A recreate needs `--allow-drop view`. Without it the plan writes the `DROP` as `-- skipped:` and no `CREATE`. A definition change carries the `WITH (...)` clause and the check option on its `CREATE`, which replace the view's options as a whole. A recreated view gets its comments and triggers again.

PostgreSQL refuses to drop a view that another object reads, instead of cascading, so that plan would fail partway through `apply`. `plan` fails first and names what reads it:

```
pista: error: cannot drop public.staff: materialized view public.staff_count, view public.eng_staff depend on it
```

Drops run deepest first, so a dependent that the same plan drops is no obstacle. A chain of views that all change shape goes through as it is, and so does a view whose dependent the desired schema no longer holds. Anything else has to be moved in a run of its own.

Dependents come from the catalog rather than the schema file. A view that `--include` / `--exclude` hides, or one outside `-n`, blocks the drop just the same. So do a rule, a policy and a routine, each named the way PostgreSQL names it. A routine counts when it reads the view in a `BEGIN ATOMIC` body or returns the view's row type; one whose body is a string literal records no dependency and does not block anything.

View drops run before the table statements and view creates after them, so a view can be dropped, its tables changed, and the view created again in one plan.

## Enum types

### What is read

`CREATE TYPE ... AS ENUM` is read with its values in order. `ALTER TYPE ... ADD VALUE` in a schema file is warned about and dropped: the desired values are the ones that `CREATE TYPE` lists.

### Changes

| Change | DDL |
|---|---|
| Added | `CREATE TYPE ... AS ENUM` |
| Removed | `DROP TYPE`, gated by `enum` |
| Value added | `ALTER TYPE ... ADD VALUE`, with `AFTER` or `BEFORE` to keep the file's order |
| Value renamed | `ALTER TYPE ... RENAME VALUE`, via the directive on the value |
| Renamed | `ALTER TYPE ... RENAME TO` |

PostgreSQL cannot remove a value from an enum or reorder its values. Either is an error at plan time:

```
pista: error: cannot remove enum value 'inactive' from public.status: PostgreSQL does not support removing enum values
```

A rename of the type is carried into the columns, composite attributes and domains that use it.

## Domains

### What is read

`CREATE DOMAIN` is read with the base type, `COLLATE`, `DEFAULT`, `NOT NULL` and `CHECK` constraints. `ALTER DOMAIN ... ADD CONSTRAINT ... CHECK` after the `CREATE DOMAIN` is read too, which is how `dump` writes a `NOT VALID` constraint. Every other `ALTER DOMAIN` is warned about and dropped. A `CHECK` without a name is named `<domain>_check`.

### Changes

| Change | DDL |
|---|---|
| Added | `CREATE DOMAIN`, then `ALTER DOMAIN ... ADD CONSTRAINT ... NOT VALID` for each such constraint |
| Removed | `DROP DOMAIN`, gated by `domain` |
| Default | `ALTER DOMAIN ... SET DEFAULT` or `DROP DEFAULT` |
| `NOT NULL` | `ALTER DOMAIN ... SET NOT NULL` or `DROP NOT NULL` |
| Constraint added or changed | `ALTER DOMAIN ... ADD CONSTRAINT`, after a `DROP CONSTRAINT` for a changed one |
| Constraint removed | `ALTER DOMAIN ... DROP CONSTRAINT`, not gated |
| `NOT VALID` to validated | `ALTER DOMAIN ... VALIDATE CONSTRAINT` |
| Renamed | `ALTER DOMAIN ... RENAME TO` |

PostgreSQL cannot change a domain's base type or collation. Either is an error at plan time:

```
pista: error: cannot change base type of domain public.email from text to character varying: PostgreSQL does not support this
```

pistachio does not rename a domain constraint, so a renamed one is dropped and added. `--assume-validated` reaches domain constraints as it does table constraints.

## Composite types

### What is read

`CREATE TYPE ... AS (...)` is read with each attribute's name, type and `COLLATE`. `ALTER TYPE ... ADD / DROP / ALTER / RENAME ATTRIBUTE` in a schema file is warned about and dropped.

### Changes

Attributes are matched by name. PostgreSQL cannot reorder them, so their order is not compared.

| Change | DDL |
|---|---|
| Added | `CREATE TYPE ... AS (...)` |
| Removed | `DROP TYPE`, gated by `composite_type` |
| Attribute added | `ALTER TYPE ... ADD ATTRIBUTE` |
| Attribute removed | `ALTER TYPE ... DROP ATTRIBUTE`, gated by `composite_type` |
| Attribute type or collation | `ALTER TYPE ... ALTER ATTRIBUTE ... TYPE` |
| Attribute renamed | `ALTER TYPE ... RENAME ATTRIBUTE`, via the directive on the attribute |
| Renamed | `ALTER TYPE ... RENAME TO` |

`ALTER ATTRIBUTE ... TYPE` fails at apply while a table column uses the type. PostgreSQL does not allow it and `CASCADE` does not help.

## Sequences

Only standalone sequences are managed. A sequence that a `serial` or identity column owns, and one that is tied to a column with `OWNED BY`, is left out on both sides: `dump` does not write it, and the desired schema's copy of it plans nothing. Such a sequence is handled as part of its column.

### What is read

`CREATE SEQUENCE` and `CREATE UNLOGGED SEQUENCE` are read, with `AS`, `INCREMENT`, `MINVALUE`, `MAXVALUE`, `START`, `CACHE`, `CYCLE` and `OWNED BY`. An option that is left out is the default that PostgreSQL gives it, not an unmanaged one: removing `CACHE 20` from the file plans `CACHE 1`. Of `ALTER SEQUENCE`, only `OWNED BY` is read. Any other option is warned about and dropped.

### Changes

| Change | DDL |
|---|---|
| Added | `CREATE SEQUENCE` with every option written out |
| Removed | `DROP SEQUENCE`, gated by `sequence` |
| Options | one `ALTER SEQUENCE` naming the options that changed |
| `UNLOGGED` | `ALTER SEQUENCE ... SET UNLOGGED` or `SET LOGGED` |
| Renamed | `ALTER SEQUENCE ... RENAME TO` |

No `RESTART` is planned, so a change that puts the current value outside the new range fails at apply with the server's error. A sequence that a column default reads through `nextval` is created before that table and dropped after it, and a rename is carried into the default.

## Routines

Functions and procedures are managed only when `--manage-routine` is passed. Without it routines are not read, so a schema maintained with `-- pista:execute` keeps working and plan output is unchanged.

```bash
pista dump --manage-routine
pista plan --manage-routine schema.sql
pista apply --manage-routine schema.sql
```

### What is read

`CREATE FUNCTION` and `CREATE PROCEDURE` are read: the parameters with their modes, names, types and defaults, the return type, the language, the body, and the attributes `IMMUTABLE` / `STABLE` / `VOLATILE`, `STRICT`, `SECURITY DEFINER`, `LEAKPROOF`, `PARALLEL`, `COST`, `ROWS` and `SET`. A body is a string, a `BEGIN ATOMIC ... END` or `RETURN <expr>` block, or the object file and symbol of a C function.

A routine is identified by its name and its argument types, so an overload set is several independent objects:

```sql
CREATE FUNCTION public.normalize(e text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT lower(e) $$;

CREATE FUNCTION public.normalize(e text, keep_case boolean) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT CASE WHEN keep_case THEN e ELSE lower(e) END $$;
```

An attribute left at its default is not written back. PostgreSQL reports `VOLATILE`, `PARALLEL UNSAFE` and the default `COST` as absent, so a desired schema may spell them out or leave them off. Argument and return types are reported without their schema when `search_path` reaches them, the same as any other name that pistachio reads back. A desired schema may write a type in the routine's own schema either way.

### Changes

The body, the language and the attributes are compared, and a change is applied with `CREATE OR REPLACE`. The changes that PostgreSQL refuses in place run as `DROP` and `CREATE`, which needs `--allow-drop routine`:

- changing the return type, including the columns of a `RETURNS TABLE`
- renaming a parameter, or adding or removing an `OUT` parameter
- removing a parameter default
- turning a function into a procedure, or back

Without the flag the plan writes the `DROP` as `-- skipped:` and leaves the routine as it is.

Adding or removing an input parameter, `IN`, `INOUT` or `VARIADIC`, is not a change to the routine. The input types are its identity, so the new signature is a new routine and the old one is dropped, which needs `--allow-drop routine` as well. Without the flag the new routine is still created and the `DROP` of the old one is written as `-- skipped:`, so the database holds both overloads.

PostgreSQL refuses the `DROP` of a recreate while anything calls the routine: a `CHECK` constraint, a column default, a generated column, an index, a view, a policy, a trigger, a domain constraint or a `BEGIN ATOMIC` routine. A body written as a string records no dependency and does not block. `plan` fails and names them:

```
pista: error: cannot drop function public.f(integer): table constraint t_check on public.t depends on it
```

The routine is recreated before the tables change, so changing the dependent in the same plan does not help. Change it in one run and the routine in the next. A view or a `BEGIN ATOMIC` routine that the plan drops does not block, since both are dropped first. A pure removal is not checked and fails at apply when something still calls the routine.

### Order

A routine is created after the types that its signature names and before every table, because a `CHECK` constraint, a `GENERATED` expression, an index expression, a policy or a trigger can call one. A signature can name a table instead of a type, as `RETURNS SETOF <table>` does. Such a routine comes after that table, and before every other table where that does not form a cycle. Dropping runs the other way, dependents first.

That order means a `LANGUAGE sql` routine whose body reads a table created in the same run fails to apply, because PostgreSQL parses a SQL body at creation time. The same holds for one that calls a routine defined later, and for a `plpgsql` routine whose `DECLARE` uses a table's row type or `%TYPE`. Applying with `--pre-sql 'SET check_function_bodies = off'` skips that check. Marking the routine `-- pista:ignore` and creating it with `-- pista:execute` also works, and so does writing the body as `BEGIN ATOMIC`.

A body written as `BEGIN ATOMIC ... END` or `RETURN <expr>` is managed too. PostgreSQL parses such a body when it creates the routine, so the routine is ordered like a view: it is created after the tables, views and routines that its body names, and dropped before them. `dump` writes it after the views, with the body as PostgreSQL returns it:

```sql
CREATE OR REPLACE FUNCTION public.total()
    RETURNS bigint
    LANGUAGE sql
    STABLE
    BEGIN ATOMIC
        SELECT count(*) AS count FROM v;
    END;
```

PostgreSQL qualifies the names in the body, so `SELECT a FROM t` comes back as `SELECT t.a FROM t`. A body written differently from the dump is replaced on every plan. A `CHECK` constraint, column default, generated column, index expression or trigger `WHEN` condition that calls such a routine cannot be created in the same run as the routine. A policy on a new table can, since those policies run last; a policy on a table that exists cannot.

### Not managed

The following are left out on both sides, so `dump` does not write them and `plan` does not propose dropping them:

- aggregates and window functions
- a routine that an extension owns, and the constructors of a range type
- a routine carrying an option that pistachio does not read, `SUPPORT` and `TRANSFORM FOR TYPE` among them

A schema file that declares one of these gets the unsupported statement warning. `-- pista:renamed-from` on a routine is an error.

`SET <parameter> FROM CURRENT` is handled differently. It captures the session value at creation time, so the statement carries nothing to compare, but the database reports the resolved value and the routine is read back like any other. Such a routine is treated as `-- pista:ignore`: it is neither created, altered nor dropped, and its signature is listed under `-- ignored:`.

## Triggers

### What is read

`CREATE TRIGGER` and `CREATE CONSTRAINT TRIGGER` on a table or view declared earlier are read, and the enable state is read via `ALTER TABLE ... ENABLE / DISABLE / ENABLE ALWAYS / ENABLE REPLICA TRIGGER <name>` after the trigger. `CREATE TRIGGER` has no syntax for a disabled trigger, so a desired schema asks for one the way `dump` writes it:

```sql
ALTER TABLE public.events DISABLE TRIGGER events_stamp;
```

PostgreSQL rejects the statement on a view, so a view's triggers have no state to set. The `ALL` and `USER` forms name no single trigger and are warned about and dropped. Event triggers belong to the database rather than to a schema and are not managed, and neither is a trigger that an extension owns.

The function that a trigger calls is managed only with `--manage-routine`. See [Routines](#routines). Without that flag, write the function with `-- pista:execute-first` so it exists before the `CREATE TRIGGER` that references it runs:

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
| Removed | `DROP TRIGGER`, gated by `trigger` |
| Definition | `CREATE OR REPLACE TRIGGER` |
| Definition, constraint trigger on either side | `DROP TRIGGER` and `CREATE TRIGGER`, gated by `trigger` |
| State | `ALTER TABLE ... ENABLE / DISABLE ... TRIGGER` |
| Renamed | `ALTER TRIGGER ... ON ... RENAME TO` |

`CREATE OR REPLACE TRIGGER` takes a lighter lock than dropping and recreating. PostgreSQL has no `CREATE OR REPLACE CONSTRAINT TRIGGER`, so a change with a constraint trigger on either side is a drop and a create, and without `--allow-drop trigger` the trigger keeps its current definition. Both forms leave a trigger enabled, so pistachio re-applies any other state right after.

The function is compared by name alone. The catalog reports it without its schema when `search_path` reaches it, so `public.stamp()` and `stamp()` are one function, and so are two same-named functions in different schemas.

A trigger on a partitioned table is cloned onto every partition. pistachio reads and writes the parent's trigger alone, and PostgreSQL keeps the clones in step. A trigger created on a partition itself is managed like any other.

## Policies and row-level security

### What is read

`ALTER TABLE ... ENABLE / DISABLE / FORCE / NO FORCE ROW LEVEL SECURITY` and `CREATE POLICY` after the table are read, with `AS PERMISSIVE / RESTRICTIVE`, `FOR`, `TO`, `USING` and `WITH CHECK`. `ALTER POLICY` and `DROP POLICY` in a schema file are warned about and dropped.

### Changes

`USING` and `WITH CHECK` are compared as expressions, so a cast that the catalog adds or an `IN` list that it rewrites is not a change. The role list is compared as a set, and `TO PUBLIC` is the same as no list.

| Change | DDL |
|---|---|
| Row-level security | `ALTER TABLE ... ENABLE / DISABLE / FORCE / NO FORCE ROW LEVEL SECURITY` |
| Policy added | `CREATE POLICY` |
| Policy removed | `DROP POLICY`, gated by `policy` |
| Roles, `USING` or `WITH CHECK` changed or added | `ALTER POLICY ... TO ... USING (...) WITH CHECK (...)`, naming the parts that changed |
| Command or permissiveness changed, or a clause removed | `DROP POLICY` and `CREATE POLICY` |
| Renamed | `ALTER POLICY ... ON ... RENAME TO` |

`ALTER POLICY` cannot change a policy's command or permissiveness, or take a `USING` or `WITH CHECK` clause away, so those changes recreate the policy. That pair is not gated.

The row-level security statements come before the policy statements of their table. The policies of a new table run at the end of the plan, after the views, so one can read a view created in the same run. The policies of a table that exists run with the table's own statements and cannot.

## Comments

`COMMENT ON` is read for tables, views, materialized views, columns of all three, composite attributes, indexes, constraints, foreign keys, triggers, policies, enum and composite types, domains, domain constraints, sequences, functions and procedures. Any other target, `SCHEMA` for one, is warned about and dropped.

`dump` writes a comment after the object that it belongs to. A comment that the schema file drops is cleared with `COMMENT ON ... IS NULL`, and `IS ''` in a file means the same as `IS NULL`.

A comment on a constraint, a foreign key, a trigger or a policy names the table that it is on. A comment on a domain constraint names the domain. `COMMENT ON INDEX` names the index alone, which PostgreSQL keeps unique within a schema:

```sql
COMMENT ON CONSTRAINT users_email_key ON public.users IS 'One account per address';
COMMENT ON TRIGGER users_touch ON public.users IS 'Sets updated_at';
COMMENT ON POLICY users_own ON public.users IS 'Own rows only';
COMMENT ON CONSTRAINT email_check ON DOMAIN public.email IS 'Has an at sign';
COMMENT ON INDEX public.users_email_idx IS 'Lookup by email';
COMMENT ON FUNCTION public.normalize(text) IS 'v1';
```

A `COMMENT ON FUNCTION` written without an argument list matches when one routine has the name. When an object is dropped and created again, the plan writes its comment again. A rename, `CREATE OR REPLACE VIEW`, `CREATE OR REPLACE TRIGGER`, `ALTER POLICY`, `ALTER CONSTRAINT` and `VALIDATE CONSTRAINT` keep the comment.

A comment on an object that the schema file does not declare before it is ignored without a warning. That includes a comment written above its `CREATE`, a comment on an inherited column of an `INHERITS` child, a comment on the index that a `PRIMARY KEY`, `UNIQUE` or `EXCLUDE` constraint owns, which is written with `COMMENT ON CONSTRAINT` instead, and a comment on a `NOT NULL` constraint, which PostgreSQL 18 `pg_dump` writes and pistachio does not read as a constraint.

## Names

An unnamed constraint, `id integer PRIMARY KEY`, `name text UNIQUE` or `col integer REFERENCES other(id)`, takes the name that PostgreSQL would give it: `{table}_pkey`, and `{table}_{col}..._key`, `{table}_{col}..._fkey`, `{table}_{col}..._excl` joining every key column. A `UNIQUE` or `EXCLUDE` joins its `INCLUDE` columns as well, and an `EXCLUDE` element written as an expression is named the way an index element is, below. A `CHECK` becomes `{table}_{col}_check` when its expression references one column and `{table}_check` when it references none or several, which is what PostgreSQL does even for a constraint written on a column.

An index written without a name, `CREATE INDEX ON users (name)`, is named `{table}_{col}..._idx`, joining every index element including the `INCLUDE` list. An element written as an expression takes the name that PostgreSQL gives it: the function that it calls, so `(lower(name))` gives `users_lower_idx`, the field of a field selection, the column under a subscript, the type of a cast that has nothing under it, or `expr` when it has none of its own.

A name that does not fit in 63 bytes is shortened the way PostgreSQL shortens it, by trimming the table and column parts and keeping the trailing label.

When a generated name meets a name already in use, whether generated or explicit, PostgreSQL appends a number with no separator, `users_id_check1` or `users_name_idx1`, that pistachio cannot predict, so such a file is rejected as a duplicate name. Write explicit `CONSTRAINT <name>` clauses and index names where that happens.
