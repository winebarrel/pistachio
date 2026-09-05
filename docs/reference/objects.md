# Supported objects

- Domain types (`CREATE DOMAIN`, `ALTER DOMAIN SET/DROP DEFAULT`, `SET/DROP NOT NULL`, `ADD/DROP/VALIDATE CONSTRAINT`)
- Enum types (`CREATE TYPE ... AS ENUM`, `ALTER TYPE ... ADD VALUE`)
- Composite types (`CREATE TYPE ... AS (...)`, `ALTER TYPE ... ADD/DROP/ALTER ATTRIBUTE`, `RENAME ATTRIBUTE`). Attributes are matched by name, so reordering them produces no diff (PostgreSQL cannot reorder attributes). `ALTER ATTRIBUTE ... TYPE` fails at apply while a table column uses the type; PostgreSQL does not allow it and `CASCADE` does not help.
- Sequences (`CREATE SEQUENCE`, `ALTER SEQUENCE`, `DROP SEQUENCE`, including unlogged sequences). Only standalone sequences are managed; sequences owned by a serial or identity column are handled as part of that column, not as separate objects.
- Tables (including unlogged and partitioned tables)
- Storage parameters, the `WITH (...)` clause. An index's parameters are always managed; a table's are opt-in with `--manage-storage-param`. See [Table storage parameters](#table-storage-parameters).
- Views
- Materialized views
- Columns (serial/bigserial/smallserial, identity, generated, TOAST storage and compression). An identity column's sequence options, the `( ... )` after `AS IDENTITY`, are managed; a change goes out as `ALTER TABLE ... ALTER COLUMN ... SET`. No `RESTART` is planned, the same as `ALTER SEQUENCE`, so a change that puts the sequence's current value outside the new range fails at apply with the server's error.
- Constraints (primary key, unique, check, exclusion, foreign key)
- Indexes (unique, partial, expression, hash, multi-column)
- Comments (on tables, columns, views, types, domains, composite types, composite attributes, sequences)
- Row-level security (`ALTER TABLE ... ENABLE/DISABLE/FORCE/NO FORCE ROW LEVEL SECURITY`, policies via `CREATE POLICY` / `ALTER POLICY` / `DROP POLICY`)
- Triggers (`CREATE TRIGGER`, `CREATE CONSTRAINT TRIGGER`, `INSTEAD OF` triggers on views, and the enable state via `ALTER TABLE ... ENABLE/DISABLE TRIGGER`); see [Triggers](#triggers)
- Routines (`CREATE FUNCTION`, `CREATE PROCEDURE`), opt-in with `--manage-routine`. An overload set is several objects, keyed by argument type. See [Routines](#routines).
- Renaming (tables, views, enums, enum values, domains, composite types, composite attributes, sequences, columns, constraints, foreign keys, indexes, policies, triggers via `-- pista:renamed-from` directive)
- Array, JSON, UUID, and other built-in types
- Quoted identifiers

pistachio parses only the statements above. It drops any other statement in a schema file, such as `SET`, `GRANT`, or `CREATE EXTENSION`, and prints a `pistachio: <file>:<line>:<column>: ignored unsupported statement:` warning to standard error for each one. The same warning covers the parts it does not read of a statement it does parse, such as the `ALTER TABLE ... ADD COLUMN` and `ALTER COLUMN ... SET DEFAULT` a `pg_dump` file carries, or `COMMENT ON INDEX`. The `ALTER COLUMN ... SET STORAGE` and `SET COMPRESSION` such a file carries are read. To keep an unsupported statement in the file and run it during `apply`, mark it with `-- pista:execute`, which also silences the warning. A `BEGIN` or `COMMIT` warning points at `--with-tx` and `--try-tx`, which wrap the apply in a transaction.


## Table storage parameters

A table's storage parameters, the `WITH (...)` clause, are managed only when `--manage-storage-param` is passed (also `$PISTA_MANAGE_STORAGE_PARAM`). They are off by default because the autovacuum settings a table is tuned with are usually set on the database, not written in the schema file.

```bash
pista dump --manage-storage-param
pista plan --manage-storage-param schema.sql
pista apply --manage-storage-param schema.sql
```

Without the flag the parameters are dropped from both sides of the diff: no `SET` or `RESET` is planned, the clause a schema file writes is left off the `CREATE TABLE`, and `dump` does not write one.

With it the schema file states every parameter the table is to have. A change goes out as `ALTER TABLE ... SET (...)`, with a `RESET` for the parameters the file no longer names; neither rewrites the table. A `toast.` parameter belongs to the TOAST relation. PostgreSQL creates that relation only for a table with a toastable column and discards the setting when there is none, so such a table re-plans it on every run. A partitioned table holds no parameter, and a partition does not inherit the parent's.

An index's parameters are part of its definition and are managed either way.

## Routines

Functions and procedures are managed only when `--manage-routine` is passed (also `$PISTA_MANAGE_ROUTINE`). Without it `pg_proc` is never read, so a schema maintained with `-- pista:execute` keeps working and plan output is unchanged.

```bash
pista dump --manage-routine
pista plan --manage-routine schema.sql
pista apply --manage-routine schema.sql
```

A routine is identified by its name and its argument types, so an overload set is several independent objects:

```sql
CREATE FUNCTION public.normalize(e text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT lower(e) $$;

CREATE FUNCTION public.normalize(e text, keep_case boolean) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT CASE WHEN keep_case THEN e ELSE lower(e) END $$;
```

The body, the language, and the attributes (`IMMUTABLE` / `STABLE` / `VOLATILE`, `STRICT`, `SECURITY DEFINER`, `LEAKPROOF`, `PARALLEL`, `COST`, `ROWS`, `SET`) are compared and applied with `CREATE OR REPLACE`. The changes PostgreSQL refuses in place run as `DROP` and `CREATE`, which needs `--allow-drop routine`:

- changing the return type, including the columns of a `RETURNS TABLE`
- renaming a parameter
- removing a parameter default
- turning a function into a procedure, or back

Adding or removing a parameter is not a modification either. The argument types are the identity, so the new signature is a new routine and the old one is dropped, which needs `--allow-drop routine` as well.

An attribute left at its default is not written back. PostgreSQL reports `VOLATILE`, `PARALLEL UNSAFE` and the default `COST` as absent, so a desired schema may spell them out or leave them off.

A routine is created after the types its signature names and before every table, because a `CHECK` constraint, a `GENERATED` expression, an index expression, a policy or a trigger can call one. A signature can name a relation instead of a type, as `RETURNS SETOF <table>` does; such a routine comes after its own relation and before every other one. Dropping runs the other way: views, then tables, then routines, then types.

!!! info
    That order means a `LANGUAGE sql` routine whose body reads a table created in the same run fails to apply, because PostgreSQL parses a SQL body at creation time. The same holds for one that calls a routine defined later. `plpgsql` is unaffected. Mark such a routine `-- pista:ignore` and create it with `-- pista:execute` instead.

Argument and return types are reported without their schema when `search_path` reaches them, the same as any other name pistachio reads back. A desired schema may write a type in the routine's own schema either way; the two spellings are one routine. A comment goes on the full signature:

```sql
COMMENT ON FUNCTION public.normalize(text) IS 'v1';
```

The following are not managed. Both sides of the diff leave them out, so `dump` does not write them and `plan` does not propose dropping them:

- aggregates and window functions
- a routine whose body is written in the SQL-standard `BEGIN ATOMIC` form. Such a body records real dependencies on the tables it reads, so it cannot be created ahead of them
- a routine an extension owns
- a routine carrying an option pistachio does not read, `SUPPORT` and `TRANSFORM FOR TYPE` among them
- renaming, via `-- pista:renamed-from`

An unmanaged routine is warned about and skipped, never an error, because the desired schema is read whether or not `--manage-routine` is set.

`SET <parameter> FROM CURRENT` is handled differently. It captures the session value at creation time, so the statement carries nothing to compare, but the database reports the resolved value and the routine is read back like any other. Such a routine is treated as `-- pista:ignore`: it is neither created, altered, nor dropped, and its signature is listed under `-- ignored:`.


## Triggers

pistachio manages triggers. The function a trigger calls is managed only with `--manage-routine`; see [Routines](#routines). Without that flag, write the function with `-- pista:execute-first` so it exists before the `CREATE TRIGGER` that references it runs:

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

A trigger names the table or view it sits on, so that relation has to be declared earlier in the file, or in an earlier file when the schema is split across several.

A definition change is applied with `CREATE OR REPLACE TRIGGER`, which takes a lighter lock than dropping and recreating. Turning a trigger into a constraint trigger, or back, is the one change PostgreSQL rejects in place; it runs as `DROP TRIGGER` and `CREATE TRIGGER` and needs `--allow-drop trigger`, the same as any other trigger drop. Without it the trigger keeps its current definition.

`CREATE TRIGGER` has no syntax for a disabled trigger, so a desired schema asks for one the way `dump` writes it:

```sql
ALTER TABLE public.events DISABLE TRIGGER events_stamp;
```

`ENABLE ALWAYS` and `ENABLE REPLICA` work the same way. PostgreSQL rejects the statement on a view, so a view's triggers have no state to set. Both `CREATE OR REPLACE TRIGGER` and a recreate leave a trigger enabled, so pistachio re-applies any other state right after.

The catalog reports a trigger's function without its schema when `search_path` reaches it, the same as any other name pistachio reads back. A desired schema that writes `public.stamp()` while the default `--search-path=public` is in effect therefore differs on every run; `--search-path=` keeps every schema and matches a `pg_dump`-style file.

A trigger on a partitioned table is cloned onto every partition. pistachio reads and writes the parent's trigger alone, and PostgreSQL keeps the clones in step.

The `ALL` and `USER` forms of `ENABLE TRIGGER` name no single trigger and are ignored, as are event triggers, which belong to the database rather than to a schema.


## Constraint and index naming

!!! info
    Unnamed constraints (e.g. `id integer PRIMARY KEY`, `name text UNIQUE`, `col integer REFERENCES other(id)`) are auto-named by pistachio following PostgreSQL's convention: `{table}_pkey`, and `{table}_{col}..._key`, `{table}_{col}..._fkey`, `{table}_{col}..._excl` joining every key column. A `CHECK` becomes `{table}_{col}_check` when its expression references one column and `{table}_check` when it references none or several, which is what PostgreSQL does even for a constraint written on a column.

    An index written without a name (e.g. `CREATE INDEX ON users (name)`) is named `{table}_{col}..._idx`, joining every index element including the `INCLUDE` list. An element written as an expression takes the name PostgreSQL gives it: the function it calls (`(lower(name))` gives `users_lower_idx`), the field of a field selection, the column under a subscript, the type of a cast that has nothing under it, or `expr` when it has none of its own.

    A name that does not fit in 63 bytes (NAMEDATALEN - 1) is shortened the way PostgreSQL shortens it, by trimming the table and column parts and keeping the trailing label.

    The auto-naming has one limitation: when a generated name meets a name already in use, whether generated or explicit, PostgreSQL appends a number with no separator (e.g. `users_id_check1`, `users_name_idx1`) that pistachio cannot predict, so such a file is rejected as a duplicate name.

    Use explicit `CONSTRAINT <name>` clauses and index names to avoid these issues.

