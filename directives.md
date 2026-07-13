# Directives

pistachio reads directives from SQL comments in schema files. A directive is a line comment of the form `-- pista:<name>` placed on its own line immediately before the target statement. Unknown directive names are rejected at parse time. A directive placed before a statement it does not apply to is ignored.

| Directive | Arguments | Applies to | Purpose |
|---|---|---|---|
| `renamed-from` | old name (required) | tables, views, enums, enum values, domains, columns, constraints, foreign keys, indexes, policies | Rename instead of drop and create |
| `execute` | check SQL (optional) | any statement | Run non-managed SQL during apply |
| `concurrently` | none | `CREATE INDEX` | Create and drop the index with `CONCURRENTLY` |
| `bulk-alter` | none | `CREATE TABLE` | Merge the table's `ALTER TABLE` actions into one statement |
| `ignore` | none | tables, views, enums, domains | Leave the object unmanaged |

## -- pista:renamed-from

Renames an object instead of dropping and recreating it. The argument is the old name. For tables, views, enums, and domains, the old name may be schema-qualified; without a schema it defaults to the default schema. For columns, constraints, foreign keys, indexes, and policies, the old name is unqualified.

```sql
-- pista:renamed-from public.old_users
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    -- pista:renamed-from users_name_key
    CONSTRAINT users_display_name_key UNIQUE (display_name)
);

-- pista:renamed-from idx_users_name
CREATE INDEX idx_users_display_name ON public.users (display_name);

-- pista:renamed-from fk_old_name
ALTER TABLE public.orders ADD CONSTRAINT fk_new_name FOREIGN KEY (user_id) REFERENCES public.users(id);
```

For columns and constraints, write the directive inside `CREATE TABLE` on the line before the definition. Directives that have already been applied are silently skipped, so leave them in place until cleanup.

For enum values, write the directive inside `CREATE TYPE ... AS ENUM` on the line before the value. The old value may be quoted or bare and is case-sensitive. The rename emits `ALTER TYPE ... RENAME VALUE`, which keeps stored data and the value's position.

```sql
CREATE TYPE public.status AS ENUM (
    'active',
    -- pista:renamed-from 'inactive'
    'disabled'
);
```

See [Renaming objects](README.md#renaming-objects) in the README for column rename caveats.

## -- pista:execute

Includes non-managed SQL (functions, triggers, grants) in schema files. The marked statement is excluded from schema diffing. The optional argument is a check SQL expression evaluated during `apply`: when it returns `true` the statement is executed, otherwise skipped. Without a check, the statement always runs.

```sql
-- pista:execute SELECT to_regprocedure('public.my_func()') IS NULL
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ ... $$ LANGUAGE plpgsql;
```

See [Executing arbitrary SQL](README.md#executing-arbitrary-sql) in the README for versioning patterns.

## -- pista:concurrently

Opts an index into `CONCURRENTLY` for `CREATE INDEX` and `DROP INDEX`. Writing `CREATE INDEX CONCURRENTLY` inline is equivalent.

```sql
-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);
```

`--disable-index-concurrently` ignores all opt-ins; `--force-index-concurrently` applies `CONCURRENTLY` to every index change. `CONCURRENTLY` operations cannot run inside a transaction, so a plan containing them conflicts with `apply --with-tx`.

## -- pista:bulk-alter

Combines the table's consecutive `ALTER TABLE` actions into a single statement with comma-separated actions. Tables without the directive keep one statement per action.

```sql
-- pista:bulk-alter
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

```sql
ALTER TABLE public.users
  ADD COLUMN email text,
  ALTER COLUMN name SET NOT NULL;
```

Foreign keys, `RENAME`, `VALIDATE CONSTRAINT`, RLS toggles, and skipped DROPs stay separate statements. The `--bulk-alter` flag merges every table regardless of directives.

## -- pista:ignore

Marks a `CREATE TABLE` / `CREATE TYPE ... AS ENUM` / `CREATE DOMAIN` / `CREATE VIEW` (including materialized views) as unmanaged. pistachio does not create, alter, or drop the object: it is dropped from both the desired and current state before diffing. This is the in-file equivalent of `--exclude` for a single object, useful for a table managed by another tool or one whose definition intentionally drifts.

```sql
-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
```

Each ignored object is reported as an `-- ignored: <name>` comment in `plan` and `apply` output.

The directive attaches to a statement written in the schema file, so it can only ignore an object you have declared. To keep an existing object that would otherwise be dropped, write its `CREATE` statement with the directive. Because the object is unmanaged, its column references are not validated at parse time.
