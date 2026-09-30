# Directives

pistachio reads directives from SQL comments in schema files. A directive is a line comment of the form `-- pista:<name>`, with no space after the colon. It is placed on its own line before the target statement. Blank lines and further comments of either form may come between the two. So a `/* ... */` note above the statement does not separate the directive from it. A directive that follows code or a `/* ... */` comment on the same line is an error. So is a directive with no statement after it in the same file. A directive never binds to a statement in the next file. A directive written inside a `/* ... */` comment is commented out and does not apply. The parser still checks it, so a typo or a stray argument in it fails the parse instead of passing unnoticed. The parser rejects unknown directive names. A directive placed before a statement that it does not apply to is ignored.

| Directive | Arguments | Applies to | Purpose |
|---|---|---|---|
| `renamed-from` | old name (required) | tables, views, enums, enum values, domains, composite types, composite attributes, sequences, columns, constraints, foreign keys, indexes, policies, triggers | Renames the object instead of dropping and creating it. |
| `execute` | check SQL (optional) | any statement | Runs non-managed SQL after the managed DDL. |
| `execute-first` | check SQL (optional) | any statement | Runs non-managed SQL before the managed DDL. |
| `concurrently` | none | `CREATE INDEX` | Creates and drops the index with `CONCURRENTLY`. |
| `bulk-alter` | none | `CREATE TABLE` | Merges the table's `ALTER TABLE` actions into one statement. |
| `ignore` | none | tables, views, enums, domains, composite types, sequences, routines | Leaves the object unmanaged. |

## -- pista:renamed-from

Renames an object instead of dropping and recreating it. The argument is the old name. For tables, views, enums, domains, composite types and sequences, the old name may be schema-qualified. Without a schema, it is in the default schema. For composite attributes, columns, constraints, foreign keys, indexes, policies and triggers, the old name is unqualified. Routines cannot be renamed. The directive on a `CREATE FUNCTION` or `CREATE PROCEDURE` is an error.

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

For columns and constraints, write the directive inside `CREATE TABLE` on the line before the definition. pistachio silently skips a directive that has already been applied, so leave it in the file until cleanup.

For enum values, write the directive inside `CREATE TYPE ... AS ENUM` on the line before the value. The old value may be quoted or bare, and it is case-sensitive. The rename emits `ALTER TYPE ... RENAME VALUE`, which keeps stored data and the value's position.

```sql
CREATE TYPE public.status AS ENUM (
    'active',
    -- pista:renamed-from 'inactive'
    'disabled'
);
```

For composite types, the directive above the statement renames the type with `ALTER TYPE ... RENAME TO`. To rename an attribute, write the directive inside `CREATE TYPE ... AS (...)` on the line before the attribute. That emits `ALTER TYPE ... RENAME ATTRIBUTE`, which keeps stored data.

```sql
-- pista:renamed-from public.address
CREATE TYPE public.postal_address AS (
    -- pista:renamed-from street
    road text,
    city text
);
```

See [Renaming objects](../guides/renaming.md) for column rename caveats.

## -- pista:execute

Includes SQL that pistachio does not manage in schema files: a grant, an extension, or a function without `--manage-routine`. The marked statement is excluded from schema diffing. The optional argument is a check SQL expression. When it returns `true`, the statement runs. Otherwise the statement is skipped. Without a check, the statement always runs.

`plan` evaluates the check too, and leaves out the statements that `apply` would skip. So the plan shows what will run. Both commands run the check under the target schemas plus `public`, so an unqualified name in the check resolves to the same object in both.

Some checks cannot be answered at plan time. `plan` runs before the managed DDL, on a read-only connection. So a check that reads a table that the same run creates fails in `plan`, and so does a check that writes. Both work during `apply`. Such a statement remains in the plan with the reason recorded, and `apply` decides:

```sql
-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM public.audit_log)
-- check SQL could not be evaluated at plan time: ERROR: relation "public.audit_log" does not exist (SQLSTATE 42P01); apply will decide
INSERT INTO public.audit_log (id, note) VALUES (1, 'seed');
```

During `apply`, the check runs at its proper moment. A failure there is an error and stops the run.

```sql
-- pista:execute SELECT to_regprocedure('public.my_func()') IS NULL
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ ... $$ LANGUAGE plpgsql;
```

See [Running arbitrary SQL](../guides/executing-sql.md) for versioning patterns.

## -- pista:execute-first

Behaves as `execute` does, but runs the statement before the managed DDL instead of after it. Use it when the managed DDL calls a function that pistachio does not manage. A `CHECK` constraint, a `GENERATED` expression, an index expression or a policy can call one.

```sql
-- pista:execute-first SELECT to_regprocedure('public.lower_v(text)') IS NULL
CREATE OR REPLACE FUNCTION public.lower_v(t text) RETURNS text AS $$ SELECT lower(t) $$ LANGUAGE sql IMMUTABLE;

CREATE TABLE public.users (
    id integer NOT NULL,
    v text,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_v_check CHECK (lower_v(v) <> 'x')
);
```

The check SQL is evaluated where the statement runs. So an `execute-first` check sees the schema before the change, and an `execute` check sees it after. Put a check that tests for a table or column that the same run creates on `execute`. `plan` cannot answer it and shows the statement as undetermined, but `apply` decides correctly. An `execute-first` check gives the same answer in both commands, because both evaluate it against the schema before the change.

Statements keep their file order within each group. There is no dependency resolution between them.

Writing both `execute` and `execute-first` on one statement is an error, because the statement cannot run on both sides of the managed DDL. When the same directive is repeated, the last one applies.

## -- pista:concurrently

Opts an index into `CONCURRENTLY` for `CREATE INDEX` and `DROP INDEX`. Writing `CREATE INDEX CONCURRENTLY` in the statement means the same thing.

```sql
-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);
```

`--disable-index-concurrently` ignores all opt-ins. `--force-index-concurrently` applies `CONCURRENTLY` to every index change. `CONCURRENTLY` operations cannot run inside a transaction, so `apply --with-tx` refuses a plan that contains them. `apply --try-tx` runs such a plan without a transaction instead of failing.

## -- pista:bulk-alter

Combines the table's consecutive `ALTER TABLE` actions into one statement with comma-separated actions. That statement takes the table's lock once, and lets PostgreSQL plan the actions together. Tables without the directive keep one statement per action.

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

These remain separate statements:

- foreign keys
- `RENAME`
- `VALIDATE CONSTRAINT`
- RLS toggles
- storage parameter `SET` / `RESET`
- skipped DROPs

The `--bulk-alter` flag merges every table, with or without the directive.

## -- pista:ignore

Marks one of these statements as unmanaged:

- `CREATE TABLE`
- `CREATE TYPE ... AS ENUM`
- `CREATE TYPE ... AS (...)`
- `CREATE DOMAIN`
- `CREATE VIEW`, including materialized views
- `CREATE SEQUENCE`
- `CREATE FUNCTION`
- `CREATE PROCEDURE`

pistachio does not create, alter or drop the object. It removes the object from both the desired and the current state before diffing. This is the in-file equivalent of `--exclude` for a single object. It is useful for a table that another tool manages, or one whose definition drifts on purpose.

```sql
-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
```

Each ignored object is reported as an `-- ignored: <name>` comment in `plan` and `apply` output.

An ignored object still uses its name in the database, so it takes part in the duplicate-name check across object kinds.

The directive attaches to a statement written in the schema file, so it can only ignore an object that you have declared. To keep an existing object that would otherwise be dropped, write its `CREATE` statement with the directive. Because the object is unmanaged, the parser does not validate its column references.
