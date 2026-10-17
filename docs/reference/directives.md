# Directives

pistachio reads directives from SQL comments in schema files. A directive is a line comment of the form `-- pista:<name>`, with no space after the colon. It is placed on its own line before the target statement. Blank lines and further comments of either form may come between the two. So a `/* ... */` note above the statement does not separate the directive from it.

A directive that follows code or a `/* ... */` comment on the same line is an error. So is a directive with no statement after it in the same file. A directive never binds to a statement in the next file. A directive written inside a `/* ... */` comment is commented out and does not apply. The parser still checks it, so a typo or a stray argument in it fails the parse instead of passing unnoticed. The parser rejects unknown directive names. A directive placed before a statement that it does not apply to is ignored.

| Directive | Arguments | Applies to | Purpose |
|---|---|---|---|
| `renamed-from` | old name (required) | tables, views, enums, enum values, domains, composite types, composite attributes, sequences, columns, constraints, foreign keys, indexes, policies, triggers | Renames the object instead of dropping and creating it. |
| `retype-using` | expression (required) | columns | Gives the `USING` expression of a column type change. |
| `not-null-via-check` | none | columns | Adds `NOT NULL` to an existing column through a `CHECK` constraint. |
| `execute` | check SQL (optional) | any statement | Runs non-managed SQL after the managed DDL. |
| `execute-first` | check SQL (optional) | any statement | Runs non-managed SQL before the managed DDL. |
| `concurrently` | none | `CREATE INDEX` | Creates and drops the index with `CONCURRENTLY`. |
| `bulk-alter` | none | `CREATE TABLE` | Merges the table's `ALTER TABLE` actions into one statement. |
| `ignore` | none | tables, views, enums, domains, composite types, sequences, routines | Leaves the object unmanaged. |
| `lint-ignore` | rule names (optional; none means every rule), then ` --` and a reason (optional) | tables, columns, indexes, foreign keys | Turns lint rules off for the object. |
| `refuse-hazard` | kinds (required) | `CREATE TABLE` | Refuses the plan if it contains a statement of one of the kinds on the table. |

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

For columns and constraints, write the directive inside `CREATE TABLE` on the line before the definition. pistachio silently skips a directive that has already been applied, so leave it in the file until cleanup. `pista fmt --strip-renamed-from` removes them.

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

## -- pista:retype-using

Gives the `USING` expression for a column type change. The expression computes the new column value from the old one. pistachio adds it to `ALTER COLUMN ... SET DATA TYPE` without changing it. Write the directive inside `CREATE TABLE`, on the line before the column.

Without a `USING` expression, PostgreSQL converts the values with an assignment cast. If the two types have no implicit or assignment cast between them, the type change fails. `text` to an enum is an example.

```sql
CREATE TYPE public.item_status AS ENUM ('active', 'inactive');

CREATE TABLE public.items (
    id integer NOT NULL,
    -- pista:retype-using status::public.item_status
    status public.item_status DEFAULT 'active' NOT NULL,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);
```

If the column was `text`, the plan is:

```sql
ALTER TABLE public.items ALTER COLUMN status DROP DEFAULT;
ALTER TABLE public.items ALTER COLUMN status SET DATA TYPE public.item_status USING status::public.item_status;
ALTER TABLE public.items ALTER COLUMN status SET DEFAULT 'active';
```

PostgreSQL does not apply the `USING` expression to the column default. So if the column has a default, pistachio drops it before the type change and sets it again after. A serial column keeps its default. Indexes and constraints that use the column get no such handling, and the type change can still fail on them.

If the type does not change, the directive does nothing. You can leave it in the file after the change is applied.

## -- pista:not-null-via-check

Adds `NOT NULL` to an existing column in four statements instead of one `SET NOT NULL`. Write the directive inside `CREATE TABLE`, on the line before the column.

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:not-null-via-check
    email text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

If `email` was nullable, the plan is:

```sql
ALTER TABLE public.users ADD CONSTRAINT users_email_not_null_check CHECK (email IS NOT NULL) NOT VALID;
ALTER TABLE public.users VALIDATE CONSTRAINT users_email_not_null_check;
ALTER TABLE public.users ALTER COLUMN email SET NOT NULL;
ALTER TABLE public.users DROP CONSTRAINT users_email_not_null_check;
```

A plain `SET NOT NULL` reads every row under an ACCESS EXCLUSIVE lock, so even reads wait. Here `VALIDATE CONSTRAINT` reads the rows, and its lock does not block reads and writes. `SET NOT NULL` then sees the validated `CHECK` and does not read the rows. The other three statements take ACCESS EXCLUSIVE, but each ends quickly.

The `CHECK` constraint is named `<table>_<column>_not_null_check`. A name longer than 63 bytes is shortened the way PostgreSQL shortens a generated name. pistachio adds and drops the constraint itself, so the drop does not need `--allow-drop constraint`. If another constraint already has that name, or the desired schema declares one, the plan is a plain `SET NOT NULL`.

If a row holds NULL, `VALIDATE CONSTRAINT` fails and the `CHECK` stays as `NOT VALID`. Fix the rows and run `apply` again. The next plan starts at `VALIDATE CONSTRAINT`.

The statements must run outside a transaction. In a transaction, the lock that `ADD CONSTRAINT` takes is held until the end, and `VALIDATE CONSTRAINT` blocks reads and writes too. So `apply --with-tx` refuses the plan, and `apply --try-tx` runs it without a transaction. `--bulk-alter` keeps `SET NOT NULL` in a statement of its own.

`--not-null-via-check` does the same for every column. The directive does nothing when the column is new or already `NOT NULL`, so you can leave it in the file.

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

The check SQL is evaluated where the statement runs. So an `execute-first` check sees the schema before the change, and an `execute` check sees it after. Put a check that tests for a table or column that the same run creates on `execute`. `plan` cannot answer it and shows the statement as undetermined, but `apply` decides correctly. An `execute-first` check that `plan` can evaluate gives the same answer in both commands, because both evaluate it against the schema before the change.

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

## -- pista:lint-ignore

Turns off the named [lint rules](../guides/linting.md) for one object. Separate several names with commas or spaces. With no name, it turns off every rule for the object. A reason can follow after ` --`. Several directive lines before one object add up.

It applies to:

- the table, before `CREATE TABLE`
- the column, before a column definition inside `CREATE TABLE`, and a foreign key written on that column with `REFERENCES`
- the foreign key, before a `FOREIGN KEY` or `CONSTRAINT ... FOREIGN KEY` line inside `CREATE TABLE`
- the index, before `CREATE INDEX`
- each foreign key, before `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`

```sql
-- pista:lint-ignore require-primary-key
CREATE TABLE public.event_log (
    -- pista:lint-ignore prefer-timestamptz -- written by an external system
    at timestamp NOT NULL,
    -- pista:lint-ignore prefer-text -- written by an external system
    body varchar(200) NOT NULL
);
```

The directive applies to the object it is written before, and not to the objects on it. Only `pista lint` reads it. It has no effect on `plan` or `apply`, and `pista parse` does not write it. For a name that no rule of the kinds the directive reaches has, `pista lint` writes a warning to standard error. The exit status does not change.

## -- pista:refuse-hazard

Refuses the plan if it contains a statement of one of the given kinds on the table. The kinds are those of `--refuse-hazard`: `rewrite`, `scan`, `blocks-reads` and `blocks-writes`. Separate several kinds with commas or spaces. See [Refusing statements](../guides/explaining-plans.md#refusing-statements) for what each kind matches.

```sql
-- pista:refuse-hazard rewrite, blocks-reads
CREATE TABLE public.orders (
    id bigint NOT NULL,
    amount integer,
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
```

A statement is checked against every table its comment names. Adding a foreign key names both tables, so the directive also covers a foreign key that references the table. The directive on a partitioned table covers its partitions, and the directive on an `INHERITS` parent covers its children. The directive on a partition or a child also covers a statement on a table above it that reaches it, such as a column type change on the parent. `-I` and `-E` do not change which tables a directive covers. Statements on a table that the plan creates are not refused.

When `--refuse-hazard` is also given, a statement that either of them matches is refused. The error says which one refused the plan.

```sql
$ pista plan schema.sql
-- refused: rewrite, blocks reads and writes: public.orders (1 index rebuilt)
ALTER TABLE public.orders ALTER COLUMN amount SET DATA TYPE bigint;
pista: error: 1 statement refused by -- pista:refuse-hazard
```

An unknown kind is an error. The directive cannot be used with `--engine dsql`. `pista parse` does not write it.
