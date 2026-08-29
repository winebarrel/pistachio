# Running arbitrary SQL

Use the `-- pista:execute` directive to include SQL statements that pistachio doesn't manage declaratively (grants, extensions, etc.). These are executed after schema changes during `apply`. Functions and procedures can be managed declaratively instead, with `--manage-routine`.

```sql
-- pista:execute
CREATE OR REPLACE FUNCTION public.update_timestamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

Add a check SQL after `-- pista:execute` to conditionally execute. The SQL runs only when the check returns `true`:

```sql
-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_timestamp')
CREATE OR REPLACE FUNCTION public.update_timestamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

Execute statements appear in `plan` output. During `apply`, the check SQL is evaluated and the statement is skipped if it returns `false`.

## The check SQL

Functions and procedures can be managed declaratively instead; see
[Routines](../reference/objects.md#routines). The check SQL after the directive
is evaluated by both `plan` and `apply`. When it returns `true` the statement is
executed, otherwise skipped, and `plan` leaves out the statements `apply` would
skip. A common pattern skips when an object already exists:

```sql
-- pista:execute SELECT to_regprocedure('public.my_func()') IS NULL
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ ... $$ LANGUAGE plpgsql;
```

To manage a function whose body changes over time, embed a version tag in `COMMENT ON FUNCTION` and execute only when the installed comment differs. Wrap the `CREATE` and `COMMENT` in a `DO` block so they are a single statement:

```sql
-- pista:execute SELECT obj_description(to_regprocedure('public.get_user_count()'), 'pg_proc') IS DISTINCT FROM 'v1'
DO $do$ BEGIN
  CREATE OR REPLACE FUNCTION public.get_user_count() RETURNS bigint AS $body$
    SELECT count(*) FROM public.users;
  $body$ LANGUAGE sql;
  COMMENT ON FUNCTION public.get_user_count() IS 'v1';
END $do$;
```

When the body changes, update the tag in both places (e.g. `'v1'` -> `'v2'`); the next `apply` will re-run.

`-- pista:execute` runs after the managed DDL. Use `-- pista:execute-first` when the managed DDL calls the function, as a `CHECK` constraint, a `GENERATED` expression, an index expression, or a policy can:

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

The check SQL is evaluated where the statement runs, so an `execute-first` check sees the schema before the change and an `execute` check sees it after.

See [Getting Started](../getting-started.md) for details.

