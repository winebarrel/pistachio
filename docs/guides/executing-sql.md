# Running arbitrary SQL

Use the `-- pista:execute` directive to include SQL statements that pistachio does not manage declaratively, for example grants and extensions. `apply` runs them after the schema changes. Functions and procedures can be managed declaratively instead. See [Routines](../reference/objects.md#routines).

```sql
-- pista:execute
CREATE OR REPLACE FUNCTION public.update_timestamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

To run the SQL conditionally, add a check SQL expression after `-- pista:execute`. The SQL runs only when the check returns `true`:

```sql
-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_timestamp')
CREATE OR REPLACE FUNCTION public.update_timestamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

`plan` evaluates the check and shows the statements that the check selects. `apply` evaluates the check again and runs the statements that it selects at that time.

## Check patterns

`plan` leaves out the statements that `apply` would skip. A common check skips the statement when the object already exists:

```sql
-- pista:execute SELECT to_regprocedure('public.my_func()') IS NULL
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ ... $$ LANGUAGE plpgsql;
```

To manage a function whose body changes over time, put a version tag in `COMMENT ON FUNCTION`. Run the statement only when the installed comment differs from the tag. Wrap the `CREATE` and the `COMMENT` in a `DO` block, so that they are a single statement:

```sql
-- pista:execute SELECT obj_description(to_regprocedure('public.get_user_count()'), 'pg_proc') IS DISTINCT FROM 'v1'
DO $do$ BEGIN
  CREATE OR REPLACE FUNCTION public.get_user_count() RETURNS bigint AS $body$
    SELECT count(*) FROM public.users;
  $body$ LANGUAGE sql;
  COMMENT ON FUNCTION public.get_user_count() IS 'v1';
END $do$;
```

When the body changes, update the tag in both places, for example `'v1'` to `'v2'`. The next `apply` runs it again.

`-- pista:execute` runs after the managed DDL. Use `-- pista:execute-first` when the managed DDL calls the function. A `CHECK` constraint, a `GENERATED` expression, an index expression or a policy can do that:

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

The check SQL is evaluated at the point where the statement runs. So an `execute-first` check sees the schema before the change, and an `execute` check sees it after the change.

With `plan --out`, the check is evaluated when the plan file is written. The file contains the statements that the check selected. Therefore both kinds of check see the schema before the change. A check that cannot be evaluated fails the plan, instead of being left to `apply`. See [Plan files](plan-files.md).

