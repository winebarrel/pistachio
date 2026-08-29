# Renaming objects

Use the `-- pista:renamed-from` directive to rename objects without dropping and recreating them:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

```bash
pista plan schema.sql
# => ALTER TABLE public.users RENAME COLUMN name TO display_name;
```

After applying, leave the directive in place (it is silently skipped) or remove it.

## Every object kind

**Tables, views, enums:**

```sql
-- pista:renamed-from public.old_status
CREATE TYPE public.new_status AS ENUM ('active', 'inactive');

-- pista:renamed-from public.old_users
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

-- pista:renamed-from public.old_view
CREATE VIEW public.new_view AS SELECT 1;
```

**Enum values** (inside `CREATE TYPE ... AS ENUM`, before the value). The rename emits `ALTER TYPE ... RENAME VALUE`, which keeps stored data and the value's position:

```sql
CREATE TYPE public.status AS ENUM (
    'active',
    -- pista:renamed-from 'inactive'
    'disabled'
);
```

**Composite types and their attributes**. The statement-level directive renames the type (`ALTER TYPE ... RENAME TO`). A directive inside `CREATE TYPE ... AS (...)`, before an attribute, renames that attribute (`ALTER TYPE ... RENAME ATTRIBUTE`) and keeps stored data:

```sql
-- pista:renamed-from public.address
CREATE TYPE public.postal_address AS (
    -- pista:renamed-from street
    road text,
    city text
);
```

**Columns, constraints, indexes** (inside `CREATE TABLE` or before `CREATE INDEX` / `ALTER TABLE ADD CONSTRAINT`):

```sql
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

> [!TIP]
> Rename directives that have already been applied are silently skipped. Leave them in place until cleanup.

### Column rename caveats

When a column is renamed, pistachio rewrites column references in same-table indexes, constraints, foreign keys (including `EXCLUDE`, partial / expression / `INCLUDE` indexes), triggers, policies and generated expressions on the current side, so a single `ALTER TABLE ... RENAME COLUMN` is emitted without redundant DDL on the dependents.

The desired-side SQL must use the new column name in those dependent definitions:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- Reference the new column name here:
CREATE INDEX idx_users_name ON public.users (display_name);
```

If the desired side still references the old name, `pista plan` errors out at parse time with a message like `column name referenced in index idx_users_name does not exist on table public.users` (identifiers are quoted only when they aren't safe unquoted). Index, constraint, foreign-key, `DEFAULT` and generated definitions are checked, and all such unresolved references are reported in a single error.

The following references are not auto-rewritten and may produce a redundant `DROP/CREATE` on the first plan (the second run after applying the rename is clean):

- View / materialized view definitions that `SELECT` the renamed column
- Foreign keys in other tables whose `REFERENCES this_table(renamed_col)` points at the renamed column
- Definitions on a partition child of the renamed table, which pistachio diffs on its own before the rewrite runs

