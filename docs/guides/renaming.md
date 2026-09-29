# Renaming objects

Without help, a renamed object reads as one object dropped and another created, and the drop loses the data. The `-- pista:renamed-from` directive names the old name, and the plan renames instead:

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

After the apply the directive is skipped, since nothing carries the old name any more. Leave it in place or remove it at the next cleanup.

The directive renames tables, views, enums and their values, domains, composite types and their attributes, sequences, columns, constraints, foreign keys, indexes, policies and triggers. Routines cannot be renamed. Where the directive goes for each, and the statement it produces, is under [Directives](../reference/directives.md#-pistarenamed-from).

## Renaming a column

A column rename reaches the column's references on the same table: its indexes, constraints, foreign keys, triggers, policies and generated expressions are compared under the new name, so one `RENAME COLUMN` goes out and nothing else on the table changes.

The desired schema has to use the new name in those definitions:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_name ON public.users (display_name);
```

A definition that still names the old column fails at parse time, naming every such reference:

```
pista: error: column name referenced in index idx_users_name does not exist on table public.users
```

Three references are not rewritten and plan a redundant drop and create on the first run. PostgreSQL renames them itself, so the second run is clean:

- a view or materialized view that selects the column
- a foreign key on another table whose `REFERENCES` names the column
- an index, trigger or policy created on a partition of the table

## Renaming a table

A table rename goes out first in the plan, so the table's other changes run under the new name. The table's own indexes, foreign keys and triggers follow the rename. A view that reads the table and a foreign key that references it from another table are replanned once, as above.
