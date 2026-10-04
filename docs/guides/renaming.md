# Renaming objects

Without help, a renamed object is treated as one object dropped and another created. The drop loses the data. The `-- pista:renamed-from` directive gives the old name, and the plan renames the object instead:

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

After the apply, the directive is skipped, because nothing has the old name any more. Leave it as it is, or remove it at the next cleanup. `pista fmt --strip-renamed-from` removes every such directive.

The directive renames these objects:

- tables
- views
- enums and their values
- domains
- composite types and their attributes
- sequences
- columns
- constraints
- foreign keys
- indexes
- policies
- triggers

Routines cannot be renamed. Where the directive goes for each object is under [Directives](../reference/directives.md#-pistarenamed-from). The statement that each rename becomes is in the Renamed row of its kind under [Supported objects](../reference/objects.md).

## Renaming a column

A column rename also covers the references to the column on the same table. Its indexes, constraints, foreign keys, triggers, policies and generated expressions are compared under the new name. So the plan emits one `RENAME COLUMN`, and nothing else on the table changes.

The desired schema must use the new name in those definitions:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pista:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_name ON public.users (display_name);
```

pistachio checks index, constraint, foreign key, `DEFAULT` and generated definitions. If one of them still uses the old column name, parsing fails. The error lists every such reference:

```
pista: error: column name referenced in index idx_users_name does not exist on table public.users
```

Three kinds of reference are not rewritten. On the first run, the plan contains a redundant drop and create for them. PostgreSQL renames them itself, so the second run produces no changes:

- a view or materialized view that selects the column
- a foreign key on another table whose `REFERENCES` names the column
- an index, trigger or policy created on a partition of the table

## Renaming a table

A table rename is emitted before the table's other statements, so those statements run under the new name. The table's own indexes, foreign keys and triggers follow the rename. A view that reads the table, and a foreign key on another table that references it, are planned again once, as described above.
