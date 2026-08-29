# Getting Started with pistachio

This guide covers setup and basic schema management with pistachio.

## Prerequisites

- PostgreSQL database (local or remote)
- pistachio installed (see [Installation](index.md#installation) for installation options)

## Step 1: Connect to your database

pistachio connects to PostgreSQL using a connection string. The default is `postgres://postgres@localhost/postgres`.

```bash
# Use the default connection
pista dump

# Or specify a connection string
pista -c 'postgres://user:pass@host:5432/mydb' dump

# Or use an environment variable
export PISTA_CONN_STR='postgres://user:pass@host:5432/mydb'
pista dump
```

To keep credentials out of the connection string, pass the password separately via `--password` or `$PISTA_PASSWORD`:

```bash
export PISTA_CONN_STR='postgres://user@host:5432/mydb'
export PISTA_PASSWORD='s3cret'
pista dump
```

You can also put options in a YAML file and load it with `--config`. See [Configuration](reference/configuration.md).

```bash
pista --config pista.yml dump
```

## Step 2: Dump the current schema

Export your existing database schema to a SQL file:

```bash
pista dump > schema.sql
```

This produces a SQL file containing tables, views, enums, indexes, constraints, and comments.

You can also split into one file per object:

```bash
pista dump --split ./schema/
```

## Step 3: Make changes

Edit your schema file to add, modify, or remove objects. For example, add a new column:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text,               -- new column
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

## Step 4: Preview the diff

Use `plan` to see the SQL pistachio would execute without applying it:

```bash
pista plan schema.sql
```

Output:

```sql
-- Plan for schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
ALTER TABLE public.users ADD COLUMN email text;
```

## Step 5: Apply the changes

Apply the changes:

```bash
pista apply schema.sql
```

Output:

```sql
-- Apply to schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
ALTER TABLE public.users ADD COLUMN email text;
-- Apply finished in 12ms
```

The `-- Apply finished in ...` comment shows the apply phase duration (SQL
execution plus output writing). It is printed only when changes are applied,
not when there are no changes.

Verify by running plan again:

```bash
pista plan schema.sql
# => -- Plan for schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
# => -- No changes
```

## Step 6: Iterate

Repeat steps 3-5 as the schema changes. The schema file holds the authoritative definition.

## Tips

- Unnamed constraints are auto-named following PostgreSQL's convention, but pistachio does not emulate PostgreSQL's identifier truncation (63 bytes) or collision suffixing, so generated names may differ. Use explicit `CONSTRAINT <name>` clauses to avoid ambiguity.
- Run `pista plan` before `pista apply` to review changes.
- Keep schema files in version control alongside application code.
