# Getting started

This page takes a database from its first dump to a first applied change. It needs a PostgreSQL server and pistachio. See [Installation](index.md#installation).

## Connect

Every command that reads a database takes a connection string. The default is `postgres://postgres@localhost/postgres`.

```bash
pista dump
pista dump -c 'postgres://user:pass@host:5432/mydb'

export PISTA_CONN_STR='postgres://user@host:5432/mydb'
export PISTA_PASSWORD='s3cret'
pista dump
```

Options can also come from a YAML file. See [Configuration](reference/configuration.md).

## Dump the schema

`dump` writes the current schema as SQL. That file is the starting point. When it is fed back to `plan`, it plans no changes.

```bash
pista dump > schema.sql
```

Every command targets the `public` schema unless `-n` names another. See [Working with multiple schemas](guides/multiple-schemas.md).

`--split` writes one file per object into a directory instead. See [`pista dump`](reference/commands/dump.md).

## Edit

Change the file so that it declares the schema as it should look. This example adds a new column:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

## Plan

`plan` prints the DDL that brings the database in line with the file, and runs nothing:

```bash
pista plan schema.sql
```

```sql
-- Connected to postgres://postgres@localhost:5432/postgres
-- Plan for schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
ALTER TABLE public.users ADD COLUMN email text;
```

A drop is not planned unless `--allow-drop` names its type. It is written as a `-- skipped:` comment. See [Controlling drops](guides/drops.md).

## Apply

`apply` computes the same plan and runs it:

```bash
pista apply schema.sql
```

```sql
-- Connected to postgres://postgres@localhost:5432/postgres
-- Apply to schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
ALTER TABLE public.users ADD COLUMN email text;
-- Apply finished in 12ms
```

A second `plan` now prints `-- No changes`. From here on, edit the file, run `plan`, and run `apply`. Keep the file in version control next to the application.

## Next

- [`pista plan`](reference/commands/plan.md) and [`pista apply`](reference/commands/apply.md) list every option.
- [Transactions and locks](guides/transactions.md) covers `--with-tx` and index builds that do not block writes.
- [Supported objects](reference/objects.md) says what pistachio reads and what each change becomes.
