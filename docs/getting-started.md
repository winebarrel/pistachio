# Getting started

This page takes a database from its first dump to a first applied change. It needs a PostgreSQL server and pistachio; see [Installation](index.md#installation).

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

`dump` writes the current schema as SQL. That file is the starting point: fed back to `plan`, it plans no changes.

```bash
pista dump > schema.sql
```

`--split` writes one file per object into a directory instead. See [`pista dump`](reference/commands/dump.md).

## Edit

Change the file the way the schema should look. A new column:

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
-- Plan for schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
ALTER TABLE public.users ADD COLUMN email text;
```

A drop is not planned unless `--allow-drop` names its type; it is written as a `-- skipped:` comment. See [Controlling drops](guides/drops.md).

## Apply

`apply` computes the same plan and runs it:

```bash
pista apply schema.sql
```

```sql
-- Apply to schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
ALTER TABLE public.users ADD COLUMN email text;
-- Apply finished in 12ms
```

A second `plan` now prints `-- No changes`. From here on, edit the file, plan, apply. Keep the file in version control next to the application.

## Next

- [`pista plan`](reference/commands/plan.md) and [`pista apply`](reference/commands/apply.md) list every option.
- [Transactions and locks](guides/transactions.md) covers `--with-tx` and index builds that do not block writes.
- [Supported objects](reference/objects.md) says what pistachio reads and what each change becomes.
