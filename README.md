# pistachio

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)

`pistachio` is a declarative schema management tool for PostgreSQL.

You describe the desired state of your database schema in SQL, and `pistachio` compares that declaration with the current database schema, then either:

- prints the SQL diff with `plan`
- applies the diff with `apply`
- dumps the current schema with `dump`

The CLI is intended for a schema-as-code workflow where the desired state is kept in a SQL file and the database is reconciled to match it.


## Install

```bash
go install github.com/winebarrel/pistachio/cmd/pist@latest
```

Or build from this repository:

```bash
go build -o pist ./cmd/pist
```

## Quick Start

Start PostgreSQL for local testing:

```bash
docker compose up -d
```

Create a desired schema file:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE TABLE public.posts (
    id integer NOT NULL,
    user_id integer NOT NULL,
    title text NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_posts_user_id ON public.posts USING btree (user_id);

ALTER TABLE ONLY public.posts
    ADD CONSTRAINT posts_user_id_fkey
    FOREIGN KEY (user_id) REFERENCES users(id);
```

Preview the changes:

```bash
pist plan schema.sql
```

Apply them:

```bash
pist apply schema.sql
```

Dump the current schema:

```bash
pist dump
```

## Commands

### `plan`

Prints the SQL needed to reconcile the current database schema with the declared desired schema.

```bash
pist plan schema.sql
```

Example output:

```sql
CREATE TABLE public.posts (
    id integer NOT NULL,
    user_id integer NOT NULL,
    title text NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_posts_user_id ON public.posts USING btree (user_id);
ALTER TABLE ONLY public.posts ADD CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES users (id);
```

### `apply`

Executes the diff SQL against the target database and prints each executed statement.

```bash
pist apply schema.sql
```

You can run SQL before the generated diff:

```bash
pist apply schema.sql --pre-sql-file pre.sql
```

You can also wrap the pre-SQL and generated statements in a transaction:

```bash
pist apply schema.sql --with-tx
```

### `dump`

Dumps the current schema as SQL.

```bash
pist dump
```

The output is suitable as a starting point for a declarative schema file.

## Options

Common options:

```text
-c, --conn-string    PostgreSQL connection string
    --password       PostgreSQL password
-n, --schemas        Schemas to inspect and modify
```

Environment variables:

- `DATABASE_URL`
- `PGPASSWORD`
- `PGSCHEMAS`

Defaults:

- connection string: `postgres://postgres@localhost/postgres`
- schemas: `public`

Examples:

```bash
DATABASE_URL=postgres://postgres:postgres@localhost/app pist plan schema.sql
PGSCHEMAS=public,tenant_a pist dump
```

## Supported Objects

Based on the current implementation and tests, `pistachio` supports diffing and dumping these PostgreSQL objects and features:

- tables
- views
- columns
- primary key, unique, check, and exclusion constraints
- foreign keys, including foreign key actions
- indexes, including unique, partial, expression, hash, and multi-column indexes
- comments on tables, columns, and views
- serial, smallserial, and bigserial-style schemas
- identity columns
- generated columns
- array, JSON, UUID, and various built-in types
- quoted identifiers
- unlogged tables
- partitioned tables

## How It Works

`pistachio` follows a declarative workflow:

- you define the desired schema in SQL
- `pistachio` reads and parses that declaration
- `pistachio` inspects the current PostgreSQL catalog
- it generates the SQL required to reconcile the current state with the declared state

In other words, you manage the target schema definition, and `pistachio` computes the migration steps needed to reach it.

The desired schema file is expected to contain declarative SQL such as:

- `CREATE TABLE`
- `CREATE VIEW`
- `CREATE INDEX`
- `ALTER TABLE ... ADD CONSTRAINT`
- `COMMENT ON ...`

## Testing

Start PostgreSQL first:

```bash
docker compose up -d
```

Then run:

```bash
make test
```
