# pistachio

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)

Declarative schema management tool for PostgreSQL. Define your desired schema in SQL and let `pistachio` figure out the diff.

<img width="800" alt="demo" src="https://github.com/user-attachments/assets/0e8b93ea-6b52-468b-9d63-6d39ed8ca041" />

## Installation

### Homebrew

```bash
brew install winebarrel/pistachio/pist
```

### Download binary

Download the latest binary from [Releases](https://github.com/winebarrel/pistachio/releases).

## Usage

```
Usage: pist <command> [flags]

Flags:
  -h, --help                  Show context-sensitive help.
  -c, --conn-string="postgres://postgres@localhost/postgres"
                              PostgreSQL connection string. See
                              https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
                              ($DATABASE_URL)
      --password=STRING       PostgreSQL password ($PGPASSWORD).
  -n, --schemas=public,...    Schemas to inspect and modify ($PGSCHEMAS).
      --version

Commands:
  apply <file> [flags]
    Apply schema changes to the database.

  plan <file> [flags]
    Print the schema diff SQL without applying it.

  dump [flags]
    Dump the current database schema as SQL.

Run "pist <command> --help" for more information on a command.
```

### plan

Compare a schema file against the current database and print the SQL needed to reconcile them.

```bash
pist plan schema.sql
```

### apply

Apply the diff to the database.

```bash
pist apply schema.sql
```

Use `--pre-sql-file` to run SQL before applying changes. Use `--with-tx` to wrap everything in a transaction.

```bash
pist apply schema.sql --pre-sql-file pre.sql --with-tx
```

### dump

Dump the current database schema as SQL. Output can be used directly as a schema file.

```bash
pist dump
```

## Example

Create a schema file:

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

Preview and apply:

```bash
pist plan schema.sql   # review the diff
pist apply schema.sql  # apply it
```

## Supported Objects

- Tables (including unlogged and partitioned tables)
- Views
- Columns (serial/bigserial/smallserial, identity, generated)
- Constraints (primary key, unique, check, exclusion, foreign key)
- Indexes (unique, partial, expression, hash, multi-column)
- Comments (on tables, columns, views)
- Array, JSON, UUID, and other built-in types
- Quoted identifiers

## Development

```bash
docker compose up -d
make test
```