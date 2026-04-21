# pistachio

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)

Declarative schema management tool for PostgreSQL. Define your desired schema in SQL and let `pistachio` figure out the diff.

<img width="800" alt="demo" src="https://github.com/user-attachments/assets/dad22b07-64d6-4064-a360-4d028944cd58" />

## Installation

### Homebrew

```bash
brew install winebarrel/pistachio/pistachio
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
  -m, --schema-map=KEY=VALUE;...
                              Schema name mapping (e.g. -m old=new).
  -I, --include=INCLUDE,...   Include only tables/views matching the pattern
                              (wildcard: *, ?).
  -E, --exclude=EXCLUDE,...   Exclude tables/views matching the pattern
                              (wildcard: *, ?).
      --version

Commands:
  apply <files> ... [flags]
    Apply schema changes to the database.

  plan <files> ... [flags]
    Print the schema diff SQL without applying it.

  dump [flags]
    Dump the current database schema as SQL.

Run "pist <command> --help" for more information on a command.
```

### plan

Compare schema file(s) against the current database and print the SQL needed to reconcile them.

```bash
pist plan schema.sql

# Multiple files
pist plan tables.sql views.sql

# Include pre-SQL in the output
pist plan schema.sql --pre-sql-file pre.sql
```

### apply

Apply the diff to the database.

```bash
pist apply schema.sql

# Multiple files
pist apply tables.sql views.sql
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

### Schema name mapping

Use `-m` / `--schema-map` to remap schema names. This is useful when you want to manage a database whose schema name differs from the one used in your SQL files.

For example, to dump a `staging` schema as if it were `public`:

```bash
pist -n staging -m staging=public dump
```

You can also use it with `plan` and `apply`. The desired SQL files use the mapped name (`public`), while the generated SQL targets the real database schema (`staging`):

```bash
# schema.sql uses "public" as the schema name
pist -n staging -m staging=public plan schema.sql
pist -n staging -m staging=public apply schema.sql
```

### Filtering tables/views

Use `-I` / `--include` to include only matching tables/views, or `-E` / `--exclude` to exclude them. Patterns support `*` and `?` wildcards. Patterns match against table/view names only (not schema-qualified names).

```bash
# Dump only tables/views matching "user*"
pist -I 'user*' dump

# Plan changes excluding temporary tables
pist -E 'tmp_*' plan schema.sql

# Combine include and exclude
pist -I 'user*' -E 'user_tmp' apply schema.sql
```

### Omit schema

Use `--omit-schema` to omit schema names from the dump output.

```bash
pist dump --omit-schema
# => CREATE TABLE users (...) instead of CREATE TABLE public.users (...)

pist dump --omit-schema --split ./schema/
# => ./schema/users.sql, ./schema/orders.sql, ...
```

When schema is omitted in SQL files, `plan` and `apply` use the schema specified by `-n`:

```bash
pist -n staging plan schema.sql   # schema-less SQL is treated as "staging"
pist -n staging apply schema.sql
```

### Split dump

Use `--split` to output each table/view as a separate file in the specified directory.

```bash
pist dump --split ./schema/
# => ./schema/public.users.sql, ./schema/public.orders.sql, ...
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

Or split schema into multiple files and use them together:

```bash
pist dump --split ./schema/       # dump per table/view
pist plan ./schema/*.sql          # review the diff
pist apply ./schema/*.sql         # apply it
```

> [!NOTE]
> Unnamed constraints (e.g. `id integer PRIMARY KEY`, `name text UNIQUE`, `col integer REFERENCES other(id)`) are not tracked by pistachio because PostgreSQL auto-generates their names at creation time, making them unpredictable from the SQL file alone. Use explicit `CONSTRAINT <name>` clauses to ensure constraints are managed correctly.

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
