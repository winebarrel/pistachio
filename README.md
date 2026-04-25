# pistachio

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)

Declarative schema management tool for PostgreSQL. Define your desired schema in SQL and let `pistachio` figure out the diff.

See also: [Getting Started Guide](getting-started.md)

<img width="800" alt="demo" src="https://github.com/user-attachments/assets/5c9ebc3c-28c8-4d18-af3f-04ffac6bd57a" />

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
                              ($PIST_CONN_STR)
      --password=STRING       PostgreSQL password ($PIST_PASSWORD).
  -n, --schemas=public,...    Schemas to inspect and modify ($PIST_SCHEMAS).
  -m, --schema-map=KEY=VALUE;...
                              Schema name mapping (e.g. -m old=new).
      --version

Commands:
  apply <files> ... [flags]
    Apply schema changes to the database.

  plan <files> ... [flags]
    Print the schema diff SQL without applying it.

  dump [flags]
    Dump the current database schema as SQL.

  fmt <files> ... [flags]
    Format SQL file(s) into canonical form.

Run "pist <command> --help" for more information on a command.
```

### plan

Compare schema file(s) against the current database and print the SQL needed to reconcile them.

```bash
pist plan schema.sql

# Multiple files
pist plan tables.sql views.sql

# Include pre-SQL in the output
pist plan schema.sql --pre-sql "SET search_path TO myschema;"
pist plan schema.sql --pre-sql-file pre.sql
```

### apply

Apply the diff to the database.

```bash
pist apply schema.sql

# Multiple files
pist apply tables.sql views.sql
```

Use `--pre-sql` or `--pre-sql-file` to run SQL before applying changes (mutually exclusive). Use `--with-tx` to wrap everything in a transaction.

```bash
# Inline SQL
pist apply schema.sql --pre-sql "SET search_path TO myschema;" --with-tx

# From file
pist apply schema.sql --pre-sql-file pre.sql --with-tx
```

Use `--index-concurrently` to generate `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` for **all** index operations. Also available as `$PIST_INDEX_CONCURRENTLY`.

```bash
pist plan --index-concurrently schema.sql
pist apply --index-concurrently schema.sql
```

To apply `CONCURRENTLY` to **individual** indexes, use the `-- pist:concurrently` directive before the `CREATE INDEX` statement:

```sql
-- pist:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);

-- This index will NOT use CONCURRENTLY
CREATE INDEX idx_users_email ON public.users USING btree (email);
```

> [!NOTE]
> When the generated diff includes `CREATE INDEX CONCURRENTLY` or `DROP INDEX CONCURRENTLY` (via `--index-concurrently` or `-- pist:concurrently`), `--with-tx` cannot be used because `CONCURRENTLY` operations cannot run inside a transaction. If there are no index changes, `--with-tx` is allowed even when the flag or directive is present.

By default, `plan` and `apply` do not drop tables, views, enums, domains, or columns. Use `--allow-drop` to enable dropping specific object types (`all`, `table`, `view`, `enum`, `domain`, `column`). Also available as `$PIST_ALLOW_DROP`.

```bash
# Allow all drops
pist plan --allow-drop all schema.sql

# Allow only column and table drops
pist apply --allow-drop column,table schema.sql
```

> [!NOTE]
> Constraints and indexes are always dropped when their definitions change or they are removed from the desired schema, regardless of `--allow-drop`. This is because PostgreSQL does not support `ALTER CONSTRAINT` or `ALTER INDEX` for definition changes — the only way to update them is DROP + ADD.

### Executing arbitrary SQL

Use `-- pist:execute` to include non-managed SQL (functions, triggers, grants) in your schema files. Add a check SQL to skip execution conditionally:

```sql
-- pist:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'my_func')
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ ... $$ LANGUAGE plpgsql;
```

See [Getting Started](getting-started.md) for details.

### dump

Dump the current database schema as SQL. Output can be used directly as a schema file.

```bash
pist dump
```

### fmt

Format SQL file(s) into the canonical form used by `dump`. Useful for normalizing hand-written schema files.

```bash
# Print formatted SQL to stdout
pist fmt schema.sql

# Format multiple files to stdout (combined)
pist fmt tables.sql views.sql

# Overwrite file(s) in place
pist fmt -w schema.sql

# Check if files are formatted (exit 1 if not, useful for CI)
pist fmt --check schema.sql
```

> [!NOTE]
> `dump` output uses PostgreSQL's own formatting (e.g. `pg_get_viewdef`), while `fmt` normalizes through the pg_query parser. This means `dump` output may not pass `fmt --check` directly. Run `fmt -w` once after the initial `dump` to normalize, then use `--check` in CI going forward.

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

### Filtering objects

Use `-I` / `--include` to include only matching objects by name, or `-E` / `--exclude` to exclude them. Patterns support `*` and `?` wildcards. Patterns match against object names only (not schema-qualified names).

Use `--enable` to restrict operations to specific object types, or `--disable` to exclude specific types. Valid types: `table`, `view`, `enum`, `domain`. Can be repeated. Also available as `$PIST_ENABLE` / `$PIST_DISABLE` environment variables.

These flags are available on the `dump`, `plan`, and `apply` subcommands.

```bash
# Dump only objects matching "user*"
pist dump -I 'user*'

# Plan changes excluding temporary tables
pist plan -E 'tmp_*' schema.sql

# Combine include and exclude
pist apply -I 'user*' -E 'user_tmp' schema.sql

# Dump only enums
pist dump --enable enum

# Dump only tables and views
pist dump --enable table,view

# Dump everything except views
pist dump --disable view

# Plan changes for enums only
pist plan --enable enum schema.sql

# Using environment variables
PIST_ENABLE=enum pist dump
PIST_DISABLE=view pist dump
```

> [!NOTE]
> `--enable` takes precedence over `--disable`. When `--enable` is set, only the specified types are included regardless of `--disable`. These flags may exclude dependent objects (e.g. `--enable table` omits enums/domains that table columns may reference), so use them primarily for inspection (`dump`, `plan`) rather than `apply`.

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

### Renaming objects

Use `-- pist:renamed-from <old_name>` directives to rename objects instead of dropping and recreating them.

**Tables, views, enums:**

```sql
-- pist:renamed-from public.old_status
CREATE TYPE public.new_status AS ENUM ('active', 'inactive');

-- pist:renamed-from public.old_users
CREATE TABLE public.users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

-- pist:renamed-from public.old_view
CREATE VIEW public.new_view AS SELECT 1;
```

**Columns, constraints, indexes** (inside `CREATE TABLE` or before `CREATE INDEX` / `ALTER TABLE ADD CONSTRAINT`):

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pist:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    -- pist:renamed-from users_name_key
    CONSTRAINT users_display_name_key UNIQUE (display_name)
);

-- pist:renamed-from idx_users_name
CREATE INDEX idx_users_display_name ON public.users (display_name);

-- pist:renamed-from fk_old_name
ALTER TABLE public.orders ADD CONSTRAINT fk_new_name FOREIGN KEY (user_id) REFERENCES public.users(id);
```

> [!TIP]
> Rename directives that have already been applied are silently skipped, so you can safely leave them in your schema files until cleanup.

### Split dump

Use `--split` to output each table/view/enum as a separate file in the specified directory.

```bash
pist dump --split ./schema/
# => ./schema/public.status.sql, ./schema/public.users.sql, ./schema/public.orders.sql, ...
```

## Example

Create a schema file:

```sql
CREATE TYPE public.status AS ENUM ('active', 'inactive');

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    status status NOT NULL,
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
pist plan schema.sql                  # review the diff (drops suppressed by default)
pist plan --allow-drop all schema.sql # review the diff (with drops)
pist apply schema.sql                 # apply it
```

Or split schema into multiple files and use them together:

```bash
pist dump --split ./schema/       # dump per table/view/enum
pist plan ./schema/*.sql          # review the diff
pist apply ./schema/*.sql         # apply it
```

> [!NOTE]
> Unnamed constraints (e.g. `id integer PRIMARY KEY`, `name text UNIQUE`, `col integer REFERENCES other(id)`) are auto-named by pistachio following PostgreSQL's naming convention (`{table}_pkey`, `{table}_{col}_key`, `{table}_{col}_check`, `{table}_{col}_fkey`, `{table}_{col}_excl`). However, pistachio's auto-naming has the following limitations:
> - When multiple constraints would generate the same name, PostgreSQL appends a numeric suffix (e.g. `_1`) that pistachio cannot predict.
> - PostgreSQL truncates identifier names to 63 bytes (NAMEDATALEN - 1). pistachio does not apply this truncation, so very long table/column names may produce mismatched constraint names.
>
> **It is strongly recommended to use explicit `CONSTRAINT <name>` clauses** to avoid these issues.

## Supported Objects

- Domain types (`CREATE DOMAIN`, `ALTER DOMAIN SET/DROP DEFAULT`, `SET/DROP NOT NULL`, `ADD/DROP CONSTRAINT`)
- Enum types (`CREATE TYPE ... AS ENUM`, `ALTER TYPE ... ADD VALUE`)
- Tables (including unlogged and partitioned tables)
- Views
- Materialized views
- Columns (serial/bigserial/smallserial, identity, generated)
- Constraints (primary key, unique, check, exclusion, foreign key)
- Indexes (unique, partial, expression, hash, multi-column)
- Comments (on tables, columns, views, types, domains)
- Renaming (tables, views, enums, domains, columns, constraints, foreign keys, indexes via `-- pist:renamed-from` directive)
- Array, JSON, UUID, and other built-in types
- Quoted identifiers

## Development

```bash
docker compose up -d
make test
```
