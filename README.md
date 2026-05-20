![](https://github.com/user-attachments/assets/6d48635e-1d93-4b4b-a7fc-e8f888780575#gh-light-mode-only)
![](https://github.com/user-attachments/assets/42250c31-d8cd-40a6-954d-77574e959a09#gh-dark-mode-only)

[![CI](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/pistachio/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/winebarrel/pistachio/branch/main/graph/badge.svg?token=lWmtTkDrbz)](https://codecov.io/gh/winebarrel/pistachio)

Declarative schema management tool for PostgreSQL. Define your desired schema in SQL and let `pistachio` figure out the diff.

See also: [Getting Started Guide](getting-started.md)

## Why pistachio?

Traditional migration tools make you hand-write and order an ever-growing
chain of `ALTER` scripts. With pistachio you keep a single source of truth —
your desired schema as plain SQL — and let the tool compute the DDL to get
there:

- **Declarative.** Edit your `CREATE TABLE` / `CREATE VIEW` definitions; no
  migration files to author or sequence.
- **Just SQL.** Schema files are ordinary PostgreSQL DDL (parsed via
  pg_query), and `pista dump` round-trips an existing database back into a
  schema file.
- **Safe by default.** Destructive changes (dropping tables, columns,
  constraints, …) are suppressed unless you opt in with `--allow-drop`, and
  surface as `-- skipped:` comments so nothing is dropped silently.
- **Production-aware.** `CONCURRENTLY` index builds, `--with-tx`, and
  `--pre-sql` / `lock_timeout` controls let you apply changes online without
  long-held locks.

> [!NOTE]
> **v1.7.0 breaking change:** the CLI binary was renamed from `pist` to `pista`, environment variables from `PIST_*` to `PISTA_*`, and SQL comment directives from `-- pist:` to `-- pista:`. Existing SQL files and shell / CI configurations must be updated before upgrading. See the [1.7.0 changelog entry](CHANGELOG.md#170---2026-05-12) for the full list of renamed names.

<img width="800" src="https://github.com/user-attachments/assets/eb961262-5c8e-459b-8461-ae089f87ae31" />

## Installation

### Homebrew

```bash
brew install winebarrel/pistachio/pistachio
```

### Download binary

Download the latest binary from [Releases](https://github.com/winebarrel/pistachio/releases).

## Try it out

A self-contained demo image bundles a local PostgreSQL with a sample
schema pre-loaded, so you can experiment with `pista` without
installing anything:

```bash
docker run --rm -it ghcr.io/winebarrel/pistachio-demo
```

The container drops you into a shell in `/demo` with `pista` and
`psql` preconfigured. Edit `desired.sql`, then try:

```bash
pista plan  desired.sql     # show the DDL diff
pista apply desired.sql     # apply the changes
pista plan  desired.sql     # ...should now print "No changes"
pista dump                  # dump the current schema
```

The source for the image is under [`demo/`](demo/).

## Usage

```
Usage: pista <command> [flags]

Flags:
  -h, --help                  Show context-sensitive help.
  -c, --conn-string="postgres://postgres@localhost/postgres"
                              PostgreSQL connection string. See
                              https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
                              ($PISTA_CONN_STR)
  -d, --dbname=STRING         PostgreSQL database name. Overrides the dbname in
                              --conn-string ($PISTA_DBNAME).
      --password=STRING       PostgreSQL password ($PISTA_PASSWORD).
  -n, --schemas=public,...    Schemas to inspect and modify ($PISTA_SCHEMAS).
  -m, --schema-map=KEY=VALUE;...
                              Schema name mapping (e.g. -m old=new).
      --version
      --[no-]pager            Force paging of long output via $PISTA_PAGER,
                              bypassing the TTY check. Use --no-pager to
                              disable. PISTA_PAGER must still be set.

Commands:
  apply <files> ... [flags]
    Apply schema changes to the database.

  plan <files> ... [flags]
    Print the schema diff SQL without applying it.

  dump [flags]
    Dump the current database schema as SQL.

Run "pista <command> --help" for more information on a command.
```

### plan

Compare schema file(s) against the current database and print the SQL needed to reconcile them.

```bash
pista plan schema.sql

# Multiple files
pista plan tables.sql views.sql

# Include pre-SQL in the output
pista plan schema.sql --pre-sql "SET statement_timeout = '5s';"
pista plan schema.sql --pre-sql-file pre.sql
```

`--pre-sql` / `--pre-sql-file` are also available as `$PISTA_PRE_SQL` / `$PISTA_PRE_SQL_FILE`.

### apply

Apply the diff to the database.

```bash
pista apply schema.sql

# Multiple files
pista apply tables.sql views.sql
```

Use `--pre-sql` or `--pre-sql-file` to run SQL before applying changes (mutually exclusive). Also available as `$PISTA_PRE_SQL` / `$PISTA_PRE_SQL_FILE`. Use `--with-tx` to wrap everything in a transaction (also available as `$PISTA_WITH_TX`).

```bash
# Inline SQL
pista apply schema.sql --pre-sql "SET statement_timeout = '5s';" --with-tx

# From file
pista apply schema.sql --pre-sql-file pre.sql --with-tx
```

To apply `CONCURRENTLY` to individual indexes, either write `CREATE INDEX CONCURRENTLY` directly or use the `-- pista:concurrently` directive before the `CREATE INDEX` statement. Both are treated equivalently:

```sql
-- pista:concurrently
CREATE INDEX idx_users_name ON public.users USING btree (name);

-- Equivalent: inline CONCURRENTLY
CREATE INDEX CONCURRENTLY idx_users_email ON public.users USING btree (email);

-- This index will NOT use CONCURRENTLY
CREATE INDEX idx_users_id ON public.users USING btree (id);
```

Use `--concurrently-pre-sql` (or `--concurrently-pre-sql-file`) to run SQL — typically `SET lock_timeout = '...'` — before any `CONCURRENTLY` index DDL. The SQL is only emitted/executed when the plan actually contains `CREATE/DROP INDEX CONCURRENTLY`, so it's safe to set unconditionally. Because `SET` is session-scoped and `CONCURRENTLY` runs outside a transaction, the value carries over to every subsequent `CONCURRENTLY` statement in the same `apply`. Also available as `$PISTA_CONCURRENTLY_PRE_SQL` / `$PISTA_CONCURRENTLY_PRE_SQL_FILE`.

```bash
pista apply schema.sql --concurrently-pre-sql "SET lock_timeout = '5s';"
```

Use `--disable-index-concurrently` to ignore all `CONCURRENTLY` opt-ins (both inline and directive) and emit plain `CREATE INDEX` / `DROP INDEX` instead. Useful when you want to keep the directives / inline `CONCURRENTLY` in your schema files but run a one-off plan/apply inside a transaction. Also available as `$PISTA_DISABLE_INDEX_CONCURRENTLY`.

```bash
pista plan --disable-index-concurrently schema.sql
pista apply --disable-index-concurrently --with-tx schema.sql
```

> [!NOTE]
> When the generated diff includes `CREATE INDEX CONCURRENTLY` or `DROP INDEX CONCURRENTLY`, `--with-tx` cannot be used because `CONCURRENTLY` operations cannot run inside a transaction. If there are no index changes, `--with-tx` is allowed even when an index is opted into `CONCURRENTLY`. To run `apply` inside a transaction in spite of the opt-in, combine `--with-tx` with `--disable-index-concurrently`.

Use `--bulk-alter` to combine consecutive `ALTER TABLE` actions on the same table into a single statement with comma-separated actions. This reduces the number of metadata locks acquired and lets PostgreSQL plan the changes together. Foreign keys, `RENAME`, `VALIDATE CONSTRAINT`, RLS toggles, and skipped DROPs are kept as separate statements. Also available as `$PISTA_BULK_ALTER`.

```bash
pista plan --bulk-alter schema.sql
pista apply --bulk-alter schema.sql
```

```sql
ALTER TABLE public.users
  ADD COLUMN email text,
  ALTER COLUMN name SET NOT NULL,
  DROP COLUMN legacy,
  ADD CONSTRAINT users_id_pos CHECK (id > 0);
```

By default, `plan` and `apply` do not drop tables, views, enums, domains, columns, constraints, foreign keys, or indexes. Use `--allow-drop` to enable dropping specific object types (`all`, `table`, `view`, `enum`, `domain`, `column`, `constraint`, `foreign_key`, `index`). Also available as `$PISTA_ALLOW_DROP`. `constraint` covers CHECK / UNIQUE / PRIMARY KEY / EXCLUSION; foreign keys are governed by `foreign_key` separately.

```bash
# Allow all drops
pista plan --allow-drop all schema.sql

# Allow only column and table drops
pista apply --allow-drop column,table schema.sql
```

Suppressed drops are emitted as commented-out DDL prefixed with `-- skipped:` so you can see what would be dropped without executing it. The plan still reports `-- No changes` when the only diff would be a suppressed drop, since no executable DDL is generated:

```sql
-- Plan for schema public (1 table, 0 views, 0 enums, 0 domains)
-- skipped: DROP TABLE public.legacy_users;
-- No changes
```

> [!NOTE]
> Only **pure removals** of constraints, foreign keys, and indexes (those absent from the desired schema) are governed by `--allow-drop=constraint` / `--allow-drop=foreign_key` / `--allow-drop=index`. **Definition changes** still execute regardless of `--allow-drop`: constraints and foreign keys as DROP + ADD, and indexes as DROP + CREATE, because PostgreSQL has no `ALTER CONSTRAINT` and no general `ALTER INDEX` form for definition changes.
>
> Foreign-key drops emitted because the owning table is being dropped follow the table-drop policy (not `foreign_key`): if the table drop is suppressed, the FK drop is suppressed too and surfaces as `-- skipped:` alongside the table.

### Executing arbitrary SQL

Use `-- pista:execute` to include non-managed SQL (functions, triggers, grants) in your schema files. The check SQL after the directive is evaluated during `apply` — when it returns `true` the statement is executed, otherwise it is skipped. The simplest form skips when an object already exists:

```sql
-- pista:execute SELECT to_regprocedure('public.my_func()') IS NULL
CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ ... $$ LANGUAGE plpgsql;
```

For idempotent management of a function whose body changes over time, embed a version tag in `COMMENT ON FUNCTION` and execute only when the installed comment differs. Wrap the `CREATE` and `COMMENT` in a `DO` block so they are a single statement:

```sql
-- pista:execute SELECT obj_description(to_regprocedure('public.get_user_count()'), 'pg_proc') IS DISTINCT FROM 'v1'
DO $do$ BEGIN
  CREATE OR REPLACE FUNCTION public.get_user_count() RETURNS bigint AS $body$
    SELECT count(*) FROM public.users;
  $body$ LANGUAGE sql;
  COMMENT ON FUNCTION public.get_user_count() IS 'v1';
END $do$;
```

When you change the body, bump the tag in both places (e.g. `'v1'` → `'v2'`); the next `apply` will re-run.

See [Getting Started](getting-started.md) for details.

### dump

Dump the current database schema as SQL. Output can be used directly as a schema file.

```bash
pista dump
```

### Paging long output

Set `$PISTA_PAGER` to forward `plan` / `apply` / `dump` output through an external command when stdout is a TTY. The command is interpreted by `sh -c`, so quoting and arguments work as in the shell. Pipes and redirects (`pista dump > file.sql`, `pista dump | grep ...`) are unaffected — the pager only kicks in for interactive use. Use `--no-pager` to disable it for a single invocation, or `--pager` to force it on when stdout is not a TTY (e.g. when piping into another pager-aware tool); `PISTA_PAGER` must still be set for `--pager` to do anything.

```bash
# Page with less, keeping ANSI colors
PISTA_PAGER='less -R' pista dump

# Pipe through a syntax highlighter that supports SQL
PISTA_PAGER='source-highlight -s sql -f esc | less -R' pista plan schema.sql

# One-off override
pista --no-pager plan schema.sql

# Force the pager even when stdout is not a TTY
PISTA_PAGER='source-highlight -s sql -f esc' pista --pager dump
```

### Schema name mapping

Use `-m` / `--schema-map` to remap schema names. This is useful when you want to manage a database whose schema name differs from the one used in your SQL files.

For example, to dump a `staging` schema as if it were `public`:

```bash
pista -n staging -m staging=public dump
```

You can also use it with `plan` and `apply`. The desired SQL files use the mapped name (`public`), while the generated SQL targets the real database schema (`staging`):

```bash
# schema.sql uses "public" as the schema name
pista -n staging -m staging=public plan schema.sql
pista -n staging -m staging=public apply schema.sql
```

### Filtering objects

Use `-I` / `--include` to include only matching objects by name, or `-E` / `--exclude` to exclude them. Patterns support `*` and `?` wildcards. Patterns match against object names only (not schema-qualified names). Also available as `$PISTA_INCLUDE` / `$PISTA_EXCLUDE` environment variables.

Use `--enable` to restrict operations to specific object types, or `--disable` to exclude specific types. Valid types: `table`, `view`, `enum`, `domain`. Can be repeated. Also available as `$PISTA_ENABLE` / `$PISTA_DISABLE` environment variables.

These flags are available on the `dump`, `plan`, and `apply` subcommands.

```bash
# Dump only objects matching "user*"
pista dump -I 'user*'

# Plan changes excluding temporary tables
pista plan -E 'tmp_*' schema.sql

# Combine include and exclude
pista apply -I 'user*' -E 'user_tmp' schema.sql

# Dump only enums
pista dump --enable enum

# Dump only tables and views
pista dump --enable table,view

# Dump everything except views
pista dump --disable view

# Plan changes for enums only
pista plan --enable enum schema.sql

# Using environment variables
PISTA_ENABLE=enum pista dump
PISTA_DISABLE=view pista dump
PISTA_INCLUDE='user*' pista dump
PISTA_EXCLUDE='tmp_*' pista plan schema.sql
```

> [!NOTE]
> `--enable` takes precedence over `--disable`. When `--enable` is set, only the specified types are included regardless of `--disable`. These flags may exclude dependent objects (e.g. `--enable table` omits enums/domains that table columns may reference), so use them primarily for inspection (`dump`, `plan`) rather than `apply`.

> [!NOTE]
> When both a CLI flag and its corresponding environment variable are set, the CLI flag overrides the environment variable (values are not merged). For example, running `PISTA_EXCLUDE='tmp_*' pista plan -E 'foo_*' schema.sql` excludes only `foo_*`; `tmp_*` is ignored.

### Omit schema

Use `--omit-schema` to omit schema names from the dump output.

```bash
pista dump --omit-schema
# => CREATE TABLE users (...) instead of CREATE TABLE public.users (...)

pista dump --omit-schema --split ./schema/
# -- Dump of schema public (2 tables, 0 views, 0 enums, 0 domains)
# -- Wrote 2 file(s) to ./schema/
# (writes ./schema/users.sql, ./schema/orders.sql, ...)
```

When schema is omitted in SQL files, `plan` and `apply` use the schema specified by `-n`:

```bash
pista -n staging plan schema.sql   # schema-less SQL is treated as "staging"
pista -n staging apply schema.sql
```

### Renaming objects

Use `-- pista:renamed-from <old_name>` directives to rename objects instead of dropping and recreating them.

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
> Rename directives that have already been applied are silently skipped, so you can safely leave them in your schema files until cleanup.

#### Column rename caveats

When a column is renamed, pistachio rewrites column references in **same-table** indexes, constraints, and foreign keys (including `EXCLUDE`, partial / expression / `INCLUDE` indexes) on the current side, so a single `ALTER TABLE ... RENAME COLUMN` is emitted without redundant `DROP/CREATE` on the dependents.

The desired-side SQL must use the **new** column name in those dependent definitions:

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

If the desired side still references the old name, `pista plan` errors out at parse time with a message like `column name referenced in index idx_users_name does not exist on table public.users` (identifiers are quoted only when they aren't safe unquoted). All such unresolved references are reported in a single error.

The following references are **not** auto-rewritten and may produce a redundant `DROP/CREATE` on the first plan (the second run after applying the rename comes out clean):

- View / materialized view definitions that `SELECT` the renamed column
- Foreign keys in *other* tables whose `REFERENCES this_table(renamed_col)` points at the renamed column

### Split dump

Use `--split` to output each table/view/enum/domain as a separate file in the specified directory.

```bash
pista dump --split ./schema/
# -- Dump of schema public (3 tables, 0 views, 1 enum, 0 domains)
# -- Wrote 4 file(s) to ./schema/
# (writes ./schema/public.status.sql, ./schema/public.users.sql, ./schema/public.orders.sql, ...)
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
pista plan schema.sql                  # review the diff (drops suppressed by default)
pista plan --allow-drop all schema.sql # review the diff (with drops)
pista apply schema.sql                 # apply it
```

Or split schema into multiple files and use them together:

```bash
pista dump --split ./schema/       # dump per table/view/enum/domain
pista plan ./schema/*.sql          # review the diff
pista apply ./schema/*.sql         # apply it
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
- Row-level security (`ALTER TABLE ... ENABLE/DISABLE/FORCE/NO FORCE ROW LEVEL SECURITY`, policies via `CREATE POLICY` / `ALTER POLICY` / `DROP POLICY`)
- Renaming (tables, views, enums, domains, columns, constraints, foreign keys, indexes, policies via `-- pista:renamed-from` directive)
- Array, JSON, UUID, and other built-in types
- Quoted identifiers

## Development

```bash
docker compose up -d
make test
```

## Related projects

- [myschema](https://github.com/winebarrel/myschema) — declarative
  schema management for MySQL.
- [ridgepole](https://github.com/ridgepole/ridgepole) — DB schema
  management using a Rails DSL.
