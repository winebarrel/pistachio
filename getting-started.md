# Getting Started with pistachio

This guide covers setup and basic schema management with pistachio.

## Prerequisites

- PostgreSQL database (local or remote)
- pistachio installed (see [README](README.md#installation) for installation options)

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
-- Plan for schema public (1 table, 0 views, 0 enums, 0 domains)
ALTER TABLE public.users ADD COLUMN email text;
```

## Step 5: Apply the changes

Apply the changes:

```bash
pista apply schema.sql
```

Output:

```sql
-- Apply to schema public (1 table, 0 views, 0 enums, 0 domains)
ALTER TABLE public.users ADD COLUMN email text;
-- Apply finished in 12ms
```

The `-- Apply finished in ...` comment shows the SQL statement execution
time. It is printed on every apply, and shows `0s` when there are no changes.

Verify by running plan again:

```bash
pista plan schema.sql
# => -- Plan for schema public (1 table, 0 views, 0 enums, 0 domains)
# => -- No changes
```

## Step 6: Iterate

Repeat steps 3-5 as the schema changes. The schema file holds the authoritative definition.

## Common workflows

### Renaming objects

Use the `-- pista:renamed-from` directive to rename objects without dropping and recreating them:

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

After applying, leave the directive in place (it is silently skipped) or remove it.

### Working with specific schemas

By default, pistachio targets the `public` schema. Use `-n` or `$PISTA_SCHEMAS` to specify a different schema:

```bash
# Dump the "myschema" schema
pista -n myschema dump

# Or use environment variable
export PISTA_SCHEMAS=myschema
pista dump

# Plan/apply against "myschema"
pista -n myschema plan schema.sql
pista -n myschema apply schema.sql
```

You can also manage multiple schemas at once:

```bash
pista -n public,myschema dump
```

### Schema name mapping

Use `-m` / `--schema-map` when SQL files use a different schema name than the database. This is common when SQL is written against `public` but deployed to a staging-specific schema:

```bash
# Dump "staging" schema but output as "public"
pista -n staging -m staging=public dump

# Plan/apply: SQL files use "public", but changes target "staging"
pista -n staging -m staging=public plan schema.sql
pista -n staging -m staging=public apply schema.sql
```

### Schema-less SQL files

If your SQL files omit schema names (e.g. `CREATE TABLE users` instead of `CREATE TABLE public.users`), pistachio uses the first schema from `-n` as the default:

```bash
# Schema-less SQL is treated as "myschema"
pista -n myschema plan schema.sql
pista -n myschema apply schema.sql
```

Use `--omit-schema` with dump to produce schema-less output:

```bash
pista dump --omit-schema > schema.sql
# => CREATE TABLE users (...) instead of CREATE TABLE public.users (...)
```

### Filtering objects

Filter by object name with `-I` (include) and `-E` (exclude):

```bash
pista dump -I 'user*'           # dump only user-related objects
pista plan -E 'tmp_*' schema.sql  # ignore temporary tables
```

Use `--enable` / `--disable` to filter by object type:

```bash
pista dump --enable enum              # dump only enums
pista dump --disable view             # dump everything except views
pista dump --enable table,enum        # dump tables and enums only
```

### Controlling drops

By default, `plan` and `apply` do **not** drop tables, views, enums, domains, columns, constraints, foreign keys, or indexes. Use `--allow-drop` to opt in:

```bash
# Allow all drops
pista plan --allow-drop all schema.sql
pista apply --allow-drop all schema.sql

# Allow only specific drop types (comma-separated or repeated)
pista apply --allow-drop column,table schema.sql

# Using environment variable
PISTA_ALLOW_DROP=all pista plan schema.sql
```

Valid types: `all`, `table`, `view`, `enum`, `domain`, `column`, `constraint`, `foreign_key`, `index`. `constraint` covers CHECK / UNIQUE / PRIMARY KEY / EXCLUSION; foreign keys are governed by `foreign_key` separately.

> [!NOTE]
> `--allow-drop=constraint`, `--allow-drop=foreign_key`, and `--allow-drop=index` only govern **pure removals** (objects absent from the desired schema). **Definition changes** still execute regardless of `--allow-drop`: constraints and foreign keys as DROP + ADD, and indexes as DROP + CREATE, because PostgreSQL has no `ALTER CONSTRAINT` and no general `ALTER INDEX` form for definition changes.

### Using transactions

Wrap apply in a transaction; all changes succeed or fail as a unit:

```bash
pista apply schema.sql --with-tx
```

### Running pre-migration SQL

Execute SQL before applying schema changes (e.g. setting a statement timeout). Use `--pre-sql` for inline SQL or `--pre-sql-file` for a file (mutually exclusive):

```bash
pista apply schema.sql --pre-sql "SET statement_timeout = '5s';" --with-tx
pista apply schema.sql --pre-sql-file pre.sql --with-tx
```

### Executing arbitrary SQL

Use the `-- pista:execute` directive to include SQL statements that pistachio doesn't manage declaratively (functions, triggers, grants, etc.). These are executed after schema changes during `apply`.

```sql
-- pista:execute
CREATE OR REPLACE FUNCTION public.update_timestamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

Add a check SQL after `-- pista:execute` to conditionally execute. The SQL runs only when the check returns `true`:

```sql
-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_timestamp')
CREATE OR REPLACE FUNCTION public.update_timestamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

Execute statements appear in `plan` output. During `apply`, the check SQL is evaluated and the statement is skipped if it returns `false`.

## CI integration

A typical CI pipeline:

```bash
# Verify no drift from database
pista plan schema.sql | grep -q "No changes"
```

## Tips

- Unnamed constraints are auto-named following PostgreSQL's convention, but pistachio does not emulate PostgreSQL's identifier truncation (63 bytes) or collision suffixing, so generated names may differ. Use explicit `CONSTRAINT <name>` clauses to avoid ambiguity.
- Run `pista plan` before `pista apply` to review changes.
- Keep schema files in version control alongside application code.
