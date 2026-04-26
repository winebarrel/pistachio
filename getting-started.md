# Getting Started with pistachio

This guide walks you through setting up pistachio and managing your PostgreSQL schema declaratively.

## Prerequisites

- PostgreSQL database (local or remote)
- pistachio installed (see [README](README.md#installation) for installation options)

## Step 1: Connect to your database

pistachio connects to PostgreSQL using a connection string. The default is `postgres://postgres@localhost/postgres`.

```bash
# Use the default connection
pist dump

# Or specify a connection string
pist -c 'postgres://user:pass@host:5432/mydb' dump

# Or use an environment variable
export PIST_CONN_STR='postgres://user:pass@host:5432/mydb'
pist dump
```

## Step 2: Dump the current schema

Export your existing database schema to a SQL file:

```bash
pist dump > schema.sql
```

This produces a canonical SQL file with your tables, views, enums, indexes, constraints, and comments.

You can also split into one file per object:

```bash
pist dump --split ./schema/
```

## Step 3: Normalize with fmt

Format the dumped schema into a consistent canonical form:

```bash
pist fmt -w schema.sql
```

This ensures the file is in the exact format pistachio expects. From now on, `fmt --check` can be used in CI to enforce formatting:

```bash
pist fmt --check schema.sql
```

## Step 4: Make changes

Edit your schema file to add, modify, or remove objects. For example, add a new column:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    email text,               -- new column
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

## Step 5: Preview the diff

Use `plan` to see what SQL pistachio would execute without actually changing anything:

```bash
pist plan schema.sql
```

Output:

```sql
-- Plan for schema public (1 table, 0 views, 0 enums, 0 domains)
ALTER TABLE public.users ADD COLUMN email text;
```

## Step 6: Apply the changes

When you're happy with the plan, apply it:

```bash
pist apply schema.sql
```

Verify by running plan again:

```bash
pist plan schema.sql
# => -- Plan for schema public (1 table, 0 views, 0 enums, 0 domains)
# => -- No changes
```

## Step 7: Iterate

Repeat steps 4-6 as your schema evolves. Your schema file is always the source of truth.

## Common workflows

### Renaming objects

Use the `-- pist:renamed-from` directive to rename objects without dropping and recreating them:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pist:renamed-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

```bash
pist plan schema.sql
# => ALTER TABLE public.users RENAME COLUMN name TO display_name;
```

After applying, you can leave the directive in place (it will be silently skipped) or remove it.

### Working with specific schemas

By default, pistachio targets the `public` schema. Use `-n` or `$PIST_SCHEMAS` to specify a different schema:

```bash
# Dump the "myschema" schema
pist -n myschema dump

# Or use environment variable
export PIST_SCHEMAS=myschema
pist dump

# Plan/apply against "myschema"
pist -n myschema plan schema.sql
pist -n myschema apply schema.sql
```

You can also manage multiple schemas at once:

```bash
pist -n public,myschema dump
```

### Schema name mapping

Use `-m` / `--schema-map` when your SQL files use a different schema name than the actual database. This is common when you write SQL against `public` but deploy to a staging-specific schema:

```bash
# Dump "staging" schema but output as "public"
pist -n staging -m staging=public dump

# Plan/apply: SQL files use "public", but changes target "staging"
pist -n staging -m staging=public plan schema.sql
pist -n staging -m staging=public apply schema.sql
```

### Schema-less SQL files

If your SQL files omit schema names (e.g. `CREATE TABLE users` instead of `CREATE TABLE public.users`), pistachio uses the first schema from `-n` as the default:

```bash
# Schema-less SQL is treated as "myschema"
pist -n myschema plan schema.sql
pist -n myschema apply schema.sql
```

Use `--omit-schema` with dump to produce schema-less output:

```bash
pist dump --omit-schema > schema.sql
# => CREATE TABLE users (...) instead of CREATE TABLE public.users (...)
```

### Filtering objects

Focus on specific objects with `-I` (include) and `-E` (exclude):

```bash
pist dump -I 'user*'           # dump only user-related objects
pist plan -E 'tmp_*' schema.sql  # ignore temporary tables
```

Use `--enable` / `--disable` to filter by object type:

```bash
pist dump --enable enum              # dump only enums
pist dump --disable view             # dump everything except views
pist dump --enable table,enum        # dump tables and enums only
```

### Controlling drops

By default, `plan` and `apply` do **not** drop tables, views, enums, domains, columns, constraints, foreign keys, or indexes. Use `--allow-drop` to opt in:

```bash
# Allow all drops
pist plan --allow-drop all schema.sql
pist apply --allow-drop all schema.sql

# Allow only specific drop types (comma-separated or repeated)
pist apply --allow-drop column,table schema.sql

# Using environment variable
PIST_ALLOW_DROP=all pist plan schema.sql
```

Valid types: `all`, `table`, `view`, `enum`, `domain`, `column`, `constraint`, `foreign_key`, `index`. `constraint` covers CHECK / UNIQUE / PRIMARY KEY / EXCLUSION; foreign keys are governed by `foreign_key` separately.

> [!NOTE]
> `--allow-drop=constraint`, `--allow-drop=foreign_key`, and `--allow-drop=index` only govern **pure removals** (objects absent from the desired schema). **Definition changes** still execute as DROP + ADD regardless of `--allow-drop`, because PostgreSQL has no `ALTER CONSTRAINT` or `ALTER INDEX` for definition changes.

### Using transactions

Wrap apply in a transaction so all changes succeed or fail together:

```bash
pist apply schema.sql --with-tx
```

### Running pre-migration SQL

Execute SQL before applying schema changes (e.g. installing extensions). Use `--pre-sql` for inline SQL or `--pre-sql-file` for a file (mutually exclusive):

```bash
pist apply schema.sql --pre-sql "CREATE EXTENSION IF NOT EXISTS pgcrypto;" --with-tx
pist apply schema.sql --pre-sql-file pre.sql --with-tx
```

### Executing arbitrary SQL

Use the `-- pist:execute` directive to include SQL statements that pistachio doesn't manage declaratively (functions, triggers, grants, etc.). These are executed after schema changes during `apply`.

```sql
-- pist:execute
CREATE OR REPLACE FUNCTION public.update_timestamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

Add a check SQL after `-- pist:execute` to conditionally execute — the SQL runs only when the check returns `true`:

```sql
-- pist:execute SELECT NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_timestamp')
CREATE OR REPLACE FUNCTION public.update_timestamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

Execute statements appear in `plan` output and are preserved by `fmt`. During `apply`, the check SQL is evaluated and the statement is skipped if it returns `false`.

## CI integration

A typical CI pipeline:

```bash
# Check formatting
pist fmt --check schema.sql

# Verify no drift from database
pist plan schema.sql | grep -q "No changes"
```

## Tips

- Unnamed constraints are auto-named following PostgreSQL's convention, but pistachio does not currently emulate PostgreSQL's identifier truncation (63 bytes) or collision suffixing, so generated names may differ. **Use explicit `CONSTRAINT <name>` clauses** to avoid ambiguity.
- Run `pist plan` before `pist apply` to review changes.
- Keep your schema file(s) in version control alongside your application code.
