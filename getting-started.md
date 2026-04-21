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
# => -- No changes
```

## Step 7: Iterate

Repeat steps 4-6 as your schema evolves. Your schema file is always the source of truth.

## Common workflows

### Renaming objects

Use the `-- pist:rename-from` directive to rename objects without dropping and recreating them:

```sql
CREATE TABLE public.users (
    id integer NOT NULL,
    -- pist:rename-from name
    display_name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

```bash
pist plan schema.sql
# => ALTER TABLE public.users RENAME COLUMN name TO display_name;
```

After applying, you can leave the directive in place (it will be silently skipped) or remove it.

### Working with multiple schemas

Target a specific schema with `-n`:

```bash
pist -n myschema dump
pist -n myschema plan schema.sql
pist -n myschema apply schema.sql
```

### Filtering objects

Focus on specific objects with `-I` (include) and `-E` (exclude):

```bash
pist dump -I 'user*'           # dump only user-related objects
pist plan -E 'tmp_*' schema.sql  # ignore temporary tables
```

### Using transactions

Wrap apply in a transaction so all changes succeed or fail together:

```bash
pist apply schema.sql --with-tx
```

### Running pre-migration SQL

Execute SQL before applying schema changes (e.g. installing extensions):

```bash
pist apply schema.sql --pre-sql-file pre.sql --with-tx
```

## CI integration

A typical CI pipeline:

```bash
# Check formatting
pist fmt --check schema.sql

# Verify no drift from database
pist plan schema.sql | grep -q "No changes"
```

## Tips

- Always use explicit `CONSTRAINT <name>` clauses. Unnamed constraints are not tracked by pistachio.
- Run `pist plan` before `pist apply` to review changes.
- Use `--with-tx` for production applies so failures are rolled back.
- Keep your schema file(s) in version control alongside your application code.
