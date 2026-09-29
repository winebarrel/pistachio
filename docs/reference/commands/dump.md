# pista dump

Write the database schema as SQL.

## Synopsis

```
pista dump [option...]
```

## Description

`pista dump` reads the schema from the database and writes it as SQL. The output is a schema file: fed back to [`pista plan`](plan.md), it plans no changes.

The SQL output opens with the connection and a count of what it found, then writes each object under a comment naming it. An index, a comment, a policy and a trigger are written with the table they belong to. Objects are ordered by type and then by name.

```sql
-- Connected to postgres://postgres@localhost:5432/postgres
-- Dump of schema public (1 table, 1 view, 1 enum, 0 domains, 0 composite types, 0 sequences)
-- public.status
CREATE TYPE public.status AS ENUM (
    'active',
    'inactive'
);

-- public.users
CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    state status,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX users_name_idx ON public.users USING btree (name);
COMMENT ON TABLE public.users IS 'app users';

-- public.active_users
CREATE OR REPLACE VIEW public.active_users AS
SELECT users.id,
    users.name
   FROM users
  WHERE users.state = 'active'::status;
```

`status` is written unqualified because the catalog reports an object that `search_path` reaches without its schema. `--search-path=` qualifies everything.

The output goes through the formatter [`pista fmt`](fmt.md) runs. `GRANT`, `CREATE EXTENSION` and roles are out of scope and are not written, so a dump loaded into an empty database restores the schema and not the privileges on it.

The connection is read-only by default. `--no-read-only` opens it read-write.

## Options

The [general options](index.md#general-options) apply as well.

### Connection

`-c` *connstr*, `--conn-string=`*connstr*
:   PostgreSQL connection string, in either form [libpq accepts](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING). Default: `postgres://postgres@localhost/postgres`. Environment: `PISTA_CONN_STR`.

`-d` *dbname*, `--dbname=`*dbname*
:   Database name. Overrides the one in the connection string. Environment: `PISTA_DBNAME`.

`--password=`*password*
:   Password, kept out of the connection string. Environment: `PISTA_PASSWORD`.

`--no-read-only`
:   Open the connection read-write. Environment: `PISTA_NO_READ_ONLY`.

### Scope

`-n` *schema*, `--schemas=`*schema*
:   Schemas to dump. Repeatable. Default: `public`. Environment: `PISTA_SCHEMAS`.

`-m` *old*`=`*new*, `--schema-map=`*old*`=`*new*
:   Write schema *old* as *new*. Repeatable, or several pairs separated by `;`.

`--search-path=`*path*
:   `search_path` for the connection. An object reachable through it is written without its schema. An empty value qualifies everything. Default: `public`. Environment: `PISTA_SEARCH_PATH`. See the [notes on `plan`](plan.md#notes).

`-I` *pattern*, `--include=`*pattern*
:   Dump only the objects whose name matches. `*` and `?` are wildcards and match the whole name; `/re/` is a regular expression and matches anywhere unless anchored. Repeatable. Environment: `PISTA_INCLUDE`.

`-E` *pattern*, `--exclude=`*pattern*
:   Leave out the objects whose name matches. Same patterns as `--include`. Repeatable. Environment: `PISTA_EXCLUDE`.

`--enable=`*type*
:   Dump only these object types: `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`. Repeatable. Takes precedence over `--disable`. Environment: `PISTA_ENABLE`.

`--disable=`*type*
:   Leave out these object types. Same values as `--enable`. Repeatable. Environment: `PISTA_DISABLE`.

`--manage-routine`
:   Dump functions and procedures. Off by default. Environment: `PISTA_MANAGE_ROUTINE`. See [Routines](../objects.md#routines).

`--manage-storage-param`
:   Write the storage parameters of tables and materialized views, the `WITH (...)` clause. Off by default. Environment: `PISTA_MANAGE_STORAGE_PARAM`. See [Storage parameters](../objects.md#storage-parameters).

`--skip-partition-child`
:   Write a partitioned table without its partitions. Environment: `PISTA_SKIP_PARTITION_CHILD`.

### Output

`--split=`*dir*
:   Write one file per table, view, enum, domain, composite type, sequence and routine into *dir*, named `<schema>.<name>.sql`, or `<name>.sql` with `--omit-schema`; two objects that give one file name, compared without case, overloaded routines for one, get a numbered suffix from the second on: `_2`, `_3` and so on. Standard output gets the header and `-- Wrote <n> file(s) to <dir>`. Conflicts with `--json`.

`--omit-schema`
:   Write every name without its schema, for a dump that is loaded into a schema of another name.

`--no-format`
:   Write the layout the model renders on its own, without the formatter. Conflicts with `--json`. Environment: `PISTA_NO_FORMAT`.

`--json`
:   Write JSON instead of SQL, in the shape [`pista parse`](parse.md) writes. No header is written. Conflicts with `--split`, `--no-format` and `--explain`. Environment: `PISTA_DUMP_JSON`. See [Parsing schema files](../../guides/parsing.md).

`--explain`
:   Write the size of each table and materialized view after its name in the header comment, and the size of each index in a comment above it. The sizes are the `pg_class` estimates `plan --explain` prints. Environment: `PISTA_DUMP_EXPLAIN`. See [Sizes in a dump](../../guides/explaining-plans.md#sizes-in-a-dump).

## Exit status

0 when the dump was written, 1 on error, 80 on a usage error.

## Environment

Every option names its variable above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment).

## Examples

To a file, and to one file per object:

```bash
pista dump > schema.sql
pista dump --split ./schema/
```

Another schema, with its routines and without its temporary tables:

```bash
pista dump -n myschema -E 'tmp_*' --manage-routine
```

Without schema names, for loading elsewhere:

```bash
pista dump --omit-schema
```

With sizes:

```bash
pista dump --explain
```

```sql
-- public.users (~2,000,000 rows, 210 MB, as of 2026-09-09)
CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
-- 43 MB
CREATE INDEX users_name_idx ON public.users USING btree (name);
```

## See also

[`pista plan`](plan.md), [`pista fmt`](fmt.md), [`pista parse`](parse.md), [Formatting schema files](../../guides/formatting.md), [Working with multiple schemas](../../guides/multiple-schemas.md)
