# pista dump

Write the database schema as SQL.

## Synopsis

```
pista dump [option...]
```

## Description

`pista dump` reads the schema from the database and writes it as SQL. The output is a schema file: fed back to [`pista plan`](plan.md), it plans no changes. See [The contract](../../about/design.md#the-contract).

The SQL output opens with the connection and a count of what it found, then writes each object under a comment naming it. An index, a comment, a policy and a trigger are written with the table that they belong to. Objects are ordered by type and then by name.

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

The output goes through the formatter that [`pista fmt`](fmt.md) runs. `GRANT`, `CREATE EXTENSION` and roles are out of scope and are not written, so a dump loaded into an empty database restores the schema and not the privileges on it.

The connection is read-only by default. `--no-read-only` opens it read-write.

## Options

The [general options](index.md#general-options) apply as well.

### Connection

`-c` *connstr*, `--conn-string=`*connstr*
:   The PostgreSQL connection string, in either form that [libpq accepts](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING). The default is `postgres://postgres@localhost/postgres`. The environment variable is `PISTA_CONN_STR`.

`-d` *dbname*, `--dbname=`*dbname*
:   The database name. It overrides the one in the connection string. The environment variable is `PISTA_DBNAME`.

`--password=`*password*
:   The password, kept out of the connection string. The environment variable is `PISTA_PASSWORD`.

`--no-read-only`
:   Open the connection read-write. The environment variable is `PISTA_NO_READ_ONLY`.

### Scope

`-n` *schema*, `--schemas=`*schema*
:   The schemas to dump. This option can be given more than once. The default is `public`. The environment variable is `PISTA_SCHEMAS`.

`-m` *old*`=`*new*, `--schema-map=`*old*`=`*new*
:   Write schema *old* as *new*. This option can be given more than once, or several pairs can be separated by `;`.

`--search-path=`*path*
:   The `search_path` for the connection. An object reachable through it is written without its schema. An empty value qualifies everything. The default is `public`. The environment variable is `PISTA_SEARCH_PATH`. See the [notes on `plan`](plan.md#notes).

`-I` *pattern*, `--include=`*pattern*
:   Dump only the objects whose name matches. `*` and `?` are wildcards and match the whole name. `/re/` is a regular expression and matches anywhere unless anchored. This option can be given more than once. The environment variable is `PISTA_INCLUDE`.

`-E` *pattern*, `--exclude=`*pattern*
:   Leave out the objects whose name matches. The patterns are the same as for `--include`. This option can be given more than once. The environment variable is `PISTA_EXCLUDE`.

`--enable=`*type*
:   Dump only these object types: `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`. This option can be given more than once. It takes precedence over `--disable`. The environment variable is `PISTA_ENABLE`.

`--disable=`*type*
:   Leave out these object types. The values are the same as for `--enable`. This option can be given more than once. The environment variable is `PISTA_DISABLE`.

`--manage-routine`
:   Dump functions and procedures. This option is off by default. The environment variable is `PISTA_MANAGE_ROUTINE`. See [Routines](../objects.md#routines).

`--manage-storage-param`
:   Write the storage parameters of tables and materialized views, the `WITH (...)` clause. This option is off by default. The environment variable is `PISTA_MANAGE_STORAGE_PARAM`. See [Storage parameters](../objects.md#storage-parameters).

`--skip-partition-child`
:   Write a partitioned table without its partitions. This option cannot be used with `--omit-partition-child-index`. The environment variable is `PISTA_SKIP_PARTITION_CHILD`.

### Output

`--split=`*dir*
:   Write one file per table, view, enum, domain, composite type, sequence and routine into *dir*, named `<schema>.<name>.sql`, or `<name>.sql` with `--omit-schema`. Two objects that give one file name, compared without case, overloaded routines for one, get a numbered suffix from the second on: `_2`, `_3` and so on. Standard output gets the header and `-- Wrote <n> file(s) to <dir>`. This option cannot be used with `--json`.

`--omit-schema`
:   Write every name without its schema, for a dump that is loaded into a schema of another name.

`--omit-partition-child-index`
:   Leave out a partition's copy of its parent's index, which PostgreSQL creates again when the dump is loaded. An index is taken as the copy when it is attached to the parent's index, has the name that PostgreSQL gives the copy and the parent index's definition and storage parameters, and has no comment. Any other partition index is written. This option cannot be used with `--skip-partition-child`. The environment variable is `PISTA_DUMP_OMIT_PARTITION_CHILD_INDEX`.

`--no-format`
:   Write the layout that the model renders on its own, without the formatter. This option cannot be used with `--json`. The environment variable is `PISTA_NO_FORMAT`.

`--json`
:   Write JSON instead of SQL, in the shape that [`pista parse`](parse.md) writes. No header is written. This option cannot be used with `--split`, `--no-format` or `--explain`. The environment variable is `PISTA_DUMP_JSON`. See [Parsing schema files](../../guides/parsing.md).

`--explain`
:   Write the size of each table and materialized view after its name in the header comment, and the size of each index in a comment above it. The sizes are the `pg_class` estimates that `plan --explain` prints. The environment variable is `PISTA_DUMP_EXPLAIN`. See [Sizes in a dump](../../guides/explaining-plans.md#sizes-in-a-dump).

## Exit status

The exit status is 0 when the dump was written, 1 on error, and 80 on a usage error.

## Environment

Every option names its variable above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment).

## Examples

Dump to a file, and to one file per object:

```bash
pista dump > schema.sql
pista dump --split ./schema/
```

Dump another schema, with its routines and without its temporary tables:

```bash
pista dump -n myschema -E 'tmp_*' --manage-routine
```

Dump without schema names, for loading elsewhere:

```bash
pista dump --omit-schema
```

Dump with sizes:

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
