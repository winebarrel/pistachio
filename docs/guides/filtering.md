# Filtering what is managed

Use `-I` / `--include` to include only matching objects by name, or `-E` / `--exclude` to exclude them. Patterns support `*` and `?` wildcards. A pattern wrapped in slashes is a regular expression, which matches anywhere in the name unless it is anchored; a wildcard has to match the whole name. Patterns match against object names only (not schema-qualified names). Also available as `$PISTA_INCLUDE` / `$PISTA_EXCLUDE` environment variables.

Use `--enable` to restrict operations to specific object types, or `--disable` to exclude specific types. Valid types: `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`. Can be repeated. Also available as `$PISTA_ENABLE` / `$PISTA_DISABLE` environment variables.

These flags are available on the `dump`, `plan`, and `apply` subcommands.

```bash
# Dump only objects matching "user*"
pista dump -I 'user*'

# Plan changes excluding temporary tables
pista plan -E 'tmp_*' schema.sql

# Combine include and exclude
pista apply -I 'user*' -E 'user_tmp' schema.sql

# Exclude numbered partitions
pista plan -E '/^posts_\d+$/' schema.sql

# Mix the two forms
pista dump -I 'user*' -I '/^audit_(log|trail)$/'

# Dump only enums
pista dump --enable enum

# Dump only tables and views
pista dump --enable table,view

# Dump everything except views
pista dump --disable view

# Plan changes for enums only
pista plan --enable enum schema.sql

# Plan changes for routines only (--manage-routine is still required)
pista plan --manage-routine --enable routine schema.sql

# Using environment variables
PISTA_ENABLE=enum pista dump
PISTA_DISABLE=view pista dump
PISTA_INCLUDE='user*' pista dump
PISTA_EXCLUDE='tmp_*' pista plan schema.sql
```

!!! note
    `--enable` takes precedence over `--disable`. When `--enable` is set, only the specified types are included regardless of `--disable`. These flags may exclude dependent objects (e.g. `--enable table` omits enums/domains that table columns may reference); use them for inspection (`dump`, `plan`), not `apply`. `routine` narrows what `--manage-routine` turned on; it does not turn routines on by itself.

!!! note
    When both a CLI flag and its corresponding environment variable are set, the CLI flag overrides the environment variable (values are not merged). For example, running `PISTA_EXCLUDE='tmp_*' pista plan -E 'foo_*' schema.sql` excludes only `foo_*`; `tmp_*` is ignored.


## Ignoring objects

Use the `-- pista:ignore` directive to leave an object unmanaged. pistachio does not create, alter, or drop a `CREATE TABLE` / `CREATE TYPE ... AS ENUM` / `CREATE TYPE ... AS (...)` / `CREATE DOMAIN` / `CREATE VIEW` / `CREATE FUNCTION` / `CREATE PROCEDURE` marked with it. This is the in-file equivalent of `--exclude` for a single object, useful for a table managed by another tool or one whose definition intentionally drifts.

```sql
-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
```

Each ignored object is reported as an `-- ignored: <name>` comment in `plan` and `apply` output. The directive attaches to a statement in the schema file, so it can only ignore an object you have declared. To keep an existing object that would otherwise be dropped, write its `CREATE` statement with the directive.


## Skipping partition children

Use `--skip-partition-child` to manage a partitioned table without its partitions. `dump` writes the parent alone, and `plan` / `apply` neither create a partition the schema file declares nor drop one the database holds. Available on `dump`, `plan` and `apply`, and as `$PISTA_SKIP_PARTITION_CHILD`.

Use it where another tool creates the partitions, pg_partman for example. Without it, a schema file that declares the parent alone plans a `DROP TABLE` for every partition, and `-E` only reaches names that carry a pattern.

```bash
pista plan --skip-partition-child schema.sql
pista dump --skip-partition-child
```

A partition is skipped whether or not it is partitioned itself, so a sub-partitioned level goes with the leaves under it. The parent still carries changes down: PostgreSQL applies `ADD COLUMN` and an index created on a partitioned table to every partition. An `INHERITS` child is not a partition and stays managed.

