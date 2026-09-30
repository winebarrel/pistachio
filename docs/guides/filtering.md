# Filtering what is managed

The filters below apply to `dump`, `plan`, `apply` and `diff`. An object that they leave out is out of scope on both sides. It is not dumped, not created and not dropped.

## By name

`-I` / `--include` keeps the objects whose name matches. `-E` / `--exclude` leaves them out. `*` and `?` are wildcards, and a wildcard pattern must match the whole name. A pattern between slashes is a regular expression. It matches anywhere in the name unless it is anchored. Patterns match the name alone, without the schema.

```bash
pista dump -I 'user*'
pista plan -E 'tmp_*' schema.sql
pista apply -I 'user*' -E 'user_tmp' schema.sql
pista plan -E '/^posts_\d+$/' schema.sql
pista dump -I 'user*' -I '/^audit_(log|trail)$/'
```

## By object type

`--enable` keeps only the named types. `--disable` leaves them out. The types are `table`, `view`, `enum`, `domain`, `composite_type`, `sequence` and `routine`. When both are given, `--enable` wins.

```bash
pista dump --enable enum
pista dump --enable table,view
pista dump --disable view
pista plan --manage-routine --enable routine schema.sql
```

A type filter can leave out an object that another object depends on, for example an enum that a table column uses. So it suits `dump`, `plan` and `diff` better than `apply`. `routine` narrows what `--manage-routine` turned on. It does not turn routines on by itself.

Every filter has an environment variable: `PISTA_INCLUDE`, `PISTA_EXCLUDE`, `PISTA_ENABLE` and `PISTA_DISABLE`. A flag replaces the variable; it does not add to it. See [Environment variables](../reference/environment.md).

## Ignoring one object

The `-- pista:ignore` directive leaves one declared object unmanaged. Use it for a table that another tool owns, or for one that drifts on purpose:

```sql
-- pista:ignore
CREATE TABLE public.legacy (
    id integer NOT NULL,
    CONSTRAINT legacy_pkey PRIMARY KEY (id)
);
```

The object is not created, altered or dropped. The plan lists it under `-- ignored:`. The directive is the in-file form of `--exclude` for one object. It is also the way to keep an existing object that a plan would otherwise drop. See [Directives](../reference/directives.md#-pistaignore).

## Skipping partition children

`--skip-partition-child` manages a partitioned table without its partitions. `dump` writes the parent alone. `plan` / `apply` do not create a partition that the schema file declares, and do not drop one that the database has. The environment variable is `PISTA_SKIP_PARTITION_CHILD`.

```bash
pista plan --skip-partition-child schema.sql
pista dump --skip-partition-child
```

Use it where another tool creates the partitions, pg_partman for example. Without it, a schema file that declares only the parent produces a `DROP TABLE` for every partition, and `-E` can only match names that follow a pattern.

A partition is skipped whether or not it is partitioned itself. So a sub-partitioned level is skipped together with the leaves under it. Changes to the parent still reach the partitions: PostgreSQL applies `ADD COLUMN`, and an index created on a partitioned table, to every partition. An `INHERITS` child is not a partition, and remains managed.
