# pista plan

Print the DDL that makes the database match the schema files.

## Synopsis

```
pista plan [option...] file...
```

## Description

`pista plan` reads the desired schema from the files and the current schema from the database. It prints the DDL that changes the current schema into the desired one. Nothing is applied. [`pista apply`](apply.md) runs the same DDL.

The files together form one schema. A statement that refers to another object, for example an `ALTER TABLE` or a `CREATE INDEX`, must come after the `CREATE` of that object. The `CREATE` can be in the same file or in an earlier one. See [Supported objects](../objects.md).

The output is SQL. It starts with the connection and a count of the objects. After that it contains:

- the pre-SQL,
- the concurrently-pre-SQL,
- the `-- pista:execute-first` statements,
- the DDL,
- the `-- pista:execute` statements,
- one `-- ignored:` comment for each object that a `-- pista:ignore` directive leaves out,
- one `-- skipped:` comment for each drop that `--allow-drop` does not allow.

An output with no executable DDL ends with `-- No changes`.

```sql
-- Connected to postgres://postgres@localhost:5432/postgres
-- Plan for schema public (2 tables, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
ALTER TABLE public.users ADD COLUMN email text;
-- skipped: DROP TABLE public.legacy_users;
```

The connection is read-only, so a `-- pista:execute` check that writes fails at plan time. `--no-read-only` opens a read-write connection.

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

These options decide what is read on both sides. `plan --out` records them in the plan file. `apply-from` reads the database with them.

`-n` *schema*, `--schemas=`*schema*
:   The schemas to inspect. This option can be given more than once. A name in the files that has no schema is qualified with the first one. The default is `public`. The environment variable is `PISTA_SCHEMAS`.

`-m` *old*`=`*new*, `--schema-map=`*old*`=`*new*
:   Read schema *old* from the database as *new*. Files written against *new* then apply to *old*. This option can be given more than once. Several pairs can also be separated by `;`.

`--search-path=`*path*
:   The `search_path` for the connection. The catalog reports an object that is reachable through the `search_path` without its schema. So this option decides which names are reported without a schema. An empty value qualifies every name. The default is `public`. The environment variable is `PISTA_SEARCH_PATH`. See [Notes](#notes).

`-I` *pattern*, `--include=`*pattern*
:   Manage only the objects whose name matches the pattern. `*` and `?` are wildcards. A wildcard pattern must match the whole name. `/re/` is a regular expression. It matches anywhere in the name unless it is anchored. The pattern is matched against the name alone, without the schema. This option can be given more than once. The environment variable is `PISTA_INCLUDE`.

`-E` *pattern*, `--exclude=`*pattern*
:   Leave out the objects whose name matches the pattern. The patterns are the same as for `--include`. This option can be given more than once. The environment variable is `PISTA_EXCLUDE`.

`--enable=`*type*
:   Manage only these object types: `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`. This option can be given more than once. It takes precedence over `--disable`. The environment variable is `PISTA_ENABLE`.

`--disable=`*type*
:   Leave out these object types. The values are the same as for `--enable`. This option can be given more than once. The environment variable is `PISTA_DISABLE`.

`--manage-routine`
:   Manage functions and procedures. This option is off by default. Dropping them still requires `--allow-drop routine`. The environment variable is `PISTA_MANAGE_ROUTINE`. See [Routines](../objects.md#routines).

`--manage-storage-param`
:   Manage the storage parameters of tables and materialized views, that is, the `WITH (...)` clause. This option is off by default. Without it, the clause is ignored on both sides. The `security_barrier` and `security_invoker` options of a plain view are managed in both cases. The environment variable is `PISTA_MANAGE_STORAGE_PARAM`. See [Storage parameters](../objects.md#storage-parameters).

`--skip-partition-child`
:   Manage a partitioned table without its partitions. An `INHERITS` child is not affected. The environment variable is `PISTA_SKIP_PARTITION_CHILD`. See [Skipping partition children](../../guides/filtering.md#skipping-partition-children).

### Statements

These options shape the DDL. `plan --out` writes the result into the plan file.

`--allow-drop=`*type*
:   Allow dropping these object types: `all`, `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`, `column`, `constraint`, `foreign_key`, `index`, `policy`, `trigger`. This option can be given more than once. Without it, no drop is planned. Each drop that is not allowed is written as a `-- skipped:` comment. `constraint` covers CHECK, UNIQUE, PRIMARY KEY and EXCLUSION constraints. `foreign_key` covers foreign keys. `DROP ATTRIBUTE` also requires `composite_type`. Recreating a view, a routine or a trigger also requires its type. The environment variable is `PISTA_ALLOW_DROP`. See [Controlling drops](../../guides/drops.md).

`--pre-sql=`*sql*
:   The SQL to write before the DDL. This option cannot be used with `--pre-sql-file`. The environment variable is `PISTA_PRE_SQL`.

`--pre-sql-file=`*file*
:   The file whose SQL is written before the DDL. The environment variable is `PISTA_PRE_SQL_FILE`.

`--concurrently-pre-sql=`*sql*
:   The SQL to write after the pre-SQL when the plan contains a `CONCURRENTLY` index statement, for example `SET lock_timeout`. This option cannot be used with `--concurrently-pre-sql-file`. The environment variable is `PISTA_CONCURRENTLY_PRE_SQL`.

`--concurrently-pre-sql-file=`*file*
:   The file whose SQL is written after the pre-SQL when the plan contains a `CONCURRENTLY` index statement. The environment variable is `PISTA_CONCURRENTLY_PRE_SQL_FILE`.

`--disable-index-concurrently`
:   Ignore every `CONCURRENTLY` opt-in, both the `-- pista:concurrently` directive and an inline `CREATE INDEX CONCURRENTLY`. Write plain `CREATE INDEX` and `DROP INDEX` instead. This option cannot be used with `--force-index-concurrently`. The environment variable is `PISTA_DISABLE_INDEX_CONCURRENTLY`.

`--force-index-concurrently`
:   Write `CONCURRENTLY` on every `CREATE INDEX` and `DROP INDEX`. This includes the drop of an index that the desired schema no longer contains, which no directive can reach. The environment variable is `PISTA_FORCE_INDEX_CONCURRENTLY`.

`--bulk-alter`
:   Merge consecutive `ALTER TABLE` actions on one table into one statement. Only column and constraint actions are merged. Foreign keys, `RENAME`, `VALIDATE CONSTRAINT`, row-level security toggles, storage parameters and skipped drops remain separate. The `-- pista:bulk-alter` directive does the same for one table. The environment variable is `PISTA_BULK_ALTER`.

`--assume-validated`
:   Treat every table constraint, domain constraint and foreign key as validated. `NOT VALID` in the desired schema is ignored. Neither `NOT VALID` nor `VALIDATE CONSTRAINT` is written. This option is for a schema in which `NOT VALID` was a migration step, not a desired state. The environment variable is `PISTA_ASSUME_VALIDATED`.

### Output

`--explain`
:   Add a comment to each statement that scans or rewrites a table. The comment says what the statement does, what its lock blocks, and the table's row and byte estimate from `pg_class`. The environment variable is `PISTA_EXPLAIN`. See [Explaining a plan](../../guides/explaining-plans.md).

`--out=`*file*
:   Also write the plan to *file*, for [`pista apply-from`](apply-from.md). The plan is fixed when it is written. `apply-from` runs these statements. It checks only that the schema has not changed since and that the server's major version is the same. A `-- pista:execute` check is evaluated now. A check that cannot be evaluated is an error. The environment variable is `PISTA_OUT`. See [Plan files](../../guides/plan-files.md).

`--check`
:   Exit with 2 when the plan contains executable DDL. The output does not change. The environment variable is `PISTA_CHECK`.

## Exit status

The exit status is 0 when the plan was printed, 1 on error, and 80 on a usage error. With `--check`, it is 2 when the plan contains executable DDL. A skipped drop alone is not executable DDL, so the exit status is 0.

## Environment

Every option names its variable above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment).

## Notes

Every connection sets `search_path` to `public`. A server-side `ALTER ROLE ... SET search_path` therefore has no effect on it. `--search-path` sets another value. PostgreSQL's own default, `"$user", public`, is not used. With that default, the objects of a schema named after the connecting role would be read without their schema. The role that runs migrations is often not the role that the application connects as.

The catalog reports an object that is reachable through `search_path` without its schema. `dump` writes the object as the catalog reports it. `plan` compares that form against the desired schema. So if the desired schema qualifies an object that the catalog reports without a schema, `plan` finds a difference on every run. Under `--search-path=` every object keeps its schema. Under `--search-path=myschema` the objects in `myschema` lose their schema. Under the default, the objects in `public` lose their schema.

`apply` sets `search_path` to the target schemas plus `public`, so that an unqualified reference resolves. `plan` does the same before it evaluates a `-- pista:execute` check. The plan output contains no such `SET`. So piping `pista plan -n myschema` into `psql` can fail on an unqualified reference. Qualify the reference, or run `pista apply`.

## Examples

Plan against the default connection, then with drops allowed:

```bash
pista plan schema.sql
pista plan --allow-drop all schema.sql
```

Plan a schema that is split across files:

```bash
pista plan schema/*.sql
```

Detect drift in CI from the exit code:

```bash
pista plan --check schema.sql
echo $?  # 0: no changes, 2: changes, 1: error
```

Write a plan file for a later apply:

```bash
pista plan --out plan.json schema.sql
pista apply-from plan.json
```

Prepend a statement timeout:

```bash
pista plan --pre-sql "SET statement_timeout = '5s';" schema.sql
```

## See also

[`pista apply`](apply.md), [`pista diff`](diff.md), [`pista dump`](dump.md), [Directives](../directives.md), [Controlling drops](../../guides/drops.md), [Filtering what is managed](../../guides/filtering.md)
