# pista plan

Print the DDL that brings the database in line with the schema files.

## Synopsis

```
pista plan [option...] file...
```

## Description

`pista plan` reads the desired schema from the files, reads the current schema from the database, and prints the DDL that takes the current schema to the desired one. Nothing is applied. [`pista apply`](apply.md) runs the same DDL.

The files are one schema. A statement that names another object, an `ALTER TABLE` or a `CREATE INDEX` for example, has to come after the `CREATE` of that object, in the same file or an earlier one. See [Supported objects](../objects.md).

The output is SQL. It opens with the connection and a count of the objects, then holds the pre-SQL, the concurrently-pre-SQL, the `-- pista:execute-first` statements, the DDL, the `-- pista:execute` statements, one `-- ignored:` comment per object that a `-- pista:ignore` directive leaves out, and one `-- skipped:` comment per drop that `--allow-drop` does not allow. `-- No changes` closes an output with no executable DDL.

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

These options decide what is read on both sides. `plan --out` records them in the plan file, and `apply-from` reads the database with them.

`-n` *schema*, `--schemas=`*schema*
:   The schemas to inspect. This option can be given more than once. A name written without a schema in the files is qualified with the first. The default is `public`. The environment variable is `PISTA_SCHEMAS`.

`-m` *old*`=`*new*, `--schema-map=`*old*`=`*new*
:   Read schema *old* from the database as *new*, so files written against *new* apply to *old*. This option can be given more than once, or several pairs can be separated by `;`.

`--search-path=`*path*
:   The `search_path` for the connection. The catalog reports an object reachable through it without its schema, so this decides which names come back bare. An empty value qualifies everything. The default is `public`. The environment variable is `PISTA_SEARCH_PATH`. See [Notes](#notes).

`-I` *pattern*, `--include=`*pattern*
:   Manage only the objects whose name matches. `*` and `?` are wildcards and match the whole name. `/re/` is a regular expression and matches anywhere unless anchored. The pattern matches the name alone, without the schema. This option can be given more than once. The environment variable is `PISTA_INCLUDE`.

`-E` *pattern*, `--exclude=`*pattern*
:   Leave out the objects whose name matches. The patterns are the same as for `--include`. This option can be given more than once. The environment variable is `PISTA_EXCLUDE`.

`--enable=`*type*
:   Manage only these object types: `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`. This option can be given more than once. It takes precedence over `--disable`. The environment variable is `PISTA_ENABLE`.

`--disable=`*type*
:   Leave out these object types. The values are the same as for `--enable`. This option can be given more than once. The environment variable is `PISTA_DISABLE`.

`--manage-routine`
:   Manage functions and procedures. This option is off by default. `--allow-drop routine` still gates dropping them. The environment variable is `PISTA_MANAGE_ROUTINE`. See [Routines](../objects.md#routines).

`--manage-storage-param`
:   Manage the storage parameters of tables and materialized views, the `WITH (...)` clause. This option is off by default. Without it the clause is ignored on both sides. A plain view's `security_barrier` and `security_invoker` are managed either way. The environment variable is `PISTA_MANAGE_STORAGE_PARAM`. See [Storage parameters](../objects.md#storage-parameters).

`--skip-partition-child`
:   Manage a partitioned table without its partitions. An `INHERITS` child is unaffected. The environment variable is `PISTA_SKIP_PARTITION_CHILD`. See [Skipping partition children](../../guides/filtering.md#skipping-partition-children).

### Statements

These options shape the DDL. `plan --out` writes the result into the plan file.

`--allow-drop=`*type*
:   Allow dropping these object types: `all`, `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`, `column`, `constraint`, `foreign_key`, `index`, `policy`, `trigger`. This option can be given more than once. Without it no drop is planned. Each is written as a `-- skipped:` comment. `constraint` covers CHECK, UNIQUE, PRIMARY KEY and EXCLUSION. `foreign_key` covers foreign keys. `composite_type` also gates `DROP ATTRIBUTE`. A view, routine or trigger recreate is gated by its type too. The environment variable is `PISTA_ALLOW_DROP`. See [Controlling drops](../../guides/drops.md).

`--pre-sql=`*sql*
:   The SQL to write before the DDL. This option cannot be used with `--pre-sql-file`. The environment variable is `PISTA_PRE_SQL`.

`--pre-sql-file=`*file*
:   The file whose SQL is written before the DDL. The environment variable is `PISTA_PRE_SQL_FILE`.

`--concurrently-pre-sql=`*sql*
:   The SQL to write after the pre-SQL when the plan holds a `CONCURRENTLY` index statement, `SET lock_timeout` for example. This option cannot be used with `--concurrently-pre-sql-file`. The environment variable is `PISTA_CONCURRENTLY_PRE_SQL`.

`--concurrently-pre-sql-file=`*file*
:   The file whose SQL is written after the pre-SQL when the plan holds a `CONCURRENTLY` index statement. The environment variable is `PISTA_CONCURRENTLY_PRE_SQL_FILE`.

`--disable-index-concurrently`
:   Ignore every `CONCURRENTLY` opt-in, the `-- pista:concurrently` directive and an inline `CREATE INDEX CONCURRENTLY` alike, and write plain `CREATE INDEX` and `DROP INDEX`. This option cannot be used with `--force-index-concurrently`. The environment variable is `PISTA_DISABLE_INDEX_CONCURRENTLY`.

`--force-index-concurrently`
:   Write `CONCURRENTLY` on every `CREATE INDEX` and `DROP INDEX`, including the drop of an index that the desired schema no longer holds, which no directive can reach. The environment variable is `PISTA_FORCE_INDEX_CONCURRENTLY`.

`--bulk-alter`
:   Merge consecutive `ALTER TABLE` actions on one table into one statement. Only column and constraint actions merge. Foreign keys, `RENAME`, `VALIDATE CONSTRAINT`, row-level security toggles, storage parameters and skipped drops stay separate. The `-- pista:bulk-alter` directive does the same for one table. The environment variable is `PISTA_BULK_ALTER`.

`--assume-validated`
:   Treat every table constraint, domain constraint and foreign key as validated. `NOT VALID` in the desired schema is ignored, and neither `NOT VALID` nor `VALIDATE CONSTRAINT` is written. This option is for a schema where `NOT VALID` was a migration step and not a desired state. The environment variable is `PISTA_ASSUME_VALIDATED`.

### Output

`--explain`
:   Comment each statement that scans or rewrites a table with what it does, what its lock blocks, and the table's row and byte estimate from `pg_class`. The environment variable is `PISTA_EXPLAIN`. See [Explaining a plan](../../guides/explaining-plans.md).

`--out=`*file*
:   Also write the plan to *file*, for [`pista apply-from`](apply-from.md). The plan is fixed when it is written: `apply-from` runs these statements and only checks that the schema has not changed under them. A `-- pista:execute` check is evaluated now, and one that cannot be evaluated is an error. The environment variable is `PISTA_OUT`. See [Plan files](../../guides/plan-files.md).

`--check`
:   Exit with 2 when the plan holds executable DDL. The output does not change. The environment variable is `PISTA_CHECK`.

## Exit status

The exit status is 0 when the plan was printed, 1 on error, and 80 on a usage error. With `--check`, it is 2 when the plan holds executable DDL. A skipped drop alone is not executable DDL and exits with 0.

## Environment

Every option names its variable above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment).

## Notes

Every connection sets `search_path` to `public`, so a server-side `ALTER ROLE ... SET search_path` does not reach it. `--search-path` sets another value. PostgreSQL's own default, `"$user", public`, is not used: it would read the objects of a schema named after the connecting role without their schema, and the role that runs migrations is often not the role that the application connects as.

The catalog reports an object reachable through `search_path` without its schema. `dump` writes the object as the catalog reports it, and `plan` compares that form against the desired schema, so a desired schema that qualifies an object that the catalog reports bare differs on every run. Under `--search-path=` every object keeps its schema. Under `--search-path=myschema` the objects in `myschema` lose theirs, and under the default so do those in `public`.

`apply` sets `search_path` to the target schemas plus `public` so an unqualified reference resolves, and `plan` does the same before it evaluates a `-- pista:execute` check. The plan output holds no such `SET`, so piping `pista plan -n myschema` into `psql` can fail on an unqualified reference. Qualify the reference or run `pista apply`.

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
