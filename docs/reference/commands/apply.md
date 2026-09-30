# pista apply

Bring the database in line with the schema files.

## Synopsis

```
pista apply [option...] file...
```

## Description

`pista apply` computes the plan that [`pista plan`](plan.md) prints and runs it. The files, the scope and the statement options are the same as `plan`'s, so `pista plan` followed by `pista apply` with the same arguments runs what the plan showed.

The statements run in this order: the pre-SQL, the concurrently-pre-SQL, the `-- pista:execute-first` statements, the DDL, then the `-- pista:execute` statements. Nothing is run when the plan holds no statement, a `-- pista:execute` statement included, and the output ends with `-- No changes`. Otherwise it ends with the time that the apply took:

```sql
-- Connected to postgres://postgres@localhost:5432/postgres
-- Apply to schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
ALTER TABLE public.users ADD COLUMN email text;
-- Apply finished in 12ms
```

The output is written when the run ends, with the statements in the order in which they ran. A failure stops the run. The output then ends at the statement that failed, and the statements that already ran stay applied unless they ran in a transaction. The connection sets `search_path` to the target schemas plus `public`, so an unqualified reference in the DDL resolves.

A drop that the desired schema implies is not run unless `--allow-drop` names its type. Each such drop is written as a `-- skipped:` comment instead.

## Options

The [general options](index.md#general-options) apply as well.

### Connection

`-c` *connstr*, `--conn-string=`*connstr*
:   The PostgreSQL connection string, in either form that [libpq accepts](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING). The default is `postgres://postgres@localhost/postgres`. The environment variable is `PISTA_CONN_STR`.

`-d` *dbname*, `--dbname=`*dbname*
:   The database name. It overrides the one in the connection string. The environment variable is `PISTA_DBNAME`.

`--password=`*password*
:   The password, kept out of the connection string. The environment variable is `PISTA_PASSWORD`.

### Scope

`-n` *schema*, `--schemas=`*schema*
:   The schemas to inspect and modify. This option can be given more than once. A name written without a schema in the files is qualified with the first. The default is `public`. The environment variable is `PISTA_SCHEMAS`.

`-m` *old*`=`*new*, `--schema-map=`*old*`=`*new*
:   Read schema *old* from the database as *new*, so files written against *new* apply to *old*. This option can be given more than once, or several pairs can be separated by `;`.

`--search-path=`*path*
:   The `search_path` for the connection. The catalog reports an object reachable through it without its schema, so this decides which names come back bare. An empty value qualifies everything. The default is `public`. The environment variable is `PISTA_SEARCH_PATH`. See the [notes on `plan`](plan.md#notes).

`-I` *pattern*, `--include=`*pattern*
:   Manage only the objects whose name matches. `*` and `?` are wildcards and match the whole name. `/re/` is a regular expression and matches anywhere unless anchored. The pattern matches the name alone, without the schema. This option can be given more than once. The environment variable is `PISTA_INCLUDE`.

`-E` *pattern*, `--exclude=`*pattern*
:   Leave out the objects whose name matches. The patterns are the same as for `--include`. This option can be given more than once. The environment variable is `PISTA_EXCLUDE`.

`--enable=`*type*
:   Manage only these object types: `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`. This option can be given more than once. It takes precedence over `--disable`. It is meant for inspection: it can leave out an object that another depends on. The environment variable is `PISTA_ENABLE`.

`--disable=`*type*
:   Leave out these object types. The values are the same as for `--enable`. This option can be given more than once. The environment variable is `PISTA_DISABLE`.

`--manage-routine`
:   Manage functions and procedures. This option is off by default. `--allow-drop routine` still gates dropping them. The environment variable is `PISTA_MANAGE_ROUTINE`. See [Routines](../objects.md#routines).

`--manage-storage-param`
:   Manage the storage parameters of tables and materialized views, the `WITH (...)` clause. This option is off by default. Without it the clause is ignored on both sides. A plain view's `security_barrier` and `security_invoker` are managed either way. The environment variable is `PISTA_MANAGE_STORAGE_PARAM`. See [Storage parameters](../objects.md#storage-parameters).

`--skip-partition-child`
:   Manage a partitioned table without its partitions. An `INHERITS` child is unaffected. The environment variable is `PISTA_SKIP_PARTITION_CHILD`. See [Skipping partition children](../../guides/filtering.md#skipping-partition-children).

### Statements

`--allow-drop=`*type*
:   Allow dropping these object types: `all`, `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`, `column`, `constraint`, `foreign_key`, `index`, `policy`, `trigger`. This option can be given more than once. Without it no drop runs. Each is written as a `-- skipped:` comment. `constraint` covers CHECK, UNIQUE, PRIMARY KEY and EXCLUSION. `foreign_key` covers foreign keys. `composite_type` also gates `DROP ATTRIBUTE`. A view, routine or trigger recreate is gated by its type too. The environment variable is `PISTA_ALLOW_DROP`. See [Controlling drops](../../guides/drops.md).

`--pre-sql=`*sql*
:   The SQL to run before the DDL, inside the transaction when there is one. This option cannot be used with `--pre-sql-file`. The environment variable is `PISTA_PRE_SQL`.

`--pre-sql-file=`*file*
:   The file whose SQL runs before the DDL. The environment variable is `PISTA_PRE_SQL_FILE`.

`--concurrently-pre-sql=`*sql*
:   The SQL to run after the pre-SQL when the plan holds a `CONCURRENTLY` index statement, `SET lock_timeout` for example. It runs outside any transaction. A `SET` is session-scoped, so it stays in effect for every later statement of the run. This option cannot be used with `--concurrently-pre-sql-file`. The environment variable is `PISTA_CONCURRENTLY_PRE_SQL`.

`--concurrently-pre-sql-file=`*file*
:   The file whose SQL runs after the pre-SQL when the plan holds a `CONCURRENTLY` index statement. The environment variable is `PISTA_CONCURRENTLY_PRE_SQL_FILE`.

`--disable-index-concurrently`
:   Ignore every `CONCURRENTLY` opt-in, the `-- pista:concurrently` directive and an inline `CREATE INDEX CONCURRENTLY` alike, and run plain `CREATE INDEX` and `DROP INDEX`. This option lets a schema keep its directives while one run goes through a transaction. It cannot be used with `--force-index-concurrently`. The environment variable is `PISTA_DISABLE_INDEX_CONCURRENTLY`.

`--force-index-concurrently`
:   Run `CONCURRENTLY` on every `CREATE INDEX` and `DROP INDEX`, including the drop of an index that the desired schema no longer holds, which no directive can reach. This option cannot be used with `--with-tx`. The environment variable is `PISTA_FORCE_INDEX_CONCURRENTLY`.

`--bulk-alter`
:   Merge consecutive `ALTER TABLE` actions on one table into one statement. Only column and constraint actions merge. Foreign keys, `RENAME`, `VALIDATE CONSTRAINT`, row-level security toggles, storage parameters and skipped drops stay separate. The `-- pista:bulk-alter` directive does the same for one table. The environment variable is `PISTA_BULK_ALTER`.

`--assume-validated`
:   Treat every table constraint, domain constraint and foreign key as validated. `NOT VALID` in the desired schema is ignored, and neither `NOT VALID` nor `VALIDATE CONSTRAINT` is run. The environment variable is `PISTA_ASSUME_VALIDATED`.

### Execution

`--with-tx`
:   Run the pre-SQL and the statements in one transaction. This option is refused when the plan holds a `CONCURRENTLY` index statement, which cannot run in a transaction. A `CONCURRENTLY` opt-in on an index that does not change is no obstacle. This option cannot be used with `--try-tx`. The environment variable is `PISTA_WITH_TX`.

`--try-tx`
:   This option is like `--with-tx`, but a plan that holds a `CONCURRENTLY` index statement runs without a transaction instead of being refused. The environment variable is `PISTA_TRY_TX`.

`--timing`
:   Write each statement's elapsed time after it as a comment. The time is measured on the client, so it covers the round trip and any wait for a lock. The environment variable is `PISTA_TIMING`.

`--exclusive`
:   Make apply runs on the same database mutually exclusive. When another exclusive apply is running, fail at once, before the database is read. This option cannot be used with `--exclusive-wait`. The environment variable is `PISTA_EXCLUSIVE`. See [Preventing concurrent applies](../../guides/exclusive-apply.md).

`--exclusive-wait=`*duration*
:   This option is like `--exclusive`, but it waits up to *duration* for the other apply to finish. `0` waits without limit. The value is a Go duration, `30s` or `5m`. The environment variable is `PISTA_EXCLUSIVE_WAIT`.

## Exit status

The exit status is 0 when every statement ran or there was nothing to run, 1 on error, and 80 on a usage error.

## Environment

Every option names its variable above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment).

## Notes

### Transactions

Without `--with-tx` or `--try-tx` each statement commits on its own. `--with-tx` wraps the pre-SQL and every statement in one transaction, marked `-- Transaction started` and `-- Transaction committed` in the output. It is refused when the plan holds a `CONCURRENTLY` index statement. `--try-tx` then runs without a transaction and writes `-- Transaction skipped: plan contains CONCURRENTLY index DDL`. See [Transactions and locks](../../guides/transactions.md).

### Timing

`--timing` writes a `-- Time:` comment after every statement that the run writes out: the pre-SQL, the concurrently-pre-SQL, the `-- pista:execute-first` and `-- pista:execute` statements, the DDL, and `BEGIN` and `COMMIT`. The `search_path` setup and a directive's check SQL are not written out, so they are not timed.

```sql
-- Transaction started
-- Time: 0.264 ms
ALTER TABLE public.users ADD COLUMN email text;
-- Time: 0.326 ms
CREATE INDEX idx_users_email ON public.users USING btree (email);
-- Time: 4213.882 ms
-- Transaction committed
-- Time: 0.921 ms
```

With `--with-tx`, work that PostgreSQL defers to commit lands on `COMMIT` rather than on the statement that caused it. A statement that fails gets no time, so the output shows where the run stopped.

## Examples

Apply, with drops of columns and tables allowed:

```bash
pista apply --allow-drop column,table schema.sql
```

Apply in one transaction, with a statement timeout:

```bash
pista apply --with-tx --pre-sql "SET statement_timeout = '5s';" schema.sql
```

Build indexes concurrently with a lock timeout, and time each statement:

```bash
pista apply --try-tx --concurrently-pre-sql "SET lock_timeout = '5s';" --timing schema.sql
```

Refuse to run beside another apply, or wait for it:

```bash
pista apply --exclusive schema.sql
pista apply --exclusive-wait=5m schema.sql
```

## See also

[`pista plan`](plan.md), [`pista apply-from`](apply-from.md), [Directives](../directives.md), [Transactions and locks](../../guides/transactions.md), [Running arbitrary SQL](../../guides/executing-sql.md)
