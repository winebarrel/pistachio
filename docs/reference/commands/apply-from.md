# pista apply-from

Run a plan file written by `plan --out`.

## Synopsis

```
pista apply-from [option...] plan-file
```

## Description

`pista apply-from` runs the statements that a plan file contains. It reads no schema file and computes no diff. The statements were decided when [`pista plan --out`](plan.md) wrote the file. `apply-from` reads the database only to check that the schema is still the one the plan was computed against. Then it runs the statements.

When the schema has changed since then, nothing runs:

```
pista: error: the database has drifted since plan file plan.json was written: run plan again, or pass --force to apply it as it is
```

The output is the one that [`pista apply`](apply.md) writes. The statements run in the same way. See [Plan files](../../guides/plan-files.md) for what the check covers.

## Options

The [general options](index.md#general-options) apply as well.

The options that decide what is read or what is run are not accepted. The plan file records them. An option given here would report drift that does not exist.

### Connection

`-c` *connstr*, `--conn-string=`*connstr*
:   The PostgreSQL connection string, in either form that [libpq accepts](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING). The default is `postgres://postgres@localhost/postgres`. The environment variable is `PISTA_CONN_STR`.

`-d` *dbname*, `--dbname=`*dbname*
:   The database name. It overrides the one in the connection string. The environment variable is `PISTA_DBNAME`.

`--password=`*password*
:   The password, kept out of the connection string. The environment variable is `PISTA_PASSWORD`.

### Execution

`--with-tx`
:   Run the pre-SQL and the statements in one transaction. This option is refused when the plan contains a `CONCURRENTLY` index statement. It cannot be used with `--try-tx`. The environment variable is `PISTA_WITH_TX`.

`--try-tx`
:   This option is like `--with-tx`. But when the plan contains a `CONCURRENTLY` index statement, the plan runs without a transaction instead of being refused. The environment variable is `PISTA_TRY_TX`.

`--timing`
:   Write the elapsed time of each statement after it, as a comment. The environment variable is `PISTA_TIMING`. See the [notes on `apply`](apply.md#timing).

`--exclusive`
:   Fail at once when another exclusive apply is running on the database. This option cannot be used with `--exclusive-wait`. The environment variable is `PISTA_EXCLUSIVE`. See [Preventing concurrent applies](../../guides/exclusive-apply.md).

`--exclusive-wait=`*duration*
:   This option is like `--exclusive`, but it waits up to *duration* for the other apply to finish. `0` waits without limit. The environment variable is `PISTA_EXCLUSIVE_WAIT`.

`--force`
:   Run the plan even when the database has drifted since the plan was written. The drift is reported as a warning at the top of the output, not as an error. A difference in the major version of the server is refused in both cases. This option cannot be set from the config file.

## Exit status

The exit status is 0 when every statement ran, 1 on error, and 80 on a usage error. Drift counts as an error.

## Environment

Every option names its variable above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment).

## Examples

Plan on one machine, and apply on another:

```bash
pista plan --out plan.json schema.sql
pista apply-from --with-tx plan.json
```

Run it despite drift:

```bash
pista apply-from --force plan.json
```

```sql
-- Warning: the database has drifted since the plan was written
```

## See also

[`pista plan`](plan.md), [`pista apply`](apply.md), [Plan files](../../guides/plan-files.md)
