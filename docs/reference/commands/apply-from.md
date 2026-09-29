# pista apply-from

Run a plan file written by `plan --out`.

## Synopsis

```
pista apply-from [option...] plan-file
```

## Description

`pista apply-from` runs the statements a plan file holds. It reads no schema file and diffs nothing: the statements were decided when [`pista plan --out`](plan.md) wrote the file. It reads the database only to check that the schema is still what the plan was computed against, then runs them.

When the schema has changed since, nothing runs:

```
pista: error: the database has drifted since plan file plan.json was written: run plan again, or pass --force to apply it as it is
```

The output is the one [`pista apply`](apply.md) writes, and the statements run the same way. See [Plan files](../../guides/plan-files.md) for what the check covers.

## Options

The [general options](index.md#general-options) apply as well.

The options that decide what is read or what is run are not accepted. The plan file records them, and one given here would report drift that is not there.

### Connection

`-c` *connstr*, `--conn-string=`*connstr*
:   PostgreSQL connection string, in either form [libpq accepts](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING). Default: `postgres://postgres@localhost/postgres`. Environment: `PISTA_CONN_STR`.

`-d` *dbname*, `--dbname=`*dbname*
:   Database name. Overrides the one in the connection string. Environment: `PISTA_DBNAME`.

`--password=`*password*
:   Password, kept out of the connection string. Environment: `PISTA_PASSWORD`.

### Execution

`--with-tx`
:   Run the pre-SQL and the statements in one transaction. Refused when the plan holds a `CONCURRENTLY` index statement. Conflicts with `--try-tx`. Environment: `PISTA_WITH_TX`.

`--try-tx`
:   Like `--with-tx`, but a plan that holds a `CONCURRENTLY` index statement runs without a transaction instead of being refused. Environment: `PISTA_TRY_TX`.

`--timing`
:   Write each statement's elapsed time after it as a comment. Environment: `PISTA_TIMING`. See the [notes on `apply`](apply.md#timing).

`--exclusive`
:   Fail at once when another exclusive apply is running on the database. Conflicts with `--exclusive-wait`. Environment: `PISTA_EXCLUSIVE`. See [Preventing concurrent applies](../../guides/exclusive-apply.md).

`--exclusive-wait=`*duration*
:   Like `--exclusive`, but wait up to *duration* for the other apply to finish. `0` waits without limit. Environment: `PISTA_EXCLUSIVE_WAIT`.

`--force`
:   Run the plan even where the database has drifted since it was written. The drift is reported as a warning at the top of the output instead of an error. A difference in the server's major version is refused either way. Not settable from the config file.

## Exit status

0 when every statement ran, 1 on error, drift included, 80 on a usage error.

## Environment

Every option names its variable above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment).

## Examples

Plan on one machine, apply on another:

```bash
pista plan --out plan.json schema.sql
pista apply-from --with-tx plan.json
```

Run it in spite of drift:

```bash
pista apply-from --force plan.json
```

```sql
-- Warning: the database has drifted since the plan was written
```

## See also

[`pista plan`](plan.md), [`pista apply`](apply.md), [Plan files](../../guides/plan-files.md)
