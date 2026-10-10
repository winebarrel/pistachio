# Aurora DSQL

`--engine dsql` applies a schema to Amazon Aurora DSQL. The desired schema is written in PostgreSQL syntax as for any other server. pistachio reads the current schema the same way, computes the same diff, and then writes the statements in the form DSQL takes. A statement that DSQL has no form of is refused when the plan is computed.

DSQL supports a subset of PostgreSQL. A desired schema with a foreign key, an enum, a trigger, a policy, a partitioned table, a composite type or a column collation is applied as usual, and DSQL refuses the statement.

## Connecting

DSQL authenticates with an IAM token instead of a password. Pass the token with `--password` or `PGPASSWORD`:

```bash
export PGPASSWORD="$(aws dsql generate-db-connect-admin-auth-token --hostname "$HOST" --region "$REGION")"
pista plan --engine dsql -c "postgres://admin@$HOST/postgres?sslmode=require" schema.sql
```

DSQL rejects the parameter that makes a connection read-only. `plan` and `dump` therefore connect read-write under `--engine dsql`.

## Statements

The plan output differs from the PostgreSQL output as follows:

| Change | Statements |
| --- | --- |
| Create an index | `CREATE INDEX ASYNC`, without `USING btree`. DSQL has one access method and rejects `USING`. Another method, such as `USING gin`, is kept, and DSQL refuses it. |
| Add a column with a `DEFAULT` | `ADD COLUMN` without the default, then `ALTER COLUMN ... SET DEFAULT`. The rows already in the table keep `NULL`. DSQL has no statement that fills them. |
| Add a unique constraint to an existing table | `CREATE UNIQUE INDEX ASYNC`, then `ADD CONSTRAINT ... UNIQUE USING INDEX`. |
| Create an identity column | `CACHE 1` is written when no cache size is given. DSQL rejects an identity column without one. |

`dump` writes `CACHE 1` on an identity column for the same reason. It writes an index with `USING btree` and without `ASYNC`, as for PostgreSQL, so that the dump can be read back. Do not write `ASYNC` in the desired schema. It is not PostgreSQL syntax, and pistachio cannot parse it.

DSQL reports some values that the schema does not state. pistachio ignores them when it compares the two sides and when it dumps:

- The access method `btree_index`, which is read as `btree`.
- The `INCLUDE` columns that DSQL adds to every primary key.
- The `lz4` compression of every column.

## Refused changes

The following changes are refused when the plan is computed. DSQL can make them only in `CREATE TABLE`, or not at all:

- `DROP COLUMN`
- `ALTER COLUMN ... SET NOT NULL`
- A change of column type
- Adding a column with `NOT NULL`
- Adding a primary key or a check constraint to an existing table
- `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY`, including those that `-- pista:concurrently` asks for
- `-- pista:bulk-alter`

The following options cannot be used with `--engine dsql`:

| Option | Reason |
| --- | --- |
| `--with-tx`, `--try-tx` | DSQL runs one DDL statement per transaction. |
| `--exclusive`, `--exclusive-wait` | DSQL has no advisory lock. |
| `--force-index-concurrently`, `--disable-index-concurrently`, `--concurrently-pre-sql`, `--concurrently-pre-sql-file` | DSQL has no `CONCURRENTLY`. |
| `--bulk-alter` | DSQL takes no lock for DDL and does not rewrite a table, so combining statements saves nothing. |
| `--explain` | DSQL does not keep the estimates in `pg_class`. |

`apply-from` takes the engine from the plan file, and refuses the same options.

## Index builds

DSQL builds an index in the background. `CREATE INDEX ASYNC` returns the job that builds it. `apply` waits for each job with `sys.wait_for_job` before it runs the next statement, and stops when the build fails. A failed build leaves the index invalid. Drop it before the next run.

`--dsql-no-wait-index-build` does not wait. `apply` then finishes while the builds are still running. A failed build is not reported. A unique constraint that takes over an index built in the same run needs the index to be complete, so that combination is refused.

DSQL limits a transaction to 5 minutes and a connection to 60 minutes. A build that takes longer than 5 minutes may end the wait. Use `--dsql-no-wait-index-build` for such an index. An `apply` that runs longer than 60 minutes loses its connection and fails.
