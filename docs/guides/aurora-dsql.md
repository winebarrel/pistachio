# Aurora DSQL

Use `--engine dsql` to manage a schema on Amazon Aurora DSQL. Write the schema in normal PostgreSQL syntax. pistachio changes the generated statements into the form that DSQL accepts. `plan` fails for the changes listed in [Changes that plan rejects](#changes-that-plan-rejects).

DSQL supports only part of PostgreSQL. For example, it has no foreign keys, enums, triggers, policies, partitioned tables, composite types or column collations. pistachio does not check for these. DSQL rejects them when you apply.

## Connecting

DSQL uses an IAM token as the password. Pass it with `--password` or `PGPASSWORD`:

```bash
export PGPASSWORD="$(aws dsql generate-db-connect-admin-auth-token --hostname "$HOST" --region "$REGION")"
pista plan --engine dsql -c "postgres://admin@$HOST/postgres?sslmode=require" schema.sql
```

`plan` and `dump` use a read-only connection, as they do on PostgreSQL. DSQL rejects the `default_transaction_read_only` parameter that pistachio normally sets. So with `--engine dsql`, pistachio runs `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` after it connects. `--no-read-only` skips it.

## Generated statements

| Change | Statements |
| --- | --- |
| Create an index | `CREATE INDEX ASYNC`, without `USING btree`. DSQL has only one index method and rejects `USING`. Any other method, such as `USING gin`, is kept, and DSQL rejects it. |
| Add a column with a `DEFAULT` | `ADD COLUMN` without the default, then `ALTER COLUMN ... SET DEFAULT`. Existing rows get `NULL`, not the default. DSQL cannot fill them. |
| Add a unique constraint to an existing table | `CREATE UNIQUE INDEX ASYNC`, then `ADD CONSTRAINT ... UNIQUE USING INDEX`. |
| Create an identity column | `CACHE 1` is added when no cache size is given. DSQL requires a cache size. |

`dump` also adds `CACHE 1` to identity columns. It writes indexes with `USING btree` and without `ASYNC`, so that pistachio can read the dump back. Do not write `ASYNC` in your schema files. It is not PostgreSQL syntax, so pistachio cannot parse it.

DSQL reports some things that are not in your schema. pistachio ignores them:

- The index method `btree_index`. pistachio reads it as `btree`.
- The `INCLUDE` columns that DSQL adds to every primary key.
- The `lz4` compression on every column.

## Changes that plan rejects

DSQL can make these changes only in `CREATE TABLE`, or not at all. `plan` fails if the schema needs one of them:

- `DROP COLUMN`
- `ALTER COLUMN ... SET NOT NULL`
- Changing a column's type
- Adding a column with `NOT NULL`
- Adding a primary key or a check constraint to an existing table
- `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY`, including those from `-- pista:concurrently`
- `-- pista:bulk-alter`

## Options you cannot use

| Option | Reason |
| --- | --- |
| `--with-tx`, `--try-tx` | DSQL allows only one DDL statement in a transaction. |
| `--exclusive`, `--exclusive-wait` | DSQL has no advisory locks. |
| `--force-index-concurrently`, `--disable-index-concurrently`, `--concurrently-pre-sql`, `--concurrently-pre-sql-file` | DSQL has no `CONCURRENTLY`. |
| `--bulk-alter` | DSQL does not lock or rewrite a table for DDL, so combining statements does not help. |
| `--explain` | DSQL does not keep the size estimates that `--explain` reads. |

`apply-from` reads the engine from the plan file and rejects the same options.

## Index builds

DSQL builds indexes in the background. `CREATE INDEX ASYNC` starts a job and returns at once. `apply` waits for each job with `sys.wait_for_job` before it runs the next statement. If a build fails, `apply` stops. The failed index stays in the database as an invalid index. Drop it before you run `apply` again.

With `--dsql-no-wait-index-build`, `apply` does not wait. It can finish before the builds do, and it does not report a build that fails. A unique constraint must wait for its index, so `apply` refuses this option when it adds one with `USING INDEX` on an index it also builds.

A DSQL transaction can last at most 5 minutes, and a connection at most 60 minutes. If a build takes more than 5 minutes, the wait may fail. Use `--dsql-no-wait-index-build` for such an index. If `apply` runs for more than 60 minutes, it loses its connection and fails.
