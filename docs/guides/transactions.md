# Transactions and locks

By default `apply` commits each statement on its own. A failure stops the run. The statements that already ran remain applied, and running `apply` again applies the rest.

## One transaction

`--with-tx` runs the pre-SQL and every statement in one transaction. The schema changes as a whole, or not at all:

```bash
pista apply --with-tx schema.sql
```

```sql
-- Transaction started
ALTER TABLE public.users ADD COLUMN email text;
CREATE INDEX users_email_idx ON public.users USING btree (email);
-- Transaction committed
```

`CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` cannot run in a transaction. So `--with-tx` is refused when the plan contains one of them. An index that opts into `CONCURRENTLY` but does not change is not a problem.

## Building indexes without blocking writes

A plain `CREATE INDEX` blocks writes to the table until the build ends. `CONCURRENTLY` does not. It is opted into per index, either inline or with the directive:

```sql
CREATE INDEX CONCURRENTLY users_email_idx ON public.users USING btree (email);

-- pista:concurrently
CREATE INDEX users_name_idx ON public.users USING btree (name);
```

The opt-in also drops the index with `DROP INDEX CONCURRENTLY`. An index that is no longer in the desired schema has no directive. To drop it that way, use `--force-index-concurrently`. That option puts `CONCURRENTLY` on every index statement of the run.

`--try-tx` runs the plan in a transaction when it can. When the plan contains a `CONCURRENTLY` statement, it runs without one:

```bash
pista apply --try-tx schema.sql
```

```sql
-- Transaction skipped: plan contains CONCURRENTLY index DDL
CREATE INDEX CONCURRENTLY users_email_idx ON public.users USING btree (email);
```

Such a run is not all-or-nothing. A `CREATE INDEX CONCURRENTLY` that fails leaves an invalid index, which you must drop by hand. To run one plan in a transaction despite the opt-ins, add `--disable-index-concurrently`. The directives remain in the files for the next run.

## Bounding lock waits

A DDL statement waits for every transaction that holds a conflicting lock. Every later statement that needs a conflicting lock waits behind it. `lock_timeout` limits that wait, and `--lock-timeout` sets it for the run:

```bash
pista apply --lock-timeout=5s schema.sql
```

When the wait runs out, the statement fails and the apply stops. With `--lock-retries`, the statement runs again after `--lock-retry-delay`. The default delay is 1 second:

```bash
pista apply --lock-timeout=500ms --lock-retries=5 --lock-retry-delay=10s schema.sql
```

With a short timeout, the queries queued behind the DDL wait only briefly. The retries give a long transaction time to finish.

Only the failed schema change runs again. The pre-SQL and `-- pista:execute` statements are not retried. In a transaction, the failure aborts the whole transaction, so `--lock-retries` cannot be used with `--with-tx` or `--try-tx`. A `CONCURRENTLY` index statement does not run again, because a failed one can leave an invalid index.

`--statement-timeout` sets `statement_timeout`. It limits each statement, including its wait for a lock. A statement that fails on it does not run again.

Both options run a `SET` before the pre-SQL. The setting lasts for the rest of the run. A `SET` in the pre-SQL overrides it. `--concurrently-pre-sql` runs only when the plan contains a `CONCURRENTLY` statement, so it can give a long index build its own value:

```bash
pista apply --statement-timeout=1m --concurrently-pre-sql "SET statement_timeout = 0;" schema.sql
```

## What a plan locks

`plan --explain` writes a comment before each statement that scans or rewrites a table. The comment says what its lock blocks and how large the table is. See [Explaining a plan](explaining-plans.md).

## Two applies at once

`--exclusive` makes apply runs on one database mutually exclusive. See [Preventing concurrent applies](exclusive-apply.md).

See [`pista apply`](../reference/commands/apply.md) for the options.
