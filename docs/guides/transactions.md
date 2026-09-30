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

A DDL statement waits for every transaction that holds a conflicting lock. Every statement that arrives later waits behind it. `lock_timeout` limits that wait, and `--pre-sql` sets it for the run:

```bash
pista apply --pre-sql "SET lock_timeout = '5s';" schema.sql
```

`--concurrently-pre-sql` runs its SQL only when the plan contains a `CONCURRENTLY` statement. Use it for a setting that matters only to a long index build:

```bash
pista apply --try-tx --concurrently-pre-sql "SET lock_timeout = '5s';" schema.sql
```

Both are session-scoped `SET`s, so they remain in effect for the rest of the run. `--pre-sql-file` and `--concurrently-pre-sql-file` read the SQL from a file instead. A `SET statement_timeout` limits the statement itself in the same way.

## What a plan locks

`plan --explain` writes a comment before each statement that scans or rewrites a table. The comment says what its lock blocks and how large the table is. See [Explaining a plan](explaining-plans.md).

## Two applies at once

`--exclusive` makes apply runs on one database mutually exclusive. See [Preventing concurrent applies](exclusive-apply.md).

See [`pista apply`](../reference/commands/apply.md) for the options.
