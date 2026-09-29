# Transactions and locks

By default `apply` commits each statement on its own. A failure stops the run, the statements that already ran stay applied, and running `apply` again applies the rest.

## One transaction

`--with-tx` runs the pre-SQL and every statement in one transaction, so the schema changes as a whole or not at all:

```bash
pista apply --with-tx schema.sql
```

```sql
-- Transaction started
ALTER TABLE public.users ADD COLUMN email text;
CREATE INDEX users_email_idx ON public.users USING btree (email);
-- Transaction committed
```

`CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` cannot run in a transaction, so `--with-tx` is refused when the plan holds one. An index that opts into `CONCURRENTLY` and does not change is no obstacle.

## Building indexes without blocking writes

A plain `CREATE INDEX` blocks writes to the table until the build ends. `CONCURRENTLY` does not, and is opted into per index, either inline or with the directive:

```sql
CREATE INDEX CONCURRENTLY users_email_idx ON public.users USING btree (email);

-- pista:concurrently
CREATE INDEX users_name_idx ON public.users USING btree (name);
```

The opt-in also drops the index with `DROP INDEX CONCURRENTLY`. An index the desired schema no longer holds carries no directive, so `--force-index-concurrently` is the way to drop it that way; it puts `CONCURRENTLY` on every index statement of the run.

`--try-tx` runs the plan in a transaction when it can, and without one when the plan holds a `CONCURRENTLY` statement:

```bash
pista apply --try-tx schema.sql
```

```sql
-- Transaction skipped: plan contains CONCURRENTLY index DDL
CREATE INDEX CONCURRENTLY users_email_idx ON public.users USING btree (email);
```

Such a run is not all-or-nothing, and a `CREATE INDEX CONCURRENTLY` that fails leaves an invalid index to drop by hand. To run one plan in a transaction in spite of the opt-ins, add `--disable-index-concurrently`; the directives stay in the files for the next run.

## Bounding lock waits

A DDL statement waits for every transaction that holds a conflicting lock, and every statement arriving later waits behind it. `lock_timeout` bounds that wait, and `--pre-sql` sets it for the run:

```bash
pista apply --pre-sql "SET lock_timeout = '5s';" schema.sql
```

`--concurrently-pre-sql` runs its SQL only when the plan holds a `CONCURRENTLY` statement, for a setting that matters to a long index build alone:

```bash
pista apply --try-tx --concurrently-pre-sql "SET lock_timeout = '5s';" schema.sql
```

Both are session-scoped `SET`s, so they stay in effect for the rest of the run. `--pre-sql-file` and `--concurrently-pre-sql-file` read the SQL from a file instead. A `SET statement_timeout` bounds the statement itself the same way.

## What a plan locks

`plan --explain` comments each statement that scans or rewrites a table with what its lock blocks and how large the table is. See [Explaining a plan](explaining-plans.md).

## Two applies at once

`--exclusive` makes apply runs on one database mutually exclusive. See [Preventing concurrent applies](exclusive-apply.md).

See [`pista apply`](../reference/commands/apply.md) for the options.
