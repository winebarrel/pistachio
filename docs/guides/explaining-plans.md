# Explaining a plan

A plan says what will run, not what it costs. `--explain` writes a comment before each statement that reads or rewrites a table that already holds data.

```
$ pista plan --explain schema.sql
-- rewrite, blocks reads and writes: public.events (~120000000 rows, 9629 MB, 5 indexes rebuilt)
ALTER TABLE public.events ALTER COLUMN amount SET DATA TYPE numeric(12,2);
-- scan, blocks writes: public.orders (~2000000 rows, 210 MB), public.customers (~50000 rows, 6280 kB)
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers (id);
-- scan, blocks nothing: public.events (~120000000 rows, 9629 MB, 24 partitions)
CREATE INDEX CONCURRENTLY events_at_idx ON public.events USING btree (at);
ALTER TABLE public.events ALTER COLUMN note SET DEFAULT '';
```

Also available as `$PISTA_EXPLAIN`. The comments are SQL comments, so the output still pipes into `psql`.


## What the comment says

The first word is what the statement does to the rows that already exist. `rewrite` copies the table into a new file and builds every index on it again, so it needs as much free disk as the table takes and writes that much WAL for a replica to replay. `scan` reads every row once, which is what a constraint validation, an index build and a NOT NULL check do, and writes no new heap.

The phrase after it is what the statement's lock stops while it runs. `blocks reads and writes` is ACCESS EXCLUSIVE, where even a `SELECT` waits. `blocks writes` is SHARE or SHARE ROW EXCLUSIVE. `blocks nothing` is SHARE UPDATE EXCLUSIVE or weaker, where only another DDL on the same table waits.

Then come the tables the statement touches, each with its size. The rows and bytes are the estimates VACUUM and ANALYZE last wrote to `pg_class`, the TOAST relation's pages included, so the comment costs no read of the table itself. A table neither has visited reads as `not analyzed`.

A rewrite also names how many indexes it builds again. A partitioned table shows the sum of its partitions and their number, an `INHERITS` parent its own rows plus its children's. A foreign key names the referenced table next to the referencing one, since both are locked, and a domain change names every table with a column of the domain.


## Which statements get one

A rewrite comes from a column type change that is not a widening, from `ADD COLUMN` with a volatile default, an identity, a generated expression or a serial type, and from `SET LOGGED` and `SET UNLOGGED`. Each takes ACCESS EXCLUSIVE.

A scan comes from `SET NOT NULL`, from `ADD COLUMN ... NOT NULL` without a default, from `ADD CONSTRAINT` for a check, a primary key, a unique key or a foreign key, from `VALIDATE CONSTRAINT` and from `CREATE INDEX`. The lock differs across them: a foreign key takes SHARE ROW EXCLUSIVE on both tables, `VALIDATE CONSTRAINT` and `CREATE INDEX CONCURRENTLY` take SHARE UPDATE EXCLUSIVE, a plain `CREATE INDEX` takes SHARE, and the rest take ACCESS EXCLUSIVE.

`ALTER DOMAIN` scans every table with a column of the domain when it sets NOT NULL, adds a validated constraint, or validates one. `CREATE MATERIALIZED VIEW` scans the tables its query reads.

Everything else changes the catalog alone and takes no comment: `DROP COLUMN`, `DROP CONSTRAINT`, `DROP INDEX`, `SET DEFAULT`, `RENAME`, `COMMENT ON`, the row-level security toggles, the storage parameters, a constraint added `NOT VALID`, and a column widened from `varchar(50)` to `varchar(100)` or to `text`. So does every statement on a table the same plan creates, since that table is empty.


## What it reads

The classification comes from a table in pistachio, applied to the statement it is about to print. Three things cannot be decided that way and are asked of the server: the rows and bytes, one read of `pg_class`; whether a column type change is a binary-coercible relabel or a conversion, one read of `pg_cast`; and whether a column default calls a volatile function, one read of `pg_proc`. The last two run only when the plan holds such a statement, and a plan with no statements reads none of the three, so a run that finds no drift costs nothing extra.


## What it does not say

The comment is about the statement, not the data. Whether a `SET NOT NULL` finds a NULL, or an `ADD CONSTRAINT ... UNIQUE` finds a duplicate, is between the server and the rows.

It reports the lock the statement takes, not the wait to get it. An ACCESS EXCLUSIVE lock queues behind every transaction already reading the table, and everything arriving later queues behind the lock, so even a statement that changes the catalog alone can stop the table for as long as one old transaction runs. `--pre-sql "SET lock_timeout = '5s'"` bounds that wait.

Two cases read coarser than PostgreSQL treats them. A binary-coercible type change that still changes an index's operator class, `integer` to `oid`, rebuilds that index and takes no comment. An `ALTER TABLE` on an `INHERITS` parent counts every child even for `ADD CONSTRAINT ... PRIMARY KEY` and `ADD CONSTRAINT ... FOREIGN KEY`, which do not recurse; pistachio writes foreign keys with `ONLY`, so nothing it emits reaches this.
