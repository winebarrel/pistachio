# Explaining a plan

A plan says what will run, not what it costs. `--explain` writes a comment before each statement that reads or rewrites a table that already contains data.

```sql
$ pista plan --explain schema.sql
-- rewrite, blocks reads and writes: public.orders (~2,000,000 rows, 210 MB, as of 2026-09-09, 3 indexes rebuilt)
ALTER TABLE public.orders ALTER COLUMN amount SET DATA TYPE numeric(12,2);
-- scan, blocks writes: public.orders (~2,000,000 rows, 210 MB, as of 2026-09-09), public.customers (~50,000 rows, 6280 kB, as of 2026-07-21)
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers (id);
-- scan, blocks nothing: public.orders (~2,000,000 rows, 210 MB, as of 2026-09-09)
CREATE INDEX CONCURRENTLY orders_created_at_idx ON public.orders USING btree (created_at);
-- scan, blocks writes: public.events (~120,000,000 rows, 9629 MB, as of 2026-09-08, 24 partitions)
CREATE INDEX events_at_idx ON public.events USING btree (at);
ALTER TABLE public.orders ALTER COLUMN note SET DEFAULT '';
```

The option is also available as `$PISTA_EXPLAIN`, and as `$PISTA_DUMP_EXPLAIN` for `dump`. The comments are SQL comments, so the output can still be piped into `psql`.


## What the comment says

The first word says what the statement does to the rows that already exist. `rewrite` copies the table into a new file and builds every index on it again. So it needs as much free disk space as the table takes, and it writes that much WAL for a replica to replay. `scan` reads every row once and writes no new heap. A constraint validation, an index build and a NOT NULL check do this.

The phrase after it says what the statement's lock blocks while the statement runs. `blocks reads and writes` is ACCESS EXCLUSIVE; even a `SELECT` waits. `blocks writes` is SHARE or SHARE ROW EXCLUSIVE. `blocks nothing` is SHARE UPDATE EXCLUSIVE or weaker; only another DDL on the same table waits.

Then come the tables that the statement touches, each with its size. The rows and bytes are the estimates that VACUUM and ANALYZE last wrote to `pg_class`, including the pages of the TOAST relation. So the comment does not read the table itself. A table that neither VACUUM nor ANALYZE has visited is shown as `not analyzed`.

`as of` is the date of that last VACUUM or ANALYZE, including autovacuum. It tells you how reliable the numbers next to it are. A table written heavily since then is larger than shown. A table loaded and never analyzed since then is shown as a year old. The date is in the local time zone. Over several tables, it is the oldest of them. It is left out when the server no longer has the time, after a statistics reset or a `pg_upgrade`. `CREATE INDEX` also writes the row estimate, but keeps no time of its own. So an estimate is sometimes newer than the date says.

A rewrite also says how many indexes it builds again. A partitioned table shows the sum of its partitions and their number. An `INHERITS` parent shows its own rows plus its children's rows. A foreign key lists the referenced table next to the referencing one, because both are locked. A domain change lists every table with a column of the domain.


## Which statements get one

A rewrite comes from:

- a column type change that is not a widening
- `ADD COLUMN` with a volatile default, an identity, a generated expression or a serial type
- `SET LOGGED` and `SET UNLOGGED`

Each takes ACCESS EXCLUSIVE.

A scan comes from:

- `SET NOT NULL`
- `ADD COLUMN ... NOT NULL` without a default
- `ADD CONSTRAINT` for a check, a primary key, a unique key or a foreign key
- `VALIDATE CONSTRAINT`
- `CREATE INDEX`

The lock differs between them:

- a foreign key takes SHARE ROW EXCLUSIVE on both tables
- `VALIDATE CONSTRAINT` and `CREATE INDEX CONCURRENTLY` take SHARE UPDATE EXCLUSIVE
- a plain `CREATE INDEX` takes SHARE
- the rest take ACCESS EXCLUSIVE

`ALTER DOMAIN` scans every table with a column of the domain when it sets NOT NULL, adds a validated constraint, or validates one. `CREATE MATERIALIZED VIEW` scans the tables that its query reads.

Everything else changes only the catalog and gets no comment:

- `DROP COLUMN`, `DROP CONSTRAINT` and `DROP INDEX`
- `SET DEFAULT`, `RENAME` and `COMMENT ON`
- the row-level security toggles
- the storage parameters
- a constraint added `NOT VALID`
- a column widened from `varchar(50)` to `varchar(100)` or to `text`

The same applies to every statement on a table that the same plan creates, because that table is empty.


## What it reads

The classification comes from a table inside pistachio, applied to the statement that it is about to print. Three things cannot be decided that way, so pistachio asks the server:

- whether a column type change is a binary-coercible relabel or a conversion: one read of `pg_cast`
- whether a column default calls a volatile function: one read of `pg_proc`
- the rows and bytes, and the date when they were written: one read of `pg_class`, with the vacuum and analyze times that the server keeps in its statistics

Each read is skipped when nothing in the plan needs it. So a plan that only creates and drops tables, or a plan with no statements at all, reads no more than it would without the flag.


## What it does not say

The comment is about the statement, not the data. It does not say whether a `SET NOT NULL` will find a NULL, or whether an `ADD CONSTRAINT ... UNIQUE` will find a duplicate. That depends on the rows.

It reports the lock that the statement takes, not the wait to get it. An ACCESS EXCLUSIVE lock queues behind every transaction that is already reading the table. Everything that arrives later queues behind the lock. So even a statement that changes only the catalog can stop the table for as long as one old transaction runs. `--pre-sql "SET lock_timeout = '5s'"` limits that wait.

In two cases the comment is coarser than what PostgreSQL does. A binary-coercible type change that still changes an index's operator class, such as `integer` to `oid`, rebuilds that index but gets no comment. An `ALTER TABLE` on an `INHERITS` parent counts every child, even for `ADD CONSTRAINT ... PRIMARY KEY` and `ADD CONSTRAINT ... FOREIGN KEY`, which do not recurse. pistachio writes foreign keys with `ONLY`, so nothing that it emits reaches this case.


## In a diff

`pista diff --explain` writes the same comment without a database. A table includes the number of indexes that a rewrite builds again, written as `may be rebuilt` for `may rewrite`, and its number of partitions, but no size. A column type change, and an added column whose default calls a function, are shown as `may rewrite`. `plan` asks the server whether the change is a relabel and whether the function is volatile. `diff` has no server to ask.

```sql
$ pista diff --explain old.sql new.sql
-- may rewrite, blocks reads and writes: public.orders (2 indexes may be rebuilt)
ALTER TABLE public.orders ALTER COLUMN amount SET DATA TYPE bigint;
-- scan, blocks writes: public.orders
CREATE INDEX orders_note_idx ON public.orders USING btree (note);
```


## Sizes in a dump

`pista dump --explain` writes the same size after the name of each table and materialized view. It writes the size of each index in a comment above the index.

```sql
-- public.events (~120,000,000 rows, 9629 MB, as of 2026-09-08, 24 partitions)
CREATE TABLE public.events (
    id bigint NOT NULL,
    at date NOT NULL
)
PARTITION BY RANGE (at);
-- 2573 MB
CREATE INDEX events_at_idx ON ONLY public.events USING btree (at);
```

A partitioned table sums its partitions. An index on it sums the indexes on the partitions. A partition counts even when `-I`, `-E` or `--skip-partition-child` leaves it out of the dump. It does not count when it is in a schema that `-n` does not name. An `INHERITS` parent counts only its own rows, because each child has a comment of its own.

An index size has no date. VACUUM and ANALYZE write it together with the table's size, so the table's date applies. An index owned by a primary key, unique or exclusion constraint is written inside `CREATE TABLE`, and has no comment.
