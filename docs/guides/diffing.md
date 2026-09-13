# Diffing schema files

`pista diff` compares two schema SQL files and prints the DDL that takes the first to the second. No database is read: the first file stands in for the current state `plan` reads from the catalog, the second is the desired state.

```bash
pista diff old.sql new.sql
```


## An example

`old.sql`:

```sql
CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

`new.sql`:

```sql
CREATE TABLE users (
    id integer NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_email ON users (email);
```

```sql
$ pista diff old.sql new.sql
-- Diff for schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
ALTER TABLE public.users ADD COLUMN email text;
CREATE INDEX idx_users_email ON public.users USING btree (email);
```


## The same rules as plan

Both files go through the parser `plan` and `apply` use, and the diff is the one `plan` computes: the same statements, in the same order, under the same drop policy. Diffing a file against `pista dump` output previews a plan for that database without a connection.

Drops are suppressed by default and printed as comments; `--allow-drop` opts in. See [Controlling drops](drops.md).

The filter options, `--bulk-alter`, `--assume-validated` and the index CONCURRENTLY flags work as they do for `plan`. Directives too: a `-- pista:renamed-from` in the second file resolves against the first file's names.


## Checking two revisions in CI

`--check` exits with code 2 when the diff contains executable changes, 0 when not, and 1 on error. Suppressed drops alone exit 0, as with `plan --check`.

```bash
git show main:schema.sql > /tmp/main-schema.sql
pista diff --check /tmp/main-schema.sql schema.sql
```


## What the first file means

The first file plays the catalog's role. Only objects in the target schemas (`-n` / `--schemas`) are compared; an object outside them is out of scope on both sides, not a drop. Functions and procedures are read only under `--manage-routine`, and a sequence a column owns is unmanaged, as in the catalog.

Directives in the first file count as its state. A `-- pista:concurrently` there makes a dropped index `DROP INDEX CONCURRENTLY`, which a plan against a database cannot know; `--disable-index-concurrently` clears it on both sides.


## Execute directives

A [`-- pista:execute`](executing-sql.md) statement in the desired file is kept in the output. Its check SQL cannot be evaluated without a database, so the statement carries a note and `apply` decides:

```sql
-- pista:execute SELECT NOT EXISTS (SELECT 1 FROM users)
-- check SQL is not evaluated without a database; apply will decide
INSERT INTO users (id) VALUES (1);
```


## What it is not

`diff` compares two files, not a database. The drift check is `pista plan --check`; `diff` answers what changed between two versions of the schema.
