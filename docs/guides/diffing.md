# Diffing schema files

`pista diff` compares two schema SQL files and prints the DDL that takes the first to the second. No database is read: the first file stands in for the current state `plan` reads from the catalog, and the second is the desired state.

```bash
pista diff old.sql new.sql
```

`--git` reads the two sides out of a git repository instead.

```bash
pista diff --git HEAD^..HEAD schema.sql
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
ALTER TABLE public.users ADD COLUMN email text;
CREATE INDEX idx_users_email ON public.users USING btree (email);
```


## The same rules as plan

Both files go through the parser `plan` and `apply` use, and the diff is the one `plan` computes: the same schema DDL, in the same order, under the same drop policy. Diffing a file against `pista dump` output previews a plan for that database without a connection.

Drops are suppressed by default and printed as comments; `--allow-drop` opts in. See [Controlling drops](drops.md).

The filter options, `--bulk-alter`, `--assume-validated` and the index CONCURRENTLY flags work as they do for `plan`. Directives too: a `-- pista:renamed-from` on the desired side resolves against the current side's names.


## Reading the files from git

`--git` reads the files named on the command line out of the repository, at the revisions a range names.

The range follows the rules `git diff` uses:

| Range | Current side | Desired side |
| --- | --- | --- |
| `A..B` | `A` | `B` |
| `A...B` | the merge base of `A` and `B` | `B` |
| `A` | `A` | the working tree |

An omitted side of `A..B` or `A...B` is `HEAD`. Each side is one revision, so any spelling git accepts works: `HEAD^`, `HEAD~3`, a tag, a branch, an abbreviated SHA, `HEAD@{yesterday}`.

`schema.sql` at `HEAD^`:

```sql
CREATE TABLE users (
    id integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
```

The commit at `HEAD` adds a column and an index:

```sql
CREATE TABLE users (
    id integer NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_email ON users (email);
```

```sql
$ pista diff --git HEAD^..HEAD schema.sql
ALTER TABLE public.users ADD COLUMN email text;
CREATE INDEX idx_users_email ON public.users USING btree (email);
```

A range naming one revision compares it against the working tree, so an edit that is not committed yet is part of the diff. With `created_at` added to the file and not committed:

```sql
$ pista diff --git HEAD schema.sql
ALTER TABLE public.users ADD COLUMN created_at timestamp with time zone DEFAULT now() NOT NULL;
```

```sql
$ pista diff --git HEAD^ schema.sql
ALTER TABLE public.users ADD COLUMN email text;
ALTER TABLE public.users ADD COLUMN created_at timestamp with time zone DEFAULT now() NOT NULL;
CREATE INDEX idx_users_email ON public.users USING btree (email);
```


## Paths and missing files

Paths resolve against the working directory, not the repository root, so the path `plan` takes is the path to pass here. Every file is read at both ends of the range, and the files of a side are one schema:

```bash
pista diff --git main..HEAD schema/tables.sql schema/indexes.sql
```

A file one side does not hold is empty there, so a file added between the two revisions reads as a create and one removed reads as a drop. A path neither side holds is an error; otherwise a mistyped path would read as an empty schema and plan a drop of everything.

The list itself is not read from git. A shell glob expands against the working tree, so a file the branch deleted is never named and its drop goes unreported, `--check` included:

```bash
pista diff --check --git origin/main...HEAD schema/*.sql   # misses a deleted schema/b.sql
```

A file only one side holds has to be named.

`git` has to be on `PATH`.


## Checking a branch in CI

`--check` exits with code 2 when the diff contains executable changes, 0 when not, and 1 on error. Suppressed drops alone exit 0, as with `plan --check`.

```bash
pista diff --check --git origin/main...HEAD schema.sql
```


## What the current side means

The current side plays the catalog's role. Only objects in the target schemas (`-n` / `--schemas`) are compared; an object outside them is out of scope on both sides, not a drop. Functions and procedures are read only under `--manage-routine`, and a sequence a column owns is unmanaged, as in the catalog.

Directives on the current side count as its state. A `-- pista:concurrently` there makes a dropped index `DROP INDEX CONCURRENTLY`, which a plan against a database cannot know; `--disable-index-concurrently` clears it on both sides.


## Execute directives

A [`-- pista:execute`](executing-sql.md) statement is not part of the diff. It is not schema state, and its check SQL cannot be evaluated without a database, so `diff` prints the schema DDL alone. `apply` runs execute statements as usual.


## What it is not

`diff` compares two schema files, not a database. The drift check is `pista plan --check`; `diff` answers what changed between two versions of the schema.
