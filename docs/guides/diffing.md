# Diffing schema files

`pista diff` compares two schema SQL files and prints the DDL that changes the first into the second. No database is read. The first file takes the place of the current state that `plan` reads from the catalog. The second file is the desired state.

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

Both files go through the parser that `plan` and `apply` use. The diff is the one that `plan` computes: the same schema DDL, in the same order, under the same drop policy. Diffing a file against `pista dump` output previews a plan for that database, without a connection.

By default a drop is skipped and printed as a comment. `--allow-drop` enables it. See [Controlling drops](drops.md).

The filter options, `--bulk-alter`, `--assume-validated` and the index CONCURRENTLY flags work as they do for `plan`. Directives do too. A `-- pista:renamed-from` on the desired side is resolved against the names on the current side.


## Reading the files from git

`--git` reads the files named on the command line from the repository, at the revisions that a range specifies.

The range follows the rules that `git diff` uses:

| Range | Current side | Desired side |
| --- | --- | --- |
| `A..B` | `A` | `B` |
| `A...B` | the merge base of `A` and `B` | `B` |
| `A` | `A` | the working tree |

An omitted side of `A..B` or `A...B` means `HEAD`. Each side is one revision, so any spelling that git accepts works: `HEAD^`, `HEAD~3`, a tag, a branch, an abbreviated SHA, `HEAD@{yesterday}`.

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

A range that names one revision compares it against the working tree. So an edit that is not committed yet is part of the diff. Here `created_at` is added to the file and not committed:

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

Paths are resolved against the working directory, not the repository root. So pass the same path that you pass to `plan`. Every file is read at both ends of the range. The files of one side form one schema:

```bash
pista diff --git main..HEAD schema/tables.sql schema/indexes.sql
```

A file that does not exist on one side is empty there. So a file added between the two revisions is shown as a create, and a file removed between them is shown as a drop. A path that exists on neither side is an error. Otherwise, a mistyped path would be read as an empty schema and produce a drop of everything.

The list of files itself is not read from git. A shell glob expands against the working tree. So a file that the branch deleted is never named, and its drop is not reported, even with `--check`:

```bash
pista diff --check --git origin/main...HEAD schema/*.sql   # misses a deleted schema/b.sql
```

A file that exists on only one side must be named.

`git` must be on `PATH`.


## Checking a branch in CI

`--check` exits with code 2 when the diff contains executable changes. It exits with 0 when it does not, and with 1 on error. A skipped drop alone exits with 0, as with `plan --check`.

```bash
pista diff --check --git origin/main...HEAD schema.sql
```


## What the current side means

The current side plays the role of the catalog. Only objects in the target schemas (`-n` / `--schemas`) are compared. An object outside them is out of scope on both sides; it is not a drop. Functions and procedures are read only under `--manage-routine`. A sequence that a column owns is left out, as in the catalog. See [Sequences](../reference/objects.md#sequences).

Directives on the current side count as part of its state. A `-- pista:concurrently` there makes a dropped index a `DROP INDEX CONCURRENTLY`. `plan` against a database has no such directive, so it writes a plain `DROP INDEX`. `--disable-index-concurrently` clears the directive on both sides.


## Execute directives

A [`-- pista:execute`](executing-sql.md) statement is not part of the diff. It is not schema state, and its check SQL cannot be evaluated without a database. So `diff` prints only the schema DDL. `apply` runs execute statements as usual.


## What it is not

`diff` compares two schema files, not a database. The drift check is `pista plan --check`. `diff` shows what changed between two versions of the schema.
