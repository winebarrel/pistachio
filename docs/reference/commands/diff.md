# pista diff

Print the DDL that changes one schema file into another.

## Synopsis

```
pista diff [option...] current-file desired-file
pista diff [option...] --git range file...
```

## Description

`pista diff` compares two schema SQL files and prints the DDL that changes the first into the second. No database is read. The first file takes the place of the current schema that [`pista plan`](plan.md) reads from the catalog. The second file is the desired schema.

The output follows the same rules as `plan`: the same DDL, in the same order, under the same drop policy. A diff between `pista dump` output and a file previews the plan for that database, without a connection. The output has no header, because there is no connection to name.

With `--git`, the files named on the command line are read from the repository at the two revisions that the range names. On each side, the whole list of files is one schema.

A `-- pista:execute` statement is not part of the output. It is not schema state, and its check SQL cannot be evaluated without a database.

## Options

The [general options](index.md#general-options) apply as well.

### Scope

`-n` *schema*, `--schemas=`*schema*
:   The schemas to compare. This option can be given more than once. A name written without a schema is qualified with the first one. An object outside these schemas is out of scope on both sides. The default is `public`. The environment variable is `PISTA_SCHEMAS`.

`-I` *pattern*, `--include=`*pattern*
:   Compare only the objects whose name matches the pattern. `*` and `?` are wildcards. A wildcard pattern must match the whole name. `/re/` is a regular expression. It matches anywhere in the name unless it is anchored. This option can be given more than once. The environment variable is `PISTA_INCLUDE`.

`-E` *pattern*, `--exclude=`*pattern*
:   Leave out the objects whose name matches the pattern. The patterns are the same as for `--include`. This option can be given more than once. The environment variable is `PISTA_EXCLUDE`.

`--enable=`*type*
:   Compare only these object types: `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`. This option can be given more than once. It takes precedence over `--disable`. The environment variable is `PISTA_ENABLE`.

`--disable=`*type*
:   Leave out these object types. The values are the same as for `--enable`. This option can be given more than once. The environment variable is `PISTA_DISABLE`.

`--manage-routine`
:   Compare functions and procedures. This option is off by default. The environment variable is `PISTA_MANAGE_ROUTINE`.

`--manage-storage-param`
:   Compare the storage parameters of tables and materialized views. This option is off by default. The environment variable is `PISTA_MANAGE_STORAGE_PARAM`.

`--skip-partition-child`
:   Compare a partitioned table without its partitions. The environment variable is `PISTA_SKIP_PARTITION_CHILD`.

### Input

`-g` *range*, `--git=`*range*
:   Read the files from git. `A..B` compares revision `A` to `B`. `A...B` compares the merge base of `A` and `B` to `B`. `A` alone compares `A` to the working tree. An omitted side is `HEAD`. Any revision spelling that git accepts works. The environment variable is `PISTA_GIT`. See [Notes](#notes).

### Statements

`--allow-drop=`*type*
:   Allow dropping these object types. The values are the same as for [`plan`](plan.md#statements). Without it, each drop is written as a `-- skipped:` comment. The environment variable is `PISTA_ALLOW_DROP`.

`--disable-index-concurrently`
:   Ignore every `CONCURRENTLY` opt-in on both sides. Write plain `CREATE INDEX` and `DROP INDEX` instead. This option cannot be used with `--force-index-concurrently`. The environment variable is `PISTA_DISABLE_INDEX_CONCURRENTLY`.

`--force-index-concurrently`
:   Write `CONCURRENTLY` on every `CREATE INDEX` and `DROP INDEX`. The environment variable is `PISTA_FORCE_INDEX_CONCURRENTLY`.

`--bulk-alter`
:   Merge consecutive `ALTER TABLE` actions on one table into one statement. The environment variable is `PISTA_BULK_ALTER`.

`--assume-validated`
:   Treat every constraint and foreign key as validated. The environment variable is `PISTA_ASSUME_VALIDATED`.

### Output

`--explain`
:   Add a comment to each statement that scans or rewrites a table. The comment says what the statement does and what its lock blocks. No database is read, so no size is shown. A type change, or a default that calls a function, is reported as `may rewrite`. The environment variable is `PISTA_EXPLAIN`. See [Explaining a plan](../../guides/explaining-plans.md#in-a-diff).

`--check`
:   Exit with 2 when the diff contains executable DDL. The environment variable is `PISTA_CHECK`.

## Exit status

The exit status is 0 when the diff was printed, 1 on error, and 80 on a usage error. With `--check`, it is 2 when the diff contains executable DDL. A skipped drop alone exits with 0.

## Environment

Every option names its variable above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment). `--git` needs `git` on `PATH`.

## Notes

Under `--git`, paths resolve against the working directory, not against the repository root. So the path that `plan` accepts is the path to pass here. Every file is read at both ends of the range. A file that one side does not contain is empty on that side. So a file added between the two revisions is reported as a create, and a file removed between them is reported as a drop. A path that neither side contains is an error.

The file list itself is not read from git. A shell glob expands against the working tree. So a file that the branch deleted is never named, and its drop is not reported, not even with `--check`. Name such a file on the command line.

Directives on the current side count as its state. A `-- pista:concurrently` on the current side makes a dropped index a `DROP INDEX CONCURRENTLY`. `plan` against a database has no such directive. A `-- pista:renamed-from` on the desired side resolves against the names on the current side.

## Examples

Compare two files:

```bash
pista diff old.sql new.sql
```

Compare the last commit, and a branch against its merge base with main:

```bash
pista diff --git HEAD^..HEAD schema.sql
pista diff --git origin/main...HEAD schema/tables.sql schema/indexes.sql
```

Compare uncommitted edits:

```bash
pista diff --git HEAD schema.sql
```

Fail CI when a branch changes the schema:

```bash
pista diff --check --git origin/main...HEAD schema.sql
echo $?  # 0: no changes, 2: changes, 1: error
```

## See also

[`pista plan`](plan.md), [Diffing schema files](../../guides/diffing.md)
