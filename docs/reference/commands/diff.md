# pista diff

Print the DDL that takes one schema file to another.

## Synopsis

```
pista diff [option...] current-file desired-file
pista diff [option...] --git range file...
```

## Description

`pista diff` compares two schema SQL files and prints the DDL that takes the first to the second. No database is read: the first file stands in for the current schema [`pista plan`](plan.md) reads from the catalog, and the second is the desired schema.

The output follows the same rules as `plan`: the same DDL, in the same order, under the same drop policy. Diffing a file against `pista dump` output previews a plan for that database without a connection. The output has no header, since there is no connection to name.

With `--git` the files named on the command line are read out of the repository at the two revisions a range names, and the whole list is one schema on each side.

A `-- pista:execute` statement is not part of the output. It is not schema state, and its check SQL cannot be evaluated without a database.

## Options

The [general options](index.md#general-options) apply as well.

### Scope

`-n` *schema*, `--schemas=`*schema*
:   Schemas to compare. Repeatable. A name written without a schema is qualified with the first, and an object outside these schemas is out of scope on both sides. Default: `public`. Environment: `PISTA_SCHEMAS`.

`-I` *pattern*, `--include=`*pattern*
:   Compare only the objects whose name matches. `*` and `?` are wildcards and match the whole name; `/re/` is a regular expression and matches anywhere unless anchored. Repeatable. Environment: `PISTA_INCLUDE`.

`-E` *pattern*, `--exclude=`*pattern*
:   Leave out the objects whose name matches. Same patterns as `--include`. Repeatable. Environment: `PISTA_EXCLUDE`.

`--enable=`*type*
:   Compare only these object types: `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`. Repeatable. Takes precedence over `--disable`. Environment: `PISTA_ENABLE`.

`--disable=`*type*
:   Leave out these object types. Same values as `--enable`. Repeatable. Environment: `PISTA_DISABLE`.

`--manage-routine`
:   Compare functions and procedures. Off by default. Environment: `PISTA_MANAGE_ROUTINE`.

`--manage-storage-param`
:   Compare the storage parameters of tables and materialized views. Off by default. Environment: `PISTA_MANAGE_STORAGE_PARAM`.

`--skip-partition-child`
:   Compare a partitioned table without its partitions. Environment: `PISTA_SKIP_PARTITION_CHILD`.

### Input

`-g` *range*, `--git=`*range*
:   Read the files from git. `A..B` compares revision `A` to `B`, `A...B` compares the merge base of `A` and `B` to `B`, and `A` alone compares `A` to the working tree. An omitted side is `HEAD`. Any revision spelling git accepts works. Environment: `PISTA_GIT`. See [Notes](#notes).

### Statements

`--allow-drop=`*type*
:   Allow dropping these object types. Same values as for [`plan`](plan.md#statements). Without it each drop is written as a `-- skipped:` comment. Environment: `PISTA_ALLOW_DROP`.

`--disable-index-concurrently`
:   Ignore every `CONCURRENTLY` opt-in on both sides and write plain `CREATE INDEX` and `DROP INDEX`. Conflicts with `--force-index-concurrently`. Environment: `PISTA_DISABLE_INDEX_CONCURRENTLY`.

`--force-index-concurrently`
:   Write `CONCURRENTLY` on every `CREATE INDEX` and `DROP INDEX`. Environment: `PISTA_FORCE_INDEX_CONCURRENTLY`.

`--bulk-alter`
:   Merge consecutive `ALTER TABLE` actions on one table into one statement. Environment: `PISTA_BULK_ALTER`.

`--assume-validated`
:   Treat every constraint and foreign key as validated. Environment: `PISTA_ASSUME_VALIDATED`.

### Output

`--explain`
:   Comment each statement that scans or rewrites a table with what it does and what its lock blocks. No database is read, so no size is shown, and a type change or a default that calls a function reads as `may rewrite`. Environment: `PISTA_EXPLAIN`. See [Explaining a plan](../../guides/explaining-plans.md#in-a-diff).

`--check`
:   Exit with 2 when the diff holds executable DDL. Environment: `PISTA_CHECK`.

## Exit status

0 when the diff was printed, 1 on error, 80 on a usage error. With `--check`, 2 when the diff holds executable DDL. A skipped drop alone exits with 0.

## Environment

Every option names its variable above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment). `--git` needs `git` on `PATH`.

## Notes

Under `--git`, paths resolve against the working directory, not the repository root, so the path `plan` takes is the path to pass here. Every file is read at both ends of the range. A file one side does not hold is empty there, so a file added between the two revisions reads as a create and one removed reads as a drop. A path neither side holds is an error.

The file list itself is not read from git. A shell glob expands against the working tree, so a file the branch deleted is never named and its drop goes unreported, `--check` included. Name such a file.

Directives on the current side count as its state. A `-- pista:concurrently` there makes a dropped index `DROP INDEX CONCURRENTLY`, which a plan against a database cannot know. A `-- pista:renamed-from` on the desired side resolves against the current side's names.

## Examples

Two files:

```bash
pista diff old.sql new.sql
```

The last commit, and a branch against its merge base with main:

```bash
pista diff --git HEAD^..HEAD schema.sql
pista diff --git origin/main...HEAD schema/tables.sql schema/indexes.sql
```

Uncommitted edits:

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
