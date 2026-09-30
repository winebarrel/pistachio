# pista diff

Print the DDL that takes one schema file to another.

## Synopsis

```
pista diff [option...] current-file desired-file
pista diff [option...] --git range file...
```

## Description

`pista diff` compares two schema SQL files and prints the DDL that takes the first to the second. No database is read: the first file stands in for the current schema that [`pista plan`](plan.md) reads from the catalog, and the second is the desired schema.

The output follows the same rules as `plan`: the same DDL, in the same order, under the same drop policy. Diffing a file against `pista dump` output previews a plan for that database without a connection. The output has no header, since there is no connection to name.

With `--git` the files named on the command line are read out of the repository at the two revisions that a range names, and the whole list is one schema on each side.

A `-- pista:execute` statement is not part of the output. It is not schema state, and its check SQL cannot be evaluated without a database.

## Options

The [general options](index.md#general-options) apply as well.

### Scope

`-n` *schema*, `--schemas=`*schema*
:   The schemas to compare. This option can be given more than once. A name written without a schema is qualified with the first, and an object outside these schemas is out of scope on both sides. The default is `public`. The environment variable is `PISTA_SCHEMAS`.

`-I` *pattern*, `--include=`*pattern*
:   Compare only the objects whose name matches. `*` and `?` are wildcards and match the whole name. `/re/` is a regular expression and matches anywhere unless anchored. This option can be given more than once. The environment variable is `PISTA_INCLUDE`.

`-E` *pattern*, `--exclude=`*pattern*
:   Leave out the objects whose name matches. The patterns are the same as for `--include`. This option can be given more than once. The environment variable is `PISTA_EXCLUDE`.

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
:   Read the files from git. `A..B` compares revision `A` to `B`, `A...B` compares the merge base of `A` and `B` to `B`, and `A` alone compares `A` to the working tree. An omitted side is `HEAD`. Any revision spelling that git accepts works. The environment variable is `PISTA_GIT`. See [Notes](#notes).

### Statements

`--allow-drop=`*type*
:   Allow dropping these object types. The values are the same as for [`plan`](plan.md#statements). Without it each drop is written as a `-- skipped:` comment. The environment variable is `PISTA_ALLOW_DROP`.

`--disable-index-concurrently`
:   Ignore every `CONCURRENTLY` opt-in on both sides and write plain `CREATE INDEX` and `DROP INDEX`. This option cannot be used with `--force-index-concurrently`. The environment variable is `PISTA_DISABLE_INDEX_CONCURRENTLY`.

`--force-index-concurrently`
:   Write `CONCURRENTLY` on every `CREATE INDEX` and `DROP INDEX`. The environment variable is `PISTA_FORCE_INDEX_CONCURRENTLY`.

`--bulk-alter`
:   Merge consecutive `ALTER TABLE` actions on one table into one statement. The environment variable is `PISTA_BULK_ALTER`.

`--assume-validated`
:   Treat every constraint and foreign key as validated. The environment variable is `PISTA_ASSUME_VALIDATED`.

### Output

`--explain`
:   Comment each statement that scans or rewrites a table with what it does and what its lock blocks. No database is read, so no size is shown, and a type change or a default that calls a function reads as `may rewrite`. The environment variable is `PISTA_EXPLAIN`. See [Explaining a plan](../../guides/explaining-plans.md#in-a-diff).

`--check`
:   Exit with 2 when the diff holds executable DDL. The environment variable is `PISTA_CHECK`.

## Exit status

The exit status is 0 when the diff was printed, 1 on error, and 80 on a usage error. With `--check`, it is 2 when the diff holds executable DDL. A skipped drop alone exits with 0.

## Environment

Every option names its variable above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment). `--git` needs `git` on `PATH`.

## Notes

Under `--git`, paths resolve against the working directory, not the repository root, so the path that `plan` takes is the path to pass here. Every file is read at both ends of the range. A file that one side does not hold is empty there, so a file added between the two revisions reads as a create and one removed reads as a drop. A path that neither side holds is an error.

The file list itself is not read from git. A shell glob expands against the working tree, so a file that the branch deleted is never named and its drop goes unreported, `--check` included. Name such a file.

Directives on the current side count as its state. A `-- pista:concurrently` there makes a dropped index `DROP INDEX CONCURRENTLY`, which a plan against a database cannot know. A `-- pista:renamed-from` on the desired side resolves against the current side's names.

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
