# Output

`plan`, `apply` and `dump` write SQL. Everything that is not a statement is a SQL comment, so the output pipes into `psql` as it is.

## Header

The commands that read a database open with the connection and a count of what they found:

```sql
-- Connected to postgres://postgres@localhost:5432/postgres
-- Plan for schema public (2 tables, 1 view, 0 enums, 0 domains, 0 composite types, 0 sequences)
```

The second line reads `Apply to` under `apply` and `Dump of` under `dump`. `diff` writes no header, and `dump --json` writes none either.

## Body

`-- No changes`
:   The plan holds no executable DDL. Closes the output of `plan`, `apply` and `diff`.

`-- skipped: <statement>`
:   A drop that `--allow-drop` does not allow. The statement is shown and not run. See [Controlling drops](../guides/drops.md).

`-- ignored: <name>`
:   An object a `-- pista:ignore` directive leaves out. See [Directives](directives.md#-pistaignore).

`-- check SQL could not be evaluated at plan time: <error>; apply will decide`
:   Written between a `-- pista:execute` directive and its statement when `plan` cannot run the check. See [Directives](directives.md#-pistaexecute).

`-- <verb>, <lock>: <table> (<size>)`
:   The `--explain` comment before a statement that scans or rewrites a table. See [Explaining a plan](../guides/explaining-plans.md).

## Apply

`-- Transaction started`, `-- Transaction committed`, `-- Transaction rolled back`
:   The bounds of the transaction `--with-tx` or `--try-tx` opens.

`-- Transaction skipped: plan contains CONCURRENTLY index DDL`
:   `--try-tx` ran without a transaction.

`-- Waiting for another exclusive apply to finish`
:   `--exclusive-wait` is waiting. Written at once, before the rest of the output.

`-- Time: <n> ms`
:   The elapsed time of the statement above it, under `--timing`.

`-- Warning: the database has drifted since the plan was written`
:   `apply-from --force` ran a plan in spite of drift.

`-- Apply finished in <duration>`
:   Closes an apply that ran at least one statement.

## Dump

`-- <schema>.<name>`
:   Precedes each object. Under `--explain` the table's size follows the name. See [Sizes in a dump](../guides/explaining-plans.md#sizes-in-a-dump).

`-- Wrote <n> file(s) to <dir>`
:   Closes `dump --split`.

## Standard error

Warnings and errors go to standard error, prefixed with `pista:`. A statement pistachio does not read is warned about once, with its position:

```
pista: schema.sql:12:1: ignored unsupported statement: GRANT select ON public.users TO app
```

An error stops the command:

```
pista: error: cannot drop public.staff: view public.eng_staff depends on it
```
