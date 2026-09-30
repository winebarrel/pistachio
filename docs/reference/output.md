# Output

`plan`, `apply` and `dump` write SQL. Everything that is not a statement is a SQL comment. The output can therefore be piped into `psql` as it is.

## Header

The commands that read a database start with the connection and a count of the objects they found:

```sql
-- Connected to postgres://postgres@localhost:5432/postgres
-- Plan for schema public (2 tables, 1 view, 0 enums, 0 domains, 0 composite types, 0 sequences)
```

The second line begins with `Apply to` under `apply` and with `Dump of` under `dump`. `diff` writes no header. `dump --json` writes none either.

## Body

`-- No changes`
:   The plan contains no executable DDL. This line closes the output of `plan`, `apply` and `diff`.

`-- skipped: <statement>`
:   A drop that `--allow-drop` does not allow. The statement is shown but not run. See [Controlling drops](../guides/drops.md).

`-- ignored: <name>`
:   An object that a `-- pista:ignore` directive excludes, or a routine with `SET ... FROM CURRENT`. See [Directives](directives.md#-pistaignore).

`-- check SQL could not be evaluated at plan time: <error>; apply will decide`
:   `plan` writes this comment between a `-- pista:execute` directive and its statement when it cannot run the check. See [Directives](directives.md#-pistaexecute).

`-- <verb>, <lock>: <table> (<size>)`
:   The `--explain` comment before a statement that scans or rewrites a table. See [Explaining a plan](../guides/explaining-plans.md).

## Apply

`-- Transaction started`, `-- Transaction committed`, `-- Transaction rolled back`
:   The start and end of the transaction that `--with-tx` or `--try-tx` opens.

`-- Transaction skipped: plan contains CONCURRENTLY index DDL`
:   `--try-tx` ran without a transaction.

`-- Waiting for another exclusive apply to finish`
:   `--exclusive-wait` is waiting. This line is written immediately, before the rest of the output.

`-- Time: <n> ms`
:   The elapsed time of the statement above it, under `--timing`.

`-- Warning: the database has drifted since the plan was written`
:   `apply-from --force` ran a plan despite drift.

`-- Apply finished in <duration>`
:   This line closes an apply that ran at least one statement. The time covers the statements and the writing of the output.

## Dump

`-- <schema>.<name>`
:   This comment comes before each object. Under `--explain`, the table size follows the name. See [Sizes in a dump](../guides/explaining-plans.md#sizes-in-a-dump).

`-- Wrote <n> file(s) to <dir>`
:   This line closes `dump --split`.

## Standard error

Warnings and errors are written to standard error with the prefix `pista:`. pistachio warns once about each statement that it does not read, and includes its position:

```
pista: schema.sql:12:1: ignored unsupported statement: GRANT select ON public.users TO app
```

An error stops the command:

```
pista: error: cannot drop public.staff: view public.eng_staff depends on it
```
