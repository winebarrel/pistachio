# Design and scope

## The contract

`pista dump` writes the schema of a database as SQL. When that output is fed
back to `plan` as the desired schema, `plan` finds no changes. If it finds any,
that is a bug.

A schema written in a form other than `dump` output may drift. Fixing such
drift has lower priority. [Known limitations](limitations.md) lists these cases
as `Priority: low`.

## What is not managed

`CREATE EXTENSION`, `CREATE ROLE` and `GRANT` are out of scope. They belong to
a different privilege layer than the schema. Manage them with the rest of the
infrastructure, for example in Terraform.

pistachio reads only the statements that it manages. It skips every other
statement and prints an `ignored unsupported statement:` warning for each one.
For a statement that must still run, consider
[`-- pista:execute`](../reference/directives.md).

## Explicit directives

pistachio emits the DDL that a change needs, and nothing more. Extra statements
cost the database time and locks. It also does not guess what the user wants.
The user says it with a directive.

For example, pistachio does not detect a rename. A renamed table looks like a
drop and an add, and a wrong guess would drop data. Mark the rename with
`-- pista:renamed-from`.

## Rare inputs

pistachio does not cover every rare input. Code for a rare case makes the rest
harder to follow. [Known limitations](limitations.md) lists the open cases and
what a fix would take.
