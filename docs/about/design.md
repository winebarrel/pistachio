# Design and scope

## The contract

When `pista dump` output is used as the desired schema, `plan` produces no
changes. A break in that round trip is a bug. CI dumps and re-plans dozens of
real-world schemas. CI also reloads a smaller set of schemas, each covering one
object kind, into an empty database and compares the result with the original
using `pg_dump`. The second check catches what the dump drops when the plan
overlooks it too.

Drift that appears only with a desired schema written in another form has lower
priority. Writing the schema in the form that `dump` writes avoids it. [Known
limitations](limitations.md) marks those entries `Priority: low` and gives the
workaround.

## What is not managed

`CREATE EXTENSION`, `CREATE ROLE` and `GRANT` are out of scope. They belong to
a different privilege layer than a schema. The role that runs a migration is
usually not the role that owns the cluster. A grant often belongs in the same
place where the database itself is provisioned. Manage them where the rest of
the infrastructure is managed, for example in Terraform.

pistachio parses only the statements that it manages. It drops every other
statement in a schema file and prints an `ignored unsupported statement:`
warning for each one. Nothing is lost silently. To keep such a statement in the
file and run it during `apply`, mark it with
[`-- pista:execute`](../reference/directives.md).

## Only the DDL a change needs

pistachio emits a statement only when something has to change, and never
implicitly. Low load on the database comes before a simple interface. So the
schema file uses a directive where an inference would be easier to use but more
costly to get wrong:

- `CONCURRENTLY` on an index is opt-in per index, not applied everywhere.
- Combining a table's `ALTER TABLE` actions into one statement is opt-in.
- A rename is a directive. pistachio does not guess that a dropped table and an
  added table are the same table. A wrong guess drops data.

## Rare inputs

A rare input is not worth an implementation that is hard to follow. When a
corner case is left open, it is recorded in [Known
limitations](limitations.md), together with what the fix would look like.
