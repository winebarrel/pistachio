# Design and scope

## The contract

`pista dump` output, fed back as the desired schema, plans clean. A break in
that round trip is a bug. CI dumps and re-plans dozens of real-world schemas, and
reloads a smaller set covering one object kind each into an empty database to
compare it against the original with `pg_dump`. The second check is what
catches something the dump drops when the plan overlooks it too.

Drift that appears only with a desired schema written some other way is lower
priority. Writing the schema the way `dump` does avoids it. [Known
limitations](limitations.md) marks those entries `Priority: low` and says what
the workaround is.

## What is not managed

`CREATE EXTENSION`, `CREATE ROLE` and `GRANT` are out of scope. They sit at a
different privilege layer than a schema: the role that runs a migration is
usually not the role that owns the cluster, and a grant often belongs to the
same place the database itself is provisioned. Manage them where the rest of
the infrastructure is managed, Terraform for example.

pistachio parses only the statements it manages. It drops anything else in a
schema file and prints an `ignored unsupported statement:` warning for each
one, so nothing is lost silently. To keep such a statement in the file and run
it during `apply`, mark it with
[`-- pista:execute`](../reference/directives.md).

## Only the DDL a change needs

A statement is emitted when something has to change, and never implicitly.
Low load on the database comes before a simple interface, so the schema file
carries a directive where an inference would be cheaper to use and more
expensive to get wrong:

- `CONCURRENTLY` on an index is opt-in per index, not applied everywhere.
- Combining a table's `ALTER TABLE` actions into one statement is opt-in.
- A rename is a directive. Nothing guesses that a dropped table and an added
  one are the same table, because guessing wrong drops data.

## Rare inputs

A rare input is not worth an implementation that is hard to follow. Where a
corner case is left open it is written down in [Known
limitations](limitations.md), with what the fix would look like.
