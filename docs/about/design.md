# Design and scope

## The contract

`pista dump` output, fed back as the desired schema, plans clean. A break in
that round trip is a bug, and the test suite is built around it: every sample
schema is dumped and re-planned, and every dump is reloaded into an empty
database and compared against the original with `pg_dump`.

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

## No DDL a change does not need

A statement is emitted only when something has to change, and never
implicitly. Low load on the database comes before a simple interface, which is
why several behaviors are directives the schema file carries rather than
inferences pistachio makes:

- `CONCURRENTLY` on an index is opt-in per index, not applied everywhere.
- Combining a table's `ALTER TABLE` actions into one statement is opt-in.
- A rename is a directive. Nothing guesses that a dropped table and an added
  one are the same table, because guessing wrong drops data.

The cost of the guess falls on the database, so the schema file states the
intent instead.

## Assumptions about the reader

The user knows PostgreSQL and their own schema. Error messages name what
PostgreSQL will reject rather than working around it, and options are not
guarded against combinations that only make sense together.

## Rare inputs

A rare input is not worth an implementation that is hard to follow. Where a
corner case is left open it is written down in [Known
limitations](limitations.md) with what the fix would look like, rather than
handled with a special case that the next reader has to carry.
