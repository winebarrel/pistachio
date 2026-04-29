# TODO

Items intentionally deferred from prior PRs. Each entry notes the originating
PR for context.

## Auto-rewrite of column references in views and cross-table FKs

When a column is renamed via `-- pist:renamed-from`, the rewriter only
updates same-table dependents (indexes, constraints, FKs on the same
table). The following references are **not** rewritten and may produce a
redundant `DROP/CREATE` on the first plan (the second run after applying
the rename comes out clean):

- View / materialized view definitions that `SELECT` the renamed column.
- Foreign keys in *other* tables whose `REFERENCES this_table(renamed_col)`
  points at the renamed column (PkAttrs side).

Resolving these requires cross-object awareness in the diff phase.

Origin: #123.

## Validation of column refs in GENERATED / DEFAULT expressions

`ValidateColumnRefs` checks index / constraint / FK definitions against
the desired column set. It does not currently walk:

- `GENERATED ALWAYS AS (<expr>) STORED` expressions on columns
- `DEFAULT <expr>` expressions on columns

A typo or stale rename in these expressions still surfaces only at apply
time. Adding a walk over `model.Column.Default` for both kinds (gated by
`Generated`) would close this gap.

Origin: #124.

## INHERITS table plan / apply support

`model.Table.SQL` has an INHERITS branch that drops the child's own
columns and only emits constraints. As a result, plan / apply for an
`INHERITS (...)` child whose desired definition adds columns produces
incorrect DDL. The validator already special-cases INHERITS children
(skipped because the inherited column set isn't materialised on the
child), but the SQL emitter and diff don't handle the legacy partition
shape end-to-end.

Origin: #125. Plan / apply fixtures were intentionally not added.

## GENERATED column expression changes

`alterColumnSQL` skips the default-diff entirely when either side is a
stored generated column, which fixes the false `SET DEFAULT` emission
but means a *real* expression change is silently ignored. PostgreSQL
requires `DROP COLUMN` + `ADD COLUMN` to change a generated expression.
The diff layer should detect generated expression changes and emit the
two-step DDL (subject to a drop policy similar to other column drops).

Origin: #125.

## Plan-time error promotion: forgotten dependent reference

When desired SQL references the new column name in a dependent
definition but forgets to add `-- pist:renamed-from` on the column
itself, current behavior is to produce DDL that fails at apply time.
The `ValidateColumnRefs` pass added in #124 already catches the
inverse case (renamed column with stale dependent reference). The
forgotten-rename direction could in principle also be caught (e.g. by
detecting "column X is in current but not desired AND a column with a
similar name exists in desired") but the heuristic has false positives
and is not pursued.

Origin: discussion during #124. No current plan to implement.
