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

Toggling a column between generated and non-generated now errors at
plan time (`cannot toggle GENERATED — DROP COLUMN + ADD COLUMN is
required`). However, a change to the *expression* of a column that is
generated on both sides is still silently skipped: catalog renders
the expression with pg_get_expr-added type casts (e.g. `price *
(quantity)::numeric`) which do not reliably compare with the
desired-side raw expression (`price * quantity`). A robust comparison
would require recursively stripping casts during normalization.

Once expression comparison works, the diff could either error out (as
the toggle case does) or emit `DROP COLUMN` + `ADD COLUMN` gated by a
drop policy.

Origin: #125.

## Silent drift on `Table.TableSpace` / `Index.TableSpace` changes

The catalog and parser populate `Table.TableSpace` and `Index.TableSpace`,
but the diff layer never compares them. Changing a tablespace in desired
SQL after the object exists has no effect on the generated plan. Should
emit `ALTER TABLE ... SET TABLESPACE <new>` and
`ALTER INDEX ... SET TABLESPACE <new>`.

Origin: post-#125 audit.

## Silent drift on `Table.Unlogged` toggle

`model.Table.Unlogged` is set on parse and used to emit `CREATE UNLOGGED
TABLE`, but transitions on existing tables (logged ⇄ unlogged) are not
diffed. PostgreSQL has `ALTER TABLE ... SET LOGGED` / `SET UNLOGGED`.
The current `no_diff_unlogged_table.yml` fixture only covers the
unchanged case.

Origin: post-#125 audit.

## `Column.StorageType` is dead code

`model.Column.StorageType` is declared but never read or written by
parser / catalog / diff. Either implement column storage diffs
(`ALTER COLUMN ... SET STORAGE PLAIN|EXTERNAL|EXTENDED|MAIN`) or drop
the field. Today the value is always the zero string.

Origin: post-#125 audit.

## Constraint `Deferrable` / `Deferred` granularity

`Constraint.Deferrable` / `Constraint.Deferred` are read from the
catalog but not compared directly in the diff. Changes are still
detected because they end up in the `pg_get_constraintdef` Definition
string, which means a deferrable toggle currently triggers DROP+ADD
of the constraint. PostgreSQL supports `ALTER CONSTRAINT ... [NOT]
DEFERRABLE [INITIALLY ...]` for in-place changes; using it would avoid
the round-trip.

Origin: post-#125 audit. Optimisation rather than a bug — current
behaviour is correct, just heavier than necessary.

## Standalone Sequences are not first-class

`catalog/sequences.go` reads sequences (used for SERIAL/IDENTITY
metadata), but parser and diff don't handle `CREATE SEQUENCE` /
`ALTER SEQUENCE` / `DROP SEQUENCE` as schema operations. Standalone
sequences in desired SQL therefore round-trip via execute directives
only. Decide whether sequence is in scope; if yes, add parser/diff
support; if no, document the limitation.

Origin: post-#125 audit.

## Table rename: cross-table dependents

`detectTableRenames` rewrites the renamed table's own indexes and FKs
in the adjusted current state, but other tables' `FOREIGN KEY ...
REFERENCES old_table(...)` and view definitions that reference the old
table name are not rewritten. PostgreSQL auto-updates these on RENAME,
so a second plan/apply comes out clean, but the first plan can emit
redundant DROP/CREATE for the dependent objects. The same scope as
the existing column-rename TODO above ("Auto-rewrite of column
references in views and cross-table FKs"), extended to table-level
renames.

Origin: pre-existing NOTE in `diff/rename.go:detectTableRenames`.

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
