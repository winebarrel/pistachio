# Controlling drops

An object that is no longer in the desired schema is not dropped unless `--allow-drop` includes its type. Instead, the plan writes the drop as a comment:

```sql
-- Connected to postgres://postgres@localhost:5432/postgres
-- Plan for schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
-- skipped: DROP TABLE public.legacy_users;
-- No changes
```

A skipped drop is not executable DDL. So a plan that contains nothing else is reported as `-- No changes`, and exits with 0 under `--check`.

## Allowing drops

```bash
pista plan --allow-drop all schema.sql
pista apply --allow-drop column,table schema.sql
PISTA_ALLOW_DROP=all pista plan schema.sql
```

The types are `all`, `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`, `column`, `constraint`, `foreign_key`, `index`, `policy` and `trigger`. To give several types, separate them with commas or repeat the flag.

- `constraint` covers CHECK, UNIQUE, PRIMARY KEY and EXCLUSION constraints. Foreign keys are `foreign_key`.
- `table` also covers the foreign keys of a dropped table.
- `composite_type` also covers `DROP ATTRIBUTE`.
- `routine` covers functions and procedures.

## What is gated

Only a pure removal requires `--allow-drop`. The drop that is part of a definition change does not. A constraint or an index whose definition changes is dropped and added back, whatever the flag says, because PostgreSQL has no `ALTER` for either. The same applies to a policy whose command or permissiveness changes, or that loses a `USING` or `WITH CHECK` clause. No stored row is lost in those cases.

A domain constraint has no type of its own, and is dropped either way.

A view, a routine or a trigger that must be recreated is the exception. Recreating one requires `view`, `routine` or `trigger`. Without it, the plan writes the `DROP` as `-- skipped:`, and the object keeps its current definition. See [Supported objects](../reference/objects.md) for when each is recreated.

## Before allowing a drop

`plan --allow-drop all` shows every drop that the desired schema implies, without running any of them. Read it before you run `apply` with the same flag.

A `DROP TABLE` and a `DROP COLUMN` are not checked for dependents. If a view reads the table and is not dropped, the apply fails at that statement, and `--with-tx` rolls back the rest. A dropped view or key is checked. For those, `plan` fails first and names what depends on it.

`-- pista:renamed-from` turns a drop and a create into a rename. See [Renaming objects](renaming.md).
