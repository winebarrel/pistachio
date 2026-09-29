# Controlling drops

An object the desired schema no longer holds is not dropped unless `--allow-drop` names its type. The plan writes the drop as a comment instead:

```sql
-- Plan for schema public (1 table, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
-- skipped: DROP TABLE public.legacy_users;
-- No changes
```

A skipped drop is not executable DDL, so a plan that holds nothing else reads `-- No changes` and exits with 0 under `--check`.

## Allowing drops

```bash
pista plan --allow-drop all schema.sql
pista apply --allow-drop column,table schema.sql
PISTA_ALLOW_DROP=all pista plan schema.sql
```

The types are `all`, `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`, `column`, `constraint`, `foreign_key`, `index`, `policy` and `trigger`. Several are given comma-separated or by repeating the flag.

- `constraint` covers CHECK, UNIQUE, PRIMARY KEY and EXCLUSION constraints. Foreign keys are `foreign_key`.
- `table` also covers the foreign keys a dropped table holds.
- `composite_type` also covers `DROP ATTRIBUTE`.
- `routine` covers functions and procedures.

## What is gated

Only a pure removal is gated. The drop half of a definition change is not: a constraint or an index whose definition changes is dropped and added back whatever the flag says, because PostgreSQL has no `ALTER` for either, and so is a policy whose command or permissiveness changes or that loses a `USING` or `WITH CHECK` clause. Data is not at stake in those.

A view, a routine or a trigger that has to be recreated is the exception. Their recreate needs `view`, `routine` or `trigger`, and without it the plan writes the `DROP` as `-- skipped:` and the object keeps its current definition. See [Supported objects](../reference/objects.md) for when each is recreated.

## Before allowing a drop

`plan --allow-drop all` shows every drop the desired schema implies without running one. Read it before `apply` with the same flag.

A `DROP TABLE` and a `DROP COLUMN` are not checked for dependents: a view that reads the table and stays makes the apply fail at that statement, and `--with-tx` rolls the rest back. A dropped view or key is checked, and `plan` fails first naming what depends on it.

`-- pista:renamed-from` turns a drop and a create into a rename. See [Renaming objects](renaming.md).
