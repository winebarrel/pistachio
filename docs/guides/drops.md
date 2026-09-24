# Controlling drops

By default, `plan` and `apply` do **not** drop tables, views, enums, domains, composite types, sequences, routines, columns, constraints, foreign keys, indexes, policies, or triggers. Use `--allow-drop` to opt in:

```bash
# Allow all drops
pista plan --allow-drop all schema.sql
pista apply --allow-drop all schema.sql

# Allow only specific drop types (comma-separated or repeated)
pista apply --allow-drop column,table schema.sql

# Using environment variable
PISTA_ALLOW_DROP=all pista plan schema.sql
```

Valid types: `all`, `table`, `view`, `enum`, `domain`, `composite_type`, `sequence`, `routine`, `column`, `constraint`, `foreign_key`, `index`, `policy`, `trigger`. `constraint` covers CHECK / UNIQUE / PRIMARY KEY / EXCLUSION; foreign keys are governed by `foreign_key` separately. `composite_type` also gates `DROP ATTRIBUTE` on a composite type. `routine` covers functions and procedures, and also gates the drop half of a recreate.

!!! note
    `--allow-drop=constraint`, `--allow-drop=foreign_key`, `--allow-drop=index`, and `--allow-drop=policy` only govern **pure removals** (objects absent from the desired schema). **Definition changes** still execute regardless of `--allow-drop`: constraints and foreign keys as DROP + ADD, and indexes as DROP + CREATE, because PostgreSQL has no `ALTER CONSTRAINT` and no general `ALTER INDEX` form for definition changes. A policy is recreated as DROP + CREATE when its command or permissiveness changes or a `USING` / `WITH CHECK` clause is removed, since `ALTER POLICY` cannot make those changes.

