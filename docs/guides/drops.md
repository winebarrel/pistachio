# Controlling drops

By default, `plan` and `apply` do **not** drop tables, views, enums, domains, columns, constraints, foreign keys, or indexes. Use `--allow-drop` to opt in:

```bash
# Allow all drops
pista plan --allow-drop all schema.sql
pista apply --allow-drop all schema.sql

# Allow only specific drop types (comma-separated or repeated)
pista apply --allow-drop column,table schema.sql

# Using environment variable
PISTA_ALLOW_DROP=all pista plan schema.sql
```

Valid types: `all`, `table`, `view`, `enum`, `domain`, `column`, `constraint`, `foreign_key`, `index`. `constraint` covers CHECK / UNIQUE / PRIMARY KEY / EXCLUSION; foreign keys are governed by `foreign_key` separately.

> [!NOTE]
> `--allow-drop=constraint`, `--allow-drop=foreign_key`, and `--allow-drop=index` only govern **pure removals** (objects absent from the desired schema). **Definition changes** still execute regardless of `--allow-drop`: constraints and foreign keys as DROP + ADD, and indexes as DROP + CREATE, because PostgreSQL has no `ALTER CONSTRAINT` and no general `ALTER INDEX` form for definition changes.

