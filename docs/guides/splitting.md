# Splitting the schema across files


Use `--split` to output each table/view/enum/domain/composite type as a separate file in the specified directory.

```bash
pista dump --split ./schema/
# -- Dump of schema public (3 tables, 0 views, 1 enum, 0 domains, 0 composite types, 0 sequences)
# -- Wrote 4 file(s) to ./schema/
# (writes ./schema/public.status.sql, ./schema/public.users.sql, ./schema/public.orders.sql, ...)
```


## Sort by dependency

By default the dump orders objects by name within each type. Use `--sort-by-deps` to order them by dependency instead, so each object comes after the objects it depends on. For example, a table is placed after the types it uses and after the tables its foreign keys reference.

```bash
pista dump --sort-by-deps
```

The output can then be loaded from top to bottom without forward references. If the dependency graph has a cycle, such as two tables with mutual foreign keys, the dump cannot be ordered and errors. The flag cannot be combined with `--split`, which writes each object to a separate file with no inter-file order.

