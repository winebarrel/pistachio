# Splitting the schema across files


Use `--split` to output each table/view/enum/domain/composite type as a separate file in the specified directory.

```bash
pista dump --split ./schema/
# -- Dump of schema public (3 tables, 0 views, 1 enum, 0 domains, 0 composite types, 0 sequences)
# -- Wrote 4 file(s) to ./schema/
# (writes ./schema/public.status.sql, ./schema/public.users.sql, ./schema/public.orders.sql, ...)
```


