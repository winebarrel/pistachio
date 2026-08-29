# Working with multiple schemas

By default, pistachio targets the `public` schema. Use `-n` or `$PISTA_SCHEMAS` to specify a different schema:

```bash
# Dump the "myschema" schema
pista -n myschema dump

# Or use environment variable
export PISTA_SCHEMAS=myschema
pista dump

# Plan/apply against "myschema"
pista -n myschema plan schema.sql
pista -n myschema apply schema.sql
```

You can also manage multiple schemas at once:

```bash
pista -n public,myschema dump
```


## Schema name mapping

Use `-m` / `--schema-map` when SQL files use a different schema name than the database. This is common when SQL is written against `public` but deployed to a staging-specific schema:

```bash
# Dump "staging" schema but output as "public"
pista -n staging -m staging=public dump

# Plan/apply: SQL files use "public", but changes target "staging"
pista -n staging -m staging=public plan schema.sql
pista -n staging -m staging=public apply schema.sql
```

## Schema-less SQL files

If your SQL files omit schema names (e.g. `CREATE TABLE users` instead of `CREATE TABLE public.users`), pistachio uses the first schema from `-n` as the default:

```bash
# Schema-less SQL is treated as "myschema"
pista -n myschema plan schema.sql
pista -n myschema apply schema.sql
```

Use `--omit-schema` with dump to produce schema-less output:

```bash
pista dump --omit-schema > schema.sql
# => CREATE TABLE users (...) instead of CREATE TABLE public.users (...)
```


## Omit schema

Use `--omit-schema` to omit schema names from the dump output.

```bash
pista dump --omit-schema
# => CREATE TABLE users (...) instead of CREATE TABLE public.users (...)

pista dump --omit-schema --split ./schema/
# -- Dump of schema public (2 tables, 0 views, 0 enums, 0 domains, 0 composite types, 0 sequences)
# -- Wrote 2 file(s) to ./schema/
# (writes ./schema/users.sql, ./schema/orders.sql, ...)
```

When schema is omitted in SQL files, `plan` and `apply` use the schema specified by `-n`:

```bash
pista -n staging plan schema.sql   # schema-less SQL is treated as "staging"
pista -n staging apply schema.sql
```

