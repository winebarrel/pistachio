# Working with multiple schemas

By default every command targets the `public` schema. `-n` / `--schemas` selects another schema, or several:

```bash
pista dump -n myschema
pista plan -n public,myschema schema.sql
PISTA_SCHEMAS=myschema pista apply schema.sql
```

Objects outside the named schemas are out of scope on both sides. They are not dumped, and they are not dropped.

## Files without schema names

A name written without a schema belongs to the first schema in `-n`. Under `-n myschema`, `CREATE TABLE users` means `myschema.users`. So one set of files can be applied to a schema of any name:

```bash
pista plan -n staging schema.sql
pista apply -n staging schema.sql
```

`dump --omit-schema` writes such files:

```bash
pista dump -n staging --omit-schema > schema.sql
# CREATE TABLE users (...) instead of CREATE TABLE staging.users (...)
```

## Mapping schema names

`-m` / `--schema-map` reads one schema from the database under another name. Use it when the files qualify their objects with a schema name that the database does not use. With `-m staging=public`, the files say `public` and the database has `staging`:

```bash
pista dump -n staging -m staging=public
pista plan -n staging -m staging=public schema.sql
pista apply -n staging -m staging=public schema.sql
```

The map applies to every place where a schema name appears:

- a column's type and default
- a partition's parent
- a constraint
- a domain's base type

## Names the catalog leaves bare

The catalog reports an object without its schema when `search_path` can reach it. So under the default `--search-path=public`, `dump` writes `status` for `public.status`, and `plan` compares that form. `--search-path=` qualifies every name. See the [notes on `plan`](../reference/commands/plan.md#notes).
