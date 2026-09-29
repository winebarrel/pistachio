# Working with multiple schemas

By default every command targets the `public` schema. `-n` / `--schemas` names another, or several:

```bash
pista dump -n myschema
pista plan -n public,myschema schema.sql
PISTA_SCHEMAS=myschema pista apply schema.sql
```

Objects outside the named schemas are out of scope on both sides: they are not dumped, and not dropped.

## Files without schema names

A name written without a schema belongs to the first schema in `-n`. `CREATE TABLE users` under `-n myschema` is `myschema.users`, so one set of files can be applied to a schema of any name:

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

`-m` / `--schema-map` reads one schema from the database under another name, for files that qualify their objects with a schema the database does not use. With `-m staging=public`, the files say `public` and the database holds `staging`:

```bash
pista dump -n staging -m staging=public
pista plan -n staging -m staging=public schema.sql
pista apply -n staging -m staging=public schema.sql
```

The map reaches every place a schema name appears: a column's type and default, a partition's parent, a constraint, a domain's base type.

## Names the catalog leaves bare

The catalog reports an object reachable through `search_path` without its schema, so under the default `--search-path=public` a `dump` writes `status` for `public.status` and `plan` compares that form. `--search-path=` qualifies everything. See the [notes on `plan`](../reference/commands/plan.md#notes).
