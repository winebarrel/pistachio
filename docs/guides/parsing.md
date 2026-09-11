# Parsing schema files

`pista parse` reads schema SQL files and prints the objects they declare as JSON. The files go through the same parser `plan` and `apply` use, so a file that plans is a file that parses. The command reads no database.

```bash
pista parse schema/*.sql
```

The output is meant for other tools: a code generator, a linter, `jq`.

```bash
pista parse schema/*.sql | jq -r '.tables | keys[]'
```


## An example

```sql
create table public.items (
    id bigserial,
    name text not null,
    constraint items_pkey primary key (id)
);
```

```json
{
  "tables": {
    "public.items": {
      "schema": "public",
      "name": "items",
      "storage_params": {},
      "columns": {
        "id": {
          "name": "id",
          "type": "bigserial",
          "not_null": true
        },
        "name": {
          "name": "name",
          "type": "text",
          "not_null": true
        }
      },
      "constraints": {
        "items_pkey": {
          "name": "items_pkey",
          "type": "primary_key",
          "definition": "PRIMARY KEY (id)",
          "columns": ["id"],
          "validated": true
        }
      },
      "foreign_keys": {},
      "indexes": {},
      "policies": {},
      "triggers": {}
    }
  },
  "views": {},
  "enums": {},
  "domains": {},
  "composite_types": {},
  "sequences": {},
  "routines": {}
}
```


## The shape of the output

The top level holds one object per managed type: `tables`, `views`, `enums`, `domains`, `composite_types`, `sequences` and `routines`. Each is keyed by the qualified name, in the order the files declare it. A name written without a schema is qualified with the first schema in `--schemas`, the way `plan` reads it. A routine's key carries its identity argument types, `public.add(bigint, bigint)`, since two routines can share a name.

An object type the files do not declare is an empty object, so a consumer can index it without checking for its presence. Statements from `-- pista:execute` come as an `execute_stmts` array after the objects.

Expressions stay SQL text. A default, a check clause, a view body or an index definition is the string the parser read, not a tree.


## Fields

A field that is unset or holds its default is left out: a nullable column has no `not_null`, a column without a default has no `default`. Two fields are written either way, because their false carries meaning: `validated`, whose false means `NOT VALID`, and `permissive`, whose false means `AS RESTRICTIVE`.

What PostgreSQL stores as a character code is written as a word. A constraint's `type` is `check`, `foreign_key`, `not_null`, `primary_key`, `unique` or `exclusion`; a column's `identity` is `always` or `by_default` and its `generated` is `stored` or `virtual`; a policy's `command` is `ALL`, `SELECT`, `INSERT`, `UPDATE` or `DELETE`; a trigger's `state` is `enabled`, `disabled`, `replica` or `always`.

Directives show up as fields. `-- pista:renamed-from` becomes `rename_from`, `-- pista:ignore` becomes `"ignore": true`.


## What it is not

`parse` reports what the files say, not what a database holds. Filters such as `--include` do not apply, and nothing is compared or diffed. For the current state of a database, use `dump`.
