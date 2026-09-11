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
    id bigint not null
);
```

```json
{
  "tables": {
    "public.items": {
      "oid": 0,
      "schema": "public",
      "name": "items",
      "rename_from": null,
      "bulk_alter": false,
      "ignore": false,
      "table_space": null,
      "storage_params": {},
      "unlogged": false,
      "partitioned": false,
      "partition_def": null,
      "partition_of": null,
      "partition_bound": null,
      "row_security": false,
      "force_row_security": false,
      "columns": {
        "id": {
          "name": "id",
          "rename_from": null,
          "type": "bigint",
          "serial_sequence": null,
          "not_null": true,
          "not_null_name": null,
          "default": null,
          "identity": "",
          "identity_sequence": null,
          "generated": "",
          "collation": null,
          "storage_type": "",
          "type_storage": "",
          "compression": "",
          "comment": null
        }
      },
      "constraints": {},
      "foreign_keys": {},
      "indexes": {},
      "policies": {},
      "triggers": {},
      "comment": null
    }
  },
  "views": {},
  "enums": {},
  "domains": {},
  "composite_types": {},
  "sequences": {},
  "routines": {},
  "execute_stmts": []
}
```


## The shape of the output

The top level holds one object per managed type: `tables`, `views`, `enums`, `domains`, `composite_types`, `sequences` and `routines`. Each is keyed by the qualified name, in the order the files declare it. A name written without a schema is qualified with the first schema in `--schemas`, the way `plan` reads it. A routine's key carries its identity argument types, `public.add(bigint, bigint)`, since two routines can share a name.

Statements from `-- pista:execute` follow the objects, in `execute_stmts`.

Expressions stay SQL text. A default, a check clause, a view body or an index definition is the string the parser read, not a tree.


## Fields

Every field is written, whatever it holds. A key is there whether the value is a name, an empty string, `false` or `null`, so a consumer reads the same keys off every object of a kind rather than testing for their presence, and an absent value is never confused with an unsupported one.

Some fields only a database can fill, so `parse` leaves them at their zero value: `oid`, a column's `serial_sequence` and `type_storage`, a constraint's `inherited`. A schema file does not say what PostgreSQL named the sequence behind a `bigserial`, what a type's default storage is, or whether a partition holds a constraint of its own.

What PostgreSQL stores as a character code is written as a word. A constraint's `type` is `check`, `foreign_key`, `not_null`, `primary_key`, `unique` or `exclusion`; a column's `identity` is `always` or `by_default` and its `generated` is `stored` or `virtual`; a policy's `command` is `ALL`, `SELECT`, `INSERT`, `UPDATE` or `DELETE`; a trigger's `state` is `enabled`, `disabled`, `replica` or `always`. A field that does not apply holds an empty string.

A generated column keeps its expression in `default`, next to `"generated": "stored"`, which is where the model holds it.

Directives show up as fields. `-- pista:renamed-from` becomes `rename_from`, `-- pista:ignore` becomes `"ignore": true`.


## What it is not

`parse` reports what the files say, not what a database holds. Filters such as `--include` do not apply, and nothing is compared or diffed. For the current state of a database, use `dump`.
