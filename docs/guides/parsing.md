# Parsing schema files

`pista parse` reads schema SQL files and prints the objects they declare as JSON. The files go through the same parser `plan` and `apply` use, so a file that plans is a file that parses. The command reads no database.

```bash
pista parse schema/*.sql
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

Every field is written, whatever it holds: a name, an empty string, `false` or `null`.

Some fields only a database can fill, so `parse` leaves them at their zero value: `oid`, a column's `serial_sequence` and `type_storage`, a constraint's `inherited`. A schema file does not say what PostgreSQL named the sequence behind a `bigserial`, what a type's default storage is, or whether a partition holds a constraint of its own.

What PostgreSQL stores as a character code is written as a word. A constraint's `type` is `check`, `foreign_key`, `not_null`, `primary_key`, `unique` or `exclusion`; a column's `identity` is `always` or `by_default` and its `generated` is `stored` or `virtual`; a policy's `command` is `ALL`, `SELECT`, `INSERT`, `UPDATE` or `DELETE`; a trigger's `state` is `enabled`, `disabled`, `replica` or `always`. A field that does not apply holds an empty string.

A generated column keeps its expression in `default`, next to `"generated": "stored"`, which is where the model holds it.

Directives show up as fields. `-- pista:renamed-from` becomes `rename_from`, `-- pista:ignore` becomes `"ignore": true`. On an enum value the rename lands in the enum's `value_rename_from`, keyed by the new value and sorted by it, while `values` stays in the order the file writes.


## The JSON Schema

The document has a JSON Schema. It sits at `docs/json/schema-1.0.json`, and the
documentation site carries it at
[https://winebarrel.github.io/pistachio/json/schema-1.0.json](https://winebarrel.github.io/pistachio/json/schema-1.0.json).

It is reflected off the structs the parser fills, so it says what the command writes rather than what a description beside it claims. `make json-schema` regenerates it, and a test fails when the committed file is not what the generator produces.


## From a database

`pista dump --json` writes a document of this shape for a database.

Some values can differ. A database knows what a schema file does not state: the catalog gives every column a `storage_type` while `parse` fills it only where the file writes `SET STORAGE`, and it spells an expression its own way, so a view's `definition` reads differently. A database also holds no `-- pista:execute` statements, so `execute_stmts` is empty.


## What it is not

`parse` reports what the files say, not what a database holds. Nothing is compared or diffed. For the current state of a database, use `dump --json`.
