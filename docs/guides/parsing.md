# Parsing schema files

`pista parse` reads schema SQL files and prints the objects that they declare as JSON. The files go through the same parser that `plan` and `apply` use. So a file that `plan` accepts is a file that `parse` accepts. The command reads no database.

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
      "columns": [
        {
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
      ],
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

The top level contains one object per managed type: `tables`, `views`, `enums`, `domains`, `composite_types`, `sequences` and `routines`. Each is keyed by the qualified name, in the order in which the files declare the objects. A name written without a schema is qualified with the first schema in `--schemas`, as `plan` reads it. A routine's key includes its identity argument types, `public.add(bigint, bigint)`, because two routines can share a name.

A table's `columns` is an array, not an object. The order is the column order, which a JSON object does not promise to keep. The name that an object would use as the key is in the column's `name` field. Everything else in a table, `constraints`, `foreign_keys`, `indexes`, `policies` and `triggers`, is an object keyed by name. Their order is the order in which the files declare them. Under `dump`, it is the catalog's name order. Nothing is lost by reading them in another order.

Statements from `-- pista:execute` follow the objects, in `execute_stmts`.

Expressions remain SQL text. A default, a check clause, a view body or an index definition is the string that the parser read, not a tree.


## Fields

Every field is written, whatever it contains: a name, an empty string, `false` or `null`.

Some fields can be filled only by a database, so `parse` leaves them at their zero value: `oid`, a column's `serial_sequence` and `type_storage`, and a constraint's `inherited`. A schema file does not say what PostgreSQL named the sequence behind a `bigserial`, what the default storage of a type is, or whether a partition has a constraint of its own.

What PostgreSQL stores as a character code is written as a word. A constraint's `type` is `check`, `foreign_key`, `not_null`, `primary_key`, `unique` or `exclusion`. A column's `identity` is `always` or `by_default` and its `generated` is `stored` or `virtual`. A policy's `command` is `ALL`, `SELECT`, `INSERT`, `UPDATE` or `DELETE`. A trigger's `state` is `enabled`, `disabled`, `replica` or `always`. A field that does not apply contains an empty string.

A generated column keeps its expression in `default`, next to `"generated": "stored"`. That is where the model stores it.

Some fields repeat part of another field in a form that needs no SQL parsing:

- A column's `base_type` is its `type` without the modifier and the array marker. A domain is replaced by the type it is based on, and a serial type by its integer type. For example, `timestamp(3) without time zone` and a domain over it both have the base type `timestamp without time zone`. `is_array` is `true` if the type, or the type a domain is based on, is an array. A domain that is not in the document is not replaced.
- An index's `columns` lists the key columns in order. An expression is `null`. `include` lists the `INCLUDE` columns. `unique` is `true` for a unique index. `method` is the access method, such as `btree`. `partial` is `true` if the index has a `WHERE` clause. Sort order, operator classes and collations are not included.
- A foreign key's `ref_columns` lists the referenced columns. `on_delete` and `on_update` are `no action`, `restrict`, `cascade`, `set null` or `set default`. A key that does not specify an action has `no action`. `match` is `simple` or `full`.

`parse` looks up an unqualified domain name in the first schema of `--schemas`, then in `public`. `dump --json` looks in each schema of `--schemas`, then in `public`.

Directives appear as fields. `-- pista:renamed-from` becomes `rename_from`, and `-- pista:ignore` becomes `"ignore": true`. On an enum value, the rename is written in the enum's `value_rename_from`, keyed by the new value and sorted by it. `values` remains in the order in which the file writes them.


## The JSON Schema

The document has a JSON Schema. It is at `docs/json/schema-1.3.json`, and the
documentation site publishes it at
[https://winebarrel.github.io/pistachio/json/schema-1.3.json](https://winebarrel.github.io/pistachio/json/schema-1.3.json).
The earlier `schema-1.0.json`, `schema-1.1.json` and `schema-1.2.json` are still published.

It is generated by reflection from the structs that the parser fills. So it describes what the command writes, not what a separate description claims. `make json-schema` regenerates it. A test fails when the committed file differs from what the generator produces.


## From a database

`pista dump --json` writes a document of this shape for a database.

Some values can differ. A database knows what a schema file does not state. The catalog gives every column a `storage_type`, while `parse` fills it only where the file writes `SET STORAGE`. The catalog also spells an expression its own way, so a view's `definition` looks different. A serial column's `default` contains the `nextval` call that PostgreSQL gave it, where `parse` leaves it `null`. A database also has no `-- pista:execute` statements, so `execute_stmts` is empty.


## What it is not

`parse` reports what the files say, not what a database contains. Nothing is compared or diffed. For the current state of a database, use `dump --json`.
