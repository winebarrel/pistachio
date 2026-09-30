# pista parse

Print the objects that schema SQL files declare as JSON.

## Synopsis

```
pista parse [option...] file...
```

## Description

`pista parse` reads the files through the parser that [`pista plan`](plan.md) and [`pista apply`](apply.md) use and prints the objects that they declare as JSON. A file that plans is a file that parses. No database is read, and nothing is compared.

The document has one object per managed type, keyed by qualified name, and the `-- pista:execute` statements after them. Its JSON Schema is published at [json/schema-1.2.json](../../json/schema-1.2.json). See [Parsing schema files](../../guides/parsing.md) for the shape and the fields.

[`pista dump --json`](dump.md) writes a document of the same shape for a database.

## Options

The [general options](index.md#general-options) apply as well.

`-n` *schema*, `--schemas=`*schema*
:   The schema to qualify an unqualified name with. Only the first is used. The default is `public`. The environment variable is `PISTA_SCHEMAS`.

## Exit status

The exit status is 0 when the document was written, 1 when a file could not be parsed, and 80 on a usage error.

## Environment

`PISTA_SCHEMAS` is described above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment).

## Examples

```bash
pista parse schema/*.sql
pista parse -n myschema schema.sql | jq '.tables | keys'
```

## See also

[`pista dump`](dump.md), [Parsing schema files](../../guides/parsing.md)
