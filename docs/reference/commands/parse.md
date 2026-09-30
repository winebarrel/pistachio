# pista parse

Print, as JSON, the objects that schema SQL files declare.

## Synopsis

```
pista parse [option...] file...
```

## Description

`pista parse` reads the files with the parser that [`pista plan`](plan.md) and [`pista apply`](apply.md) use. It prints the objects that the files declare. The output is JSON. Every file that `plan` accepts, `parse` accepts too. No database is read, and nothing is compared.

The document contains one JSON object per managed type, keyed by qualified name. After them come the `-- pista:execute` statements. Its JSON Schema is published at [json/schema-1.2.json](../../json/schema-1.2.json). See [Parsing schema files](../../guides/parsing.md) for the shape and the fields.

[`pista dump --json`](dump.md) writes a document of the same shape for a database.

## Options

The [general options](index.md#general-options) apply as well.

`-n` *schema*, `--schemas=`*schema*
:   The schema that qualifies an unqualified name. Only the first value is used. The default is `public`. The environment variable is `PISTA_SCHEMAS`.

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
