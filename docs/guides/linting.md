# Linting schema files

`pista lint` checks the objects in schema files against rules that you write. pistachio has no built-in rules. This page describes the rule file, the values a rule reads, and how to turn a rule off for one object. [`pista lint`](../reference/commands/lint.md) lists the options.


## A rule file

A rule file is YAML with a list of rules:

```yaml
rules:
  - name: require-primary-key
    on: table
    assert: table.constraints.values().exists(c, c.type == "primary_key")
    message: the table has no primary key
```

Each rule has four fields, and all of them are required:

`name`
:   The name that the output and `-- pista:lint-ignore` use. It must be unique across the rule files.

`on`
:   The kind of object the rule checks: `table`, `column`, `index` or `foreign_key`.

`assert`
:   A [CEL](https://cel.dev/) expression that must be true for every object of that kind. It must return a boolean.

`message`
:   The text printed after the rule name when the expression is false.

An unknown field is an error. A rule that does not compile is an error, and so is a rule that fails to evaluate, for example by reading a field that does not exist.


## What a rule reads

A rule reads the objects as [`pista parse`](../reference/commands/parse.md) writes them in JSON. Run `pista parse` on your files to see the fields. [Parsing schema files](parsing.md) describes them. The fields that repeat part of another field, such as a column's `base_type` and an index's `columns`, are there so that a rule does not have to parse SQL text.

These variables are set:

| `on` | Variables |
|---|---|
| `table` | `table` |
| `column` | `table`, `column` |
| `index` | `table`, `index` |
| `foreign_key` | `table`, `fk` |

Every rule can also read `doc`, the whole document. A rule that uses a variable its kind does not set fails to compile.

An index on a materialized view is checked as well. Its `table` is the view.

Most of the document is keyed by name, such as a table's `constraints` and `indexes`. CEL macros such as `exists` iterate the keys of a map, so use `values()` to iterate the objects. A table's `columns` is a list.


## Functions

Besides the standard CEL functions, a rule can use these:

`m.values()`
:   The values of the map `m`, as a list.

`hasPrefix(list, prefix)`
:   True when `list` starts with the elements of `prefix`.

`debug(label, value)`
:   Writes `value` as JSON to standard error, with the object, the rule and `label`, and returns `value`. Wrap part of an expression in it to see what the expression reads. CEL stops evaluating `&&`, `||` and macros early, so a `debug` call that is not reached writes nothing.

```
debug: index public.posts_created_idx: duplicate-index: cols = ["created_at"]
```


## Examples

A column type:

```yaml
- name: prefer-timestamptz
  on: column
  assert: column.base_type != "timestamp without time zone"
  message: use timestamp with time zone
```

`base_type` drops the modifier and follows a domain, so this also finds `timestamp(3)` and a domain over `timestamp`.

A foreign key that no index starts with:

```yaml
- name: fk-needs-index
  on: foreign_key
  assert: >-
    table.indexes.values().exists(i, !i.partial && hasPrefix(i.columns, fk.columns))
    || table.constraints.values().exists(c,
         c.type in ["primary_key", "unique"] && hasPrefix(c.columns, fk.columns))
  message: no index starts with the columns of the foreign key
```

A rule that reads another table through `doc`:

```yaml
- name: fk-same-type
  on: foreign_key
  assert: >-
    table.columns.filter(c, c.name == fk.columns[0])[0].base_type ==
    doc.tables[fk.ref_schema + "." + fk.ref_table].columns.filter(c, c.name == fk.ref_columns[0])[0].base_type
  message: the column and the column it references have different types
```

The keys of `doc.tables` are qualified names as the document writes them, so a name that needs quoting has its quotes.


## Standard rules

The repository has a set of rules in [`rules/`](https://github.com/winebarrel/pistachio/tree/main/rules): a primary key on every table, an index for every foreign key, no duplicate index, column types, and snake_case names. Copy the files you want into your repository and change them there. pistachio does not read rules from a URL, so a rule changes only when you change your copy.


## Turning a rule off for one object

[`-- pista:lint-ignore`](../reference/directives.md#-pistalint-ignore) names the rules that do not apply to the next object. Separate several names with commas or spaces.

```sql
-- pista:lint-ignore require-primary-key
CREATE TABLE public.event_log (
    -- pista:lint-ignore prefer-timestamptz
    at timestamp NOT NULL,
    body text NOT NULL
);
```

Before `CREATE TABLE` it applies to the table, and before a column or a `CONSTRAINT` line inside it, to that column or foreign key. Before `CREATE INDEX` it applies to the index, and before `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`, to the foreign key. It applies to that object only: the directive on a table does not turn the rule off for the table's columns.

A table marked `-- pista:ignore` is not checked at all.
