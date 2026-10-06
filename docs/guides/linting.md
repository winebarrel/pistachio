# Linting schema files

`pista lint` checks the objects in schema files against rules that you write. pistachio has no built-in rules, but the repository has a set of standard rules that you can copy. This page shows how to run the check, how to use the standard rules, how to write a rule, and how to turn a rule off for one object. [`pista lint`](../reference/commands/lint.md) lists the options.


## Running the check

```bash
pista lint -r lint/ schema/*.sql
```

`-r` names a rule file or a directory of rule files. The command prints one line for each object that breaks a rule:

```
schema/posts.sql:7:5: column public.posts.created_at: prefer-timestamptz: use timestamp with time zone
schema/posts.sql:9:5: foreign key posts_user_fkey on public.posts: fk-needs-index: no index starts with the columns of the foreign key
```

Each line has the file, line and column of the object, the object, the rule name and the rule's message. The exit status is 2 when a line is printed and 0 when none is, so the command can fail a CI job.


## Using the standard rules

The repository has these rules in [`rules/`](https://github.com/winebarrel/pistachio/tree/main/rules):

| File | Rule | Checks |
|---|---|---|
| `keys.yml` | `require-primary-key` | Every table has a primary key. |
| | `prefer-bigint-key` | A primary key column is not `smallint` or `integer`. |
| | `fk-needs-index` | An index, a primary key or a unique constraint starts with the columns of each foreign key. |
| `indexes.yml` | `duplicate-index` | No two indexes have the same columns, `INCLUDE` columns, access method and uniqueness. |
| | `redundant-index` | No B-tree index has columns that another B-tree index starts with. |
| `types.yml` | `prefer-timestamptz` | No column is `timestamp without time zone`. |
| | `no-timetz` | No column is `time with time zone`. |
| | `prefer-text` | No column is `varchar` or `char`. |
| | `no-money` | No column is `money`. |
| | `prefer-identity` | No column is `serial`, `smallserial` or `bigserial`. |
| | `prefer-jsonb` | No column is `json`. |
| `naming.yml` | `snake-case-table` | Every table name is snake_case. |
| | `snake-case-column` | Every column name is snake_case. |

The type rules follow the PostgreSQL wiki page [Don't Do This](https://wiki.postgresql.org/wiki/Don't_Do_This).

To use them:

1. Copy the files you want into a directory of your repository, such as `lint/`. Download them from GitHub, or copy them from a clone of pistachio.
2. Delete the rules you do not want, and change the others to fit your schema.
3. Run `pista lint -r lint/ schema/*.sql`, locally and in CI.

pistachio does not read rules from a URL. Your copy changes only when you change it, so a new release of pistachio never makes your check fail.

To keep a rule but skip one object, use `-- pista:lint-ignore`. See [Turning a rule off for one object](#turning-a-rule-off-for-one-object).


## Writing a rule

A rule file is YAML with a list of rules:

```yaml
rules:
  - name: require-primary-key
    on: table
    assert: table.constraints.values().exists(c, c.type == "primary_key")
    message: the table has no primary key
```

Each rule has four fields. All of them are required.

`name`
:   The name that the output and `-- pista:lint-ignore` use. It must be unique across all the rule files.

`on`
:   The kind of object the rule checks: `table`, `column`, `index` or `foreign_key`.

`assert`
:   A [CEL](https://cel.dev/) expression. It must be true for every object of that kind, and it must return a boolean.

`message`
:   The text printed when the expression is false.

An unknown field is an error. So is a rule that does not compile, and a rule that fails while it runs, for example because it reads a field that does not exist.


## What a rule reads

A rule reads each object as [`pista parse`](../reference/commands/parse.md) writes it in JSON. Run `pista parse` on your files to see the fields. [Parsing schema files](parsing.md) describes them. Some fields, such as a column's `base_type` and an index's `columns`, exist so that a rule does not have to parse SQL text.

The variables a rule can use depend on `on`:

| `on` | Variables |
|---|---|
| `table` | `table` |
| `column` | `table`, `column` |
| `index` | `table`, `index` |
| `foreign_key` | `table`, `fk` |

Every rule can also use `doc`, the whole document. A rule that uses a variable its kind does not have fails to compile.

An index on a materialized view is checked too. For it, `table` is the view.

Most of the document is keyed by name. A table's `constraints` and `indexes` are examples. CEL macros such as `exists` loop over the keys of a map, so use `values()` to loop over the objects. A table's `columns` is a list.


## Functions

A rule can use the standard CEL functions and these:

`m.values()`
:   The values of the map `m`, as a list.

`hasPrefix(list, prefix)`
:   True if `list` starts with the elements of `prefix`.

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

`base_type` has no modifier and replaces a domain with its base type. So this rule also finds `timestamp(3)` and a domain over `timestamp`.

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

This example works only for names that need no quotes. The keys of `doc.tables` quote a name that needs it.


## Turning a rule off for one object

[`-- pista:lint-ignore`](../reference/directives.md#-pistalint-ignore) turns off the named rules for the next object. Separate several names with commas or spaces.

```sql
-- pista:lint-ignore require-primary-key
CREATE TABLE public.event_log (
    -- pista:lint-ignore prefer-timestamptz
    at timestamp NOT NULL,
    body text NOT NULL
);
```

Write it before one of these:

- `CREATE TABLE`, for the table
- a column or a `CONSTRAINT ... FOREIGN KEY` line inside `CREATE TABLE`, for that column or foreign key
- `CREATE INDEX`, for the index
- `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`, for the foreign key

It applies to that object only. The directive on a table does not turn the rule off for the table's columns.

A table marked `-- pista:ignore` is not checked at all.
