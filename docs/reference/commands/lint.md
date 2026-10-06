# pista lint

Check schema SQL files against lint rules.

## Synopsis

```
pista lint [option...] file...
```

## Description

`pista lint` reads the files with the parser that [`pista plan`](plan.md) uses, and checks each table, column, index and foreign key against the rules in the rule files. No database is read.

A rule is a [CEL](https://cel.dev/) expression that must be true for every object of one kind. The expression reads the object as [`pista parse`](parse.md) writes it in JSON. pistachio has no built-in rules. The repository has a set of standard rules in [`rules/`](https://github.com/winebarrel/pistachio/tree/main/rules) to copy and change. See [Linting schema files](../../guides/linting.md) for the rule file format.

Each object for which a rule is false is printed on one line:

```
table public.logs: require-primary-key: the table has no primary key
column public.posts.created_at: prefer-timestamptz: use timestamp with time zone
index public.posts_created_idx: duplicate-index: another index has the same definition
foreign key posts_user_fkey on public.posts: fk-needs-index: no index starts with the columns of the foreign key
```

The objects come in the order the files declare them. For one object, the rules come in the order they are read.

A table marked [`-- pista:ignore`](../directives.md#-pistaignore) is not checked, nor is anything on it. [`-- pista:lint-ignore`](../directives.md#-pistalint-ignore) turns off named rules for one object.

## Options

The [general options](index.md#general-options) apply as well.

`-r` *path*, `--rules=`*path*
:   A rule file, or a directory. A directory's `.yml` and `.yaml` files are read in name order. Subdirectories are not read. Repeat the option, or separate the paths with commas, to read more than one. A rule name must be unique across all the files. This option is required. The environment variable is `PISTA_LINT_RULES`.

`-n` *schema*, `--schemas=`*schema*
:   The schema that qualifies an unqualified name. Only the first value is used. The default is `public`. The environment variable is `PISTA_SCHEMAS`.

## Exit status

The exit status is 0 when every rule is true for every object, 2 when a rule is false for an object, 1 when a file cannot be parsed, a rule cannot be compiled or a rule fails to evaluate, and 80 on a usage error.

## Environment

`PISTA_LINT_RULES` and `PISTA_SCHEMAS` are described above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment).

## Examples

Check a schema with the standard rules copied into `lint/`:

```bash
pista lint -r lint/ schema/*.sql
echo $?  # 0: no violations, 2: violations, 1: error
```

Set the rules in the config file:

```yaml
# pista.yml
rules:
  - lint/
```

```bash
pista -C pista.yml lint schema/*.sql
```

## See also

[Linting schema files](../../guides/linting.md), [`pista parse`](parse.md), [Directives](../directives.md)
