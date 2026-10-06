# Commands

```
pista <command> [option...] [argument...]
```

| Command | Purpose | Reads a database |
|---|---|---|
| [`plan`](plan.md) | Print the DDL that makes the database match the schema files. | yes |
| [`apply`](apply.md) | Run that DDL. | yes |
| [`apply-from`](apply-from.md) | Run a plan file written by `plan --out`. | yes |
| [`dump`](dump.md) | Write the database schema as SQL. | yes |
| [`diff`](diff.md) | Print the DDL that changes one schema file into another. | no |
| [`fmt`](fmt.md) | Format schema files in place. | no |
| [`parse`](parse.md) | Print, as JSON, the objects that schema files declare. | no |
| [`lint`](lint.md) | Check schema files against lint rules. | no |

Each command has a page with the same sections: Synopsis, Description, Options, Exit status, Environment, Examples and See also. A page has a Notes section when the command has behavior that does not fit under one option.

By default every command targets the `public` schema. See [Working with multiple schemas](../../guides/multiple-schemas.md).

## General options

These options apply to every command. They can come before or after the command name.

`-C` *file*, `--config=`*file*
:   Load options from a YAML file. Keys are flag names. See [Configuration](../configuration.md). The environment variable is `PISTA_CONFIG`.

`--pager`, `--no-pager`
:   `--pager` sends the output through `$PISTA_PAGER` even when standard output is not a terminal. `--no-pager` turns the pager off for this run. Without either, the pager runs when standard output is a terminal and `PISTA_PAGER` is set. See [Paging long output](../configuration.md#paging-long-output).

`--version`
:   Print the version of pista, then the version of pg_query_go on a second line, and exit. pg_query_go parses and deparses the SQL.

`-h`, `--help`
:   Print the usage of the command and exit.

## Exit status

Every command exits with 0 on success and 1 on error. A usage error, such as an unknown flag or a missing argument, exits with 80. `plan --check`, `diff --check` and `fmt --check` exit with 2 to report a difference, and `lint` exits with 2 to report a violated rule. Each page says what counts. See [Exit status](../exit-status.md).

## Environment

Every option that has an environment variable names it in its entry. The variable accepts the same value as the flag. A flag overrides its variable, and the two are not merged: `PISTA_EXCLUDE='tmp_*' pista plan -E 'foo_*' schema.sql` excludes only `foo_*`.

The precedence is the command-line flag, then the environment variable, then the config file, then the default. [Environment variables](../environment.md) lists them all.

`PISTA_CONFIG`
:   The config file to load. It is the same as `--config`.

`PISTA_PAGER`
:   The command that the output is paged through. See [Paging long output](../configuration.md#paging-long-output).

## Reading an option entry

*value* in italics is a placeholder. A list option is one that "can be given more than once". It accepts several values, either by repeating the option or as one comma-separated value: `--allow-drop column,table` and `--allow-drop column --allow-drop table` are the same. An option that "cannot be used with" another is refused when both are given.
