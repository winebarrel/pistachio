# Commands

```
pista <command> [option...] [argument...]
```

| Command | Purpose | Reads a database |
|---|---|---|
| [`plan`](plan.md) | Print the DDL that brings the database in line with the schema files. | yes |
| [`apply`](apply.md) | Run that DDL. | yes |
| [`apply-from`](apply-from.md) | Run a plan file written by `plan --out`. | yes |
| [`dump`](dump.md) | Write the database schema as SQL. | yes |
| [`diff`](diff.md) | Print the DDL that takes one schema file to another. | no |
| [`fmt`](fmt.md) | Lay out schema files in place. | no |
| [`parse`](parse.md) | Print the objects that schema files declare as JSON. | no |

Each command has a page with the same sections: Synopsis, Description, Options, Exit status, Environment, Examples and See also. A page has a Notes section where a command has behavior that does not fit under one option.

By default every command targets the `public` schema. See [Working with multiple schemas](../../guides/multiple-schemas.md).

## General options

These options come before or after the command name and apply to every command.

`-C` *file*, `--config=`*file*
:   Load options from a YAML file. Keys are flag names. See [Configuration](../configuration.md). The environment variable is `PISTA_CONFIG`.

`--pager`, `--no-pager`
:   `--pager` forces the output through `$PISTA_PAGER` even when standard output is not a terminal. `--no-pager` turns the pager off for this run. Without either, the pager runs when standard output is a terminal and `PISTA_PAGER` is set. See [Paging long output](../configuration.md#paging-long-output).

`--version`
:   Print the version and exit.

`-h`, `--help`
:   Print the usage of the command and exit.

## Exit status

Every command exits with 0 on success and 1 on error. A usage error, such as an unknown flag or a missing argument, exits with 80. `plan --check`, `diff --check` and `fmt --check` exit with 2 to report a difference. Each page says what counts as one. See [Exit status](../exit-status.md).

## Environment

Every option that has an environment variable names it in its entry. The variable holds the same value that the flag takes. A flag overrides its variable and the two are not merged: `PISTA_EXCLUDE='tmp_*' pista plan -E 'foo_*' schema.sql` excludes `foo_*` alone.

The precedence is the command-line flag, then the environment variable, then the config file, then the default. [Environment variables](../environment.md) lists them all.

`PISTA_CONFIG`
:   The config file that `--config` would name.

`PISTA_PAGER`
:   The command that the output is paged through. See [Paging long output](../configuration.md#paging-long-output).

## Reading an option entry

*value* in italics is a placeholder. A list option, one that "can be given more than once", takes several values either repeated or in one comma-separated value: `--allow-drop column,table` and `--allow-drop column --allow-drop table` are the same. An option that "cannot be used with" another is refused together with it.
