# Configuration

## Configuration file

Use `-C` / `--config` to load options from a YAML file.

Keys are flag names (for example `conn-string`, not `conn_string`). An unknown key is an error.

```yaml
# pista.yml
conn-string: postgres://user@db.example.com/app
schemas:
  - public
  - billing
schema-map:
  staging: public
exclude:
  - tmp_*
```

```bash
pista --config pista.yml dump
pista --config pista.yml plan schema.sql

# Or set the path with an environment variable
export PISTA_CONFIG=pista.yml
pista dump
```

One file works for every command. A key that the running command does not use is ignored. The file cannot set `force`. A value there would turn off the drift check of every `apply-from` that reads the file. Pass `--force` on the command line.

A command-line flag overrides an environment variable. An environment variable overrides the config file. The config file overrides the default.


## Paging long output

Set `$PISTA_PAGER` to send the output of any command through an external command when stdout is a TTY. `sh -c` (`cmd /c` on Windows) runs the command, so quoting and arguments work as in the shell. Pipes and redirects (`pista dump > file.sql`, `pista dump | grep ...`) are not affected. The pager runs only for interactive output. Use `--no-pager` to disable it for a single invocation. Use `--pager` to force it on when stdout is not a TTY (for example, when piping into another pager-aware tool). `--pager` does nothing unless `PISTA_PAGER` is set.

```bash
# Page with less, keeping ANSI colors
PISTA_PAGER='less -R' pista dump

# Pipe through a syntax highlighter that supports SQL
PISTA_PAGER='source-highlight -s sql -f esc | less -R' pista plan schema.sql

# One-off override
pista --no-pager plan schema.sql

# Force the pager even when stdout is not a TTY
PISTA_PAGER='source-highlight -s sql -f esc' pista --pager dump
```

