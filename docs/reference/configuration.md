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

One file works for every command. Keys that the running command does not use are ignored.

Precedence: command-line flag > environment variable > config file > default.


## Paging long output

Set `$PISTA_PAGER` to forward `plan` / `apply` / `dump` output through an external command when stdout is a TTY. The command is interpreted by `sh -c` (`cmd /c` on Windows), so quoting and arguments work as in the shell. Pipes and redirects (`pista dump > file.sql`, `pista dump | grep ...`) are unaffected; the pager runs only for interactive output. Use `--no-pager` to disable it for a single invocation, or `--pager` to force it on when stdout is not a TTY (e.g. when piping into another pager-aware tool). `PISTA_PAGER` must still be set for `--pager` to do anything.

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

