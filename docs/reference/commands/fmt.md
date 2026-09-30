# pista fmt

Format schema SQL files in place.

## Synopsis

```
pista fmt [option...] file...
```

## Description

`pista fmt` rewrites each file in place and prints the names of the files that changed. No database is read.

Only the whitespace between the tokens changes. A quoted identifier loses its quotes when it means the same without them. A file that does not parse is reported and left unchanged. A file whose result would not have the same tokens as the input is also reported and left unchanged. The other files are still formatted. See [Formatting schema files](../../guides/formatting.md) for the layout rules.

`pista dump` writes its output through the same formatter, so a dump needs no formatting.

## Options

The [general options](index.md#general-options) apply as well.

`--check`
:   Write nothing. Report the files that are not formatted. Exit with 2 when there is at least one such file and no file failed. The environment variable is `PISTA_FMT_CHECK`.

## Exit status

The exit status is 0 when every file was formatted. With `--check`, it is 0 when every file was already formatted. It is 1 when a file could not be parsed or written, and 80 on a usage error. With `--check`, it is 2 when a file is not formatted and no file failed.

## Environment

`PISTA_FMT_CHECK` is described above. `PISTA_CONFIG` and `PISTA_PAGER` are described under [Commands](index.md#environment).

## Examples

Format a directory, and check it in CI:

```bash
pista fmt schema/*.sql
pista fmt --check schema/*.sql
echo $?  # 0: formatted, 2: not formatted, 1: error
```

## See also

[`pista dump`](dump.md), [Formatting schema files](../../guides/formatting.md)
