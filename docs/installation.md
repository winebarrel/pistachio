# Installation

pistachio is a single binary named `pista`. Install it with one of the
methods below.

## Homebrew

```bash
brew install winebarrel/pistachio/pistachio
```

## mise

```bash
mise use github:winebarrel/pistachio            # latest
mise use github:winebarrel/pistachio@<version>  # a specific version
```

## Nix

```bash
nix profile install github:winebarrel/pistachio            # main
nix profile install github:winebarrel/pistachio/<version>  # a specific version
```

The flake builds `pista` from source. `--version` prints the commit it was
built from.

## Download binary

Download the latest binary from [Releases](https://github.com/winebarrel/pistachio/releases/latest).

| OS      | Arch         |
|---------|--------------|
| macOS   | amd64, arm64 |
| Linux   | amd64, arm64 |
| Windows | amd64        |

## Docker

To try `pista` without installing it, run the demo image, which bundles
PostgreSQL with a sample schema. See [Try it with Docker](index.md#try-it-with-docker).

## Next steps

[Getting started](getting-started.md) takes you from the first dump of a
database to the first applied change.
