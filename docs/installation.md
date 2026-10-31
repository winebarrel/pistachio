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

## Download binary

Download the latest binary from [Releases](https://github.com/winebarrel/pistachio/releases/latest).

| OS      | Arch         |
|---------|--------------|
| macOS   | amd64, arm64 |
| Linux   | amd64, arm64 |
| Windows | amd64        |

## Docker

Each release is also published as a container image:

```bash
docker run --rm ghcr.io/winebarrel/pistachio:<tag> --help
```

The tag is the release tag, such as `v1.89.5`.

To try `pista` without installing it, run the demo image, which bundles
PostgreSQL with a sample schema. See [Try it with Docker](index.md#try-it-with-docker).

## Verifying a release

Each release asset, `checksums.txt` and the container image carry a build
provenance attestation signed in the release workflow. Verify one with the
GitHub CLI:

```bash
gh attestation verify pistachio_<version>_<os>_<arch>.tar.gz --repo winebarrel/pistachio
gh attestation verify oci://ghcr.io/winebarrel/pistachio:<tag> --repo winebarrel/pistachio
```

The `pista` binary inside each archive is attested too, so an installed
binary can be verified directly:

```bash
gh attestation verify "$(command -v pista)" --repo winebarrel/pistachio
```

The command exits with a nonzero status when the file or image was not built
by the release workflow of this repository. The demo image has no attestation.

## Next steps

[Getting started](getting-started.md) takes you from the first dump of a
database to the first applied change.
