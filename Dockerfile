# Image for the pista CLI, built and pushed by GoReleaser (see .goreleaser.yml).
#
# GoReleaser passes a build context that holds the already cross-built
# binaries, one directory per platform (e.g. linux/amd64/pista), so this
# Dockerfile only copies the right one in. The binary links against glibc
# because of cgo (libpg_query), so the runtime image cannot be musl-based.
FROM gcr.io/distroless/base-debian13

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/pista /usr/local/bin/pista

ENTRYPOINT ["/usr/local/bin/pista"]
