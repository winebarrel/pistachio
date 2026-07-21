# syntax=docker/dockerfile:1
#
# pistachio demo image: PostgreSQL + pista CLI + sample schemas.
# Build from the repo root:
#   docker build -f demo/Dockerfile -t pistachio-demo .
# Run:
#   docker run --rm -it pistachio-demo
#
FROM golang:1.26-alpine AS build
RUN apk add --no-cache gcc musl-dev
WORKDIR /src
COPY go.* /src/
RUN go mod download
COPY . /src/
ARG PISTA_VERSION
RUN CGO_ENABLED=1 go build -o /out/pista -ldflags "-X main.version=${PISTA_VERSION#v}" ./cmd/pista

FROM postgres:18-alpine
RUN apk add --no-cache bash sudo vim less bat && \
    echo 'postgres ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/postgres-nopasswd && \
    chmod 0440 /etc/sudoers.d/postgres-nopasswd && \
    printf "alias ll='ls -lF'\nalias vi='vim'\nexport LESSOPEN='| bat --color=always -pp %%s'\nexport LESS='-R'\n" > /var/lib/postgresql/.bashrc && \
    printf "syntax on\n" > /var/lib/postgresql/.vimrc && \
    chown postgres:postgres /var/lib/postgresql/.bashrc /var/lib/postgresql/.vimrc

COPY --from=build /out/pista /usr/local/bin/pista
COPY demo/entrypoint.sh /entrypoint.sh
COPY demo/manual.txt demo/current.sql demo/desired.sql /demo/
RUN chmod +x /entrypoint.sh && chown -R postgres:postgres /demo

ENV PGHOST=/var/run/postgresql \
    PGUSER=postgres \
    PGDATABASE=demo \
    PISTA_CONN_STR=postgres://postgres@/demo \
    PISTA_PAGER="bat -l sql -p --paging=never --color=always"

WORKDIR /demo
USER postgres

# Initialise the cluster and pre-load current.sql at build time so
# every `docker run --rm` starts from the same instant-ready state.
RUN initdb -D /var/lib/postgresql/data >/dev/null && \
    pg_ctl -D /var/lib/postgresql/data -l /tmp/init.log start && \
    until pg_isready -q; do sleep 0.1; done && \
    createdb demo && \
    psql -d demo -v ON_ERROR_STOP=1 -f /demo/current.sql && \
    pg_ctl -D /var/lib/postgresql/data -m fast stop

# Source revision this image was built from (set in CI). Placed last so it
# does not invalidate the cache of the expensive initdb layer above.
ARG PISTA_REVISION
ENV PISTA_REVISION=${PISTA_REVISION}

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
