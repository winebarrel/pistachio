FROM golang:1.26 AS build

WORKDIR /src
COPY go.* /src/
RUN go mod download

COPY *.go /src/
COPY catalog/ /src/catalog/
COPY cmd/ /src/cmd/
COPY diff/ /src/diff/
COPY internal/ /src/internal/
COPY model/ /src/model/
COPY parser/ /src/parser/
ARG PIST_VERSION
RUN CGO_ENABLED=1 go build -o pist -ldflags "-X main.version=${PIST_VERSION#v}" ./cmd/pist

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/pist /usr/local/bin/

ENTRYPOINT ["pist"]
