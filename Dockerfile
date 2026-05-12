FROM golang:1.26-alpine AS build

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.* /src/
RUN go mod download

COPY . /src/
ARG PISTA_VERSION
RUN CGO_ENABLED=1 go build -o pista -ldflags "-X main.version=${PISTA_VERSION#v}" ./cmd/pista

FROM alpine:3

RUN apk add --no-cache ca-certificates

COPY --from=build /src/pista /usr/local/bin/

ENTRYPOINT ["pista"]
