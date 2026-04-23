FROM golang:1.26-alpine AS build

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.* /src/
RUN go mod download

COPY . /src/
ARG PIST_VERSION
RUN CGO_ENABLED=1 go build -o pist -ldflags "-X main.version=${PIST_VERSION#v}" ./cmd/pist

FROM alpine:3

RUN apk add --no-cache ca-certificates

COPY --from=build /src/pist /usr/local/bin/

ENTRYPOINT ["pist"]
