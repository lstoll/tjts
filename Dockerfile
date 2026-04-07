# syntax=docker/dockerfile:1.3

FROM golang:1-trixie AS build

RUN mkdir -p /src/tjts
WORKDIR /src/tjts

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build go install ./...

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 1000 --home-dir /tmp --shell /sbin/nologin --no-create-home app

COPY --from=build /go/bin/tjts /usr/bin/tjts

USER app

ENTRYPOINT ["/usr/bin/tjts"]
