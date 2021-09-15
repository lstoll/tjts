# syntax=docker/dockerfile:1.3

FROM golang:1.17 AS build

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/tmp" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid 1000 \
    app

RUN mkdir -p /src/tjts
WORKDIR /src/tjts

# COPY go.mod go.sum ./
COPY go.mod ./
RUN go mod download

COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go install ./...

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/group /etc/group

COPY --from=build /go/bin/tjts /usr/bin/tjts

ENTRYPOINT ["/usr/bin/tjts"]
