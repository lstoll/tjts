FROM golang:1.14 as build

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/tmp" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid 1000 \
    app

COPY . /build

RUN cd /build && CGO_ENABLED=0 go install ./...

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/group /etc/group

COPY --from=build /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=build /go/bin/tjts /usr/bin/tjts

USER app:app
EXPOSE 8080

ENTRYPOINT ["/usr/bin/tjts"]
