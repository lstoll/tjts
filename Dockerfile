FROM golang:1.6

ADD . /go/src/github.com/lstoll/tjts
RUN go install github.com/lstoll/tjts/...

ENTRYPOINT /go/bin/tjts
EXPOSE 8080