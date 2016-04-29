package main

import (
	"net/url"
	"time"

	"github.com/lstoll/iceshift"
)

func main() {
	url, err := url.Parse("http://live-radio01.mediahubaustralia.com/2TJW/aac/")
	if err != nil {
		panic(err)
	}
	server := iceshift.NewIceServer()
	server.AddEndpoint(url, "triplej", 60*time.Minute)
	server.ListenAndServe("127.0.0.1:8080")
}
