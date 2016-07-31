package main

import (
	"time"

	"github.com/lstoll/iceshift"
)

// TripleJURL is HARDCODE
var TripleJURL = "http://live-radio01.mediahubaustralia.com/2TJW/aac/"

func main() {
	c := iceshift.NewClient(TripleJURL, 2*time.Second)
	chd := make(chan []byte, 512)
	go func() {
		c.Start(chd)
	}()
	s := iceshift.NewServer()
	s.AddEndpoint("triplej", chd)
	s.ListenAndServe("localhost:8080")
}
