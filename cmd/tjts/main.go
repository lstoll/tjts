package main

import (
	"time"

	"github.com/lstoll/tjts"
)

// TripleJURL is HARDCODE
var TripleJURL = "http://live-radio01.mediahubaustralia.com/2TJW/aac/"

func main() {
	c := tjts.NewClient(TripleJURL, 2*time.Second)
	chd := make(chan []byte, 512)
	go func() {
		c.Start(chd)
	}()
	s := tjts.NewServer()
	s.AddEndpoint("triplej", chd)
	s.ListenAndServe("localhost:8080")
}
