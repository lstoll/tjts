package main

import (
	"time"

	"github.com/lstoll/tjts"
)

// TripleJURL is HARDCODE
var TripleJURL = "http://live-radio01.mediahubaustralia.com/2TJW/aac/"

func main() {
	cht := 2 * time.Second
	c := tjts.NewClient(TripleJURL, cht)
	chd := make(chan []byte, 512)
	go func() {
		c.Start(chd)
	}()
	sh := tjts.NewMemShifter(chd, cht, 20*time.Hour, "triplej.stream.cache", 10*time.Second)
	s := tjts.NewServer()
	s.AddEndpoint("triplej", sh)
	s.ListenAndServe("localhost:8080")
}
