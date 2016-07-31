package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/lstoll/tjts"
)

// TripleJURL is HARDCODE
var TripleJURL = "http://live-radio01.mediahubaustralia.com/2TJW/aac/"

func main() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt)
	signal.Notify(sc, os.Kill)

	cachePath := os.Getenv("CACHE_PATH")
	strCacheInterval := os.Getenv("CACHE_INTERVAL")
	cacheInterval := 10 * time.Minute
	if strCacheInterval != "" {
		parsed, err := time.ParseDuration(strCacheInterval)
		if err != nil {
			log.Fatalf("Error parsing CACHE_INTERVAL=%s: %q", strCacheInterval, err)
		}
		cacheInterval = parsed
	}
	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	cht := 2 * time.Second
	c := tjts.NewClient(TripleJURL, cht)
	chd := make(chan []byte, 512)
	go func() {
		c.Start(chd)
	}()
	var tjCache string
	if cachePath != "" {
		tjCache = cachePath + "/triplej.stream.cache"
	}
	sh := tjts.NewMemShifter(chd, cht, 20*time.Hour, tjCache, cacheInterval)
	s := tjts.NewServer()
	s.AddEndpoint("triplej", sh)
	go func() {
		for range sc {
			log.Print("Shutdown requested")
			sh.Shutdown()
			os.Exit(0)
		}
	}()

	s.ListenAndServe(net.JoinHostPort(host, port))
}
