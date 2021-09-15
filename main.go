package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"time"
)

// TripleJURL is HARDCODE
const TripleJURL = "http://live-radio01.mediahubaustralia.com/2TJW/aac/"
const DoubleJURL = "http://live-radio02.mediahubaustralia.com/DJDW/aac/"

func main() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt)

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

	tc := NewClient(TripleJURL, 16384) // 64k
	tchd := make(chan []byte, 512)
	go func() {
		if err := tc.Start(tchd); err != nil {
			log.Fatalf("error starting triplej: %v", err)
		}
	}()

	dc := NewClient(DoubleJURL, 16384) // 64 k
	dchd := make(chan []byte, 512)
	go func() {
		if err := dc.Start(dchd); err != nil {
			log.Fatalf("error starting doublej: %v", err)
		}
	}()

	var tjCache, djCache string
	if cachePath != "" {
		tjCache = cachePath + "/triplej.stream.cache"
		djCache = cachePath + "/doublej.stream.cache"
	}

	tsh, err := NewMemShifter(tchd, 2*time.Second, 20*time.Hour, tjCache, cacheInterval)
	if err != nil {
		log.Fatalf("creating shifter: %v", err)
	}
	dsh, err := NewMemShifter(dchd, 2*time.Second, 20*time.Hour, djCache, cacheInterval)
	if err != nil {
		log.Fatalf("creating shifter: %v", err)
	}
	s := NewServer()
	s.AddEndpoint("doublej", dsh)
	s.AddEndpoint("triplej", tsh)
	go func() {
		for range sc {
			log.Print("Shutdown requested")
			tsh.Shutdown()
			dsh.Shutdown()
			os.Exit(0)
		}
	}()

	s.ListenAndServe(net.JoinHostPort(host, port))
}
