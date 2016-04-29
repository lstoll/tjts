package iceshift

import (
	"net/url"
	"strings"
	"time"
)

// Shifter represents a module that can time-shift the icecast
// bitstream.  When called for an offset, it should return a chan that
// will receive bytes to send to the user
type Shifter interface {
	Start(offset time.Duration) chan []byte
}

// NewDiskShifter returns a shifter for the given stream that uses the
// disk as it's storage mechanism. It will buffer for maxOffset
func NewDiskShifter(icecastURL *url.URL, maxOffset time.Duration) Shifter {
	_ = shoutClient(icecastURL)
	return nil
}

func shoutClient(url *url.URL) *Shout {
	var host, port string
	spHost := strings.Split(url.Host, ":")
	host = spHost[0]
	if len(spHost) > 1 {
		port = spHost[1]
	} else {
		port = "80"
	}

	opts := map[string]string{
		"host":  host,
		"port":  port,
		"mount": url.Path,
	}
	return NewShout(opts)
}
