package iceshift

import (
	"net/url"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

// Shifter represents a module that can time-shift the icecast
// bitstream.  When called for an offset, it should return a chan that
// will receive bytes to send to the user
type Shifter interface {
	StreamFrom(offset time.Duration) (chan []byte, chan error)
}

type diskShifter struct {
	client *Shout
}

// NewDiskShifter returns a shifter for the given stream that uses the
// disk as it's storage mechanism. It will buffer for maxOffset
func NewDiskShifter(icecastURL *url.URL, maxOffset time.Duration) (Shifter, error) {
	s := &diskShifter{
		client: shoutClient(icecastURL),
	}
	return s, s.start()
}

func (d *diskShifter) StreamFrom(offset time.Duration) (chan []byte, chan error) {
	dataChan := make(chan []byte)
	errChan := make(chan error)

	return dataChan, errChan
}

func (d *diskShifter) start() error {
	d.l("start", "opening-connection").Debugf("Opening connection")
	if err := d.client.Open(); err != nil {
		d.l("start", "opening-connection").Error(err)
		return err
	}

	return nil
}

func (d *diskShifter) l(fn, at string) *log.Entry {
	return log.WithFields(log.Fields{
		"module": "diskShifter",
		"fn":     fn,
		"at":     at,
	})
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
