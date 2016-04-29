package iceshift

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
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
	url  *url.URL
	conn *httputil.ClientConn
}

// NewDiskShifter returns a shifter for the given stream that uses the
// disk as it's storage mechanism. It will buffer for maxOffset
func NewDiskShifter(icecastURL *url.URL, maxOffset time.Duration) (Shifter, error) {
	s := &diskShifter{
		url: icecastURL,
	}
	return s, s.start()
}

func (d *diskShifter) StreamFrom(offset time.Duration) (chan []byte, chan error) {
	dataChan := make(chan []byte)
	errChan := make(chan error)

	return dataChan, errChan
}

func (d *diskShifter) start() error {
	d.l("start", "opening-connection").Debug("Opening connection")

	tcpConn, err := net.Dial("tcp", d.url.Host)
	if err != nil {
		return err
	}
	d.conn = httputil.NewClientConn(tcpConn, nil)

	req, err := http.NewRequest("GET", d.url.Path, nil)
	if err != nil {
		return err
	}
	resp, err := d.conn.Do(req)
	if err != nil {
		return err
	}

	// timed reader resp.body

	return nil
}

func (d *diskShifter) l(fn, at string) *log.Entry {
	return log.WithFields(log.Fields{
		"module": "diskShifter",
		"fn":     fn,
		"at":     at,
	})
}
