package iceshift

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

type Client struct {
	URL        string
	chunkTime  time.Duration
	metaint    int
	bitrate    int
	chunkBytes int
}

func NewClient(url string, chunkTime time.Duration) *Client {
	return &Client{
		URL:       url,
		chunkTime: chunkTime,
	}
}

// Start begins a stream. Received data will be output on the passed in channel
// every X time, as configured. Time is calcualted via bitrate. This function is
// blocking, and retrying - it will not return on error by default.
func (c *Client) Start(receiveCh chan []byte) error {
	errch := make(chan error)

	go func() {
		for {
			r, err := c.openConn()
			if err != nil {
				log.Printf("Error opening connection: %q", err)
				time.Sleep(5 * time.Second)
				continue
			}

			for {
				data := make([]byte, c.chunkBytes)
				_, err := io.ReadAtLeast(r.Body, data, c.chunkBytes)
				if err != nil {
					log.Printf("Error reading from connection: %q", err)
					break
				}
				receiveCh <- data
			}
		}
	}()

	select {
	case err := <-errch:
		close(receiveCh)
		return err
	}
}

func (c *Client) openConn() (*http.Response, error) {
	log.Printf("Starting connection to %q", c.URL)
	cl := &http.Client{}
	req, err := http.NewRequest("GET", c.URL, nil)
	if err != nil {
		return nil, err
	}
	// req.Header.Add("Icy-MetaData", "1")
	resp, err := cl.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Stream returned non-200 status: %d", resp.StatusCode)
	}
	/*c.metaint, err = strconv.Atoi(resp.Header.Get("icy-metaint"))
	if err != nil {
		return nil, err
	}*/
	c.bitrate, err = strconv.Atoi(resp.Header.Get("icy-br"))
	if err != nil {
		return nil, err
	}
	c.chunkBytes = (c.bitrate / 8) * 1024 * int(c.chunkTime.Seconds())
	log.Printf("Connection Established %q", c.URL)
	return resp, nil
}
