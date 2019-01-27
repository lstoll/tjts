package tjts

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type Client struct {
	URL        string
	chunkBytes int
}

// NewClient returns a client for URL. chunkbytes is how big each chunk we grab
// is. It should correspond to the amount of data in each replay tick.
func NewClient(url string, chunkBytes int) *Client {
	return &Client{
		URL:        url,
		chunkBytes: chunkBytes,
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
				_, err := io.ReadFull(r.Body, data)
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
	resp, err := cl.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Stream returned non-200 status: %d", resp.StatusCode)
	}
	log.Printf("Connection Established %q", c.URL)
	return resp, nil
}
