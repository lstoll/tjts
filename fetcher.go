package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"github.com/etherlabsio/go-m3u8/m3u8"
)

type chunkStore interface {
	// WriteChunk should read fromt he provided reader, persisting it at the
	// given id and name. It should be recallable by those keys
	WriteChunk(ctx context.Context, chunkName string, r io.Reader) error
	ChunkExists(ctx context.Context, chunkName string) (bool, error)
}

// fetcher is a run group-compatible item that subscribes to a stream, and
// fetches the data as needed. The data will be stored into a chunkStore, and an
// indexManager will be used to track state
type fetcher struct {
	hc *http.Client
	cs chunkStore

	url *url.URL

	stopC  chan struct{}
	ticker *time.Ticker
}

func (f *fetcher) Run() error {
	if f.hc == nil {
		f.hc = http.DefaultClient
	}
	if f.stopC == nil {
		f.stopC = make(chan struct{})
	}

	pl, err := f.getPlaylist()
	if err != nil {
		return fmt.Errorf("getting initial playlist: %v", err)
	}

	// set the ticker to half the target duration. This should let us pick up
	// segments timely, without polling things too much.
	f.ticker = time.NewTicker(time.Duration(pl.Target) / 2 * time.Second)

	for {
		select {
		case <-f.ticker.C:
			// we don't hard error in here, assume we will retry/recover
			pl, err := f.getPlaylist()
			if err != nil {
				log.Printf("getting playlist: %v", err)
				continue
			}

			for _, s := range pl.Segments() {
				if err := f.downloadSegment(s); err != nil {
					log.Printf("downloading segment: %v", err)
					continue
				}
			}
		case <-f.stopC:
			return nil
		}
	}
}

func (f *fetcher) Interrupt(_ error) {
	f.stopC <- struct{}{}
}

func (f *fetcher) getPlaylist() (*m3u8.Playlist, error) {
	r, err := f.hc.Get(f.url.String())
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("wanted 200 from %s, got: %d", f.url.String(), r.StatusCode)
	}

	pl, err := m3u8.Read(r.Body)
	if err != nil {
		return nil, fmt.Errorf("reading playlist from %s: %v", f.url.String(), err)
	}
	return pl, nil
}

func (f *fetcher) downloadSegment(s *m3u8.SegmentItem) error {
	cn, err := chunkNameFromURL(s.Segment)
	if err != nil {
		return err
	}

	ok, err := f.cs.ChunkExists(context.TODO(), cn)
	if err != nil {
		return fmt.Errorf("checking chunk existence: %v", err)
	}
	if ok {
		log.Printf("chunk %s exists, skipping", cn)
		return nil
	}

	log.Printf("downloading chunk %s", cn)
	r, err := f.hc.Get(s.Segment)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("wanted 200 from %s, got: %d", f.url.String(), r.StatusCode)
	}

	if err := f.cs.WriteChunk(context.TODO(), cn, r.Body); err != nil {
		return fmt.Errorf("writing chunk: %v", err)
	}

	return nil
}

func chunkNameFromURL(u string) (string, error) {
	pu, err := url.Parse(u)
	if err != nil {
		return "", fmt.Errorf("parsing %s: %v", u, err)
	}
	return filepath.Base(pu.Path), nil
}
