package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"github.com/etherlabsio/go-m3u8/m3u8"
	"github.com/sirupsen/logrus"
)

// fetcher is a run group-compatible item that subscribes to a stream, and
// fetches the data as needed. The data will be stored into a chunkStore, and an
// indexManager will be used to track state
type fetcher struct {
	l logrus.FieldLogger

	hc *http.Client
	cs *stationChunkStore

	url *url.URL

	stopC  chan struct{}
	ticker *time.Ticker
}

func newFetcher(l logrus.FieldLogger, cs *stationChunkStore, streamURL string) (*fetcher, error) {
	hc := &http.Client{
		Timeout: time.Second * 5,
	}

	u, err := url.Parse(streamURL)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %v", streamURL, err)
	}

	return &fetcher{
		l:     l,
		hc:    hc,
		url:   u,
		cs:    cs,
		stopC: make(chan struct{}),
	}, nil
}

func (f *fetcher) Run() error {
	f.l.Debug("Run started")

	// set the initial ticker to fire immeditely. We'll reset it once inspecting
	// the playlist we got
	f.ticker = time.NewTicker(1 * time.Nanosecond)

	for {
		select {
		case <-f.ticker.C:
			f.l.Debug("tick")

			// we don't hard error in here, assume we will retry/recover
			pl, err := f.getPlaylist()
			if err != nil {
				f.l.WithError(err).Warn("getting playlist")
				continue
			}

			var td time.Duration

			for _, s := range pl.Segments() {
				if err := f.downloadSegment(s); err != nil {
					f.l.WithError(err).Warn("downloading segment")
					continue
				}
				td = td + time.Duration(s.Duration*float64(time.Second))
			}

			// set the next fetch for when ~75% of this fetch is up. that should
			// give us time to fetch/retry without being agressive.
			rsd := time.Duration(float64(td) * 0.75)
			f.l.Debugf("Resetting ticker to interval %s", rsd)
			f.ticker.Reset(rsd)
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
		f.l.Debugf("chunk %s exists, skipping", cn)
		return nil
	}

	f.l.Debugf("downloading chunk %s", cn)
	r, err := f.hc.Get(s.Segment)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("wanted 200 from %s, got: %d", f.url.String(), r.StatusCode)
	}

	if err := f.cs.WriteChunk(context.TODO(), cn, s.Duration, r.Body); err != nil {
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
