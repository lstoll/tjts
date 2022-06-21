package main

import (
	"context"
	"errors"
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

	url      *url.URL
	streamID string

	stopC  chan struct{}
	ticker *time.Ticker
}

func newFetcher(l logrus.FieldLogger, cs *stationChunkStore, streamID, streamURL string) (*fetcher, error) {
	hc := &http.Client{
		Timeout: time.Second * 5,
	}

	u, err := url.Parse(streamURL)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %v", streamURL, err)
	}

	return &fetcher{
		l:        l,
		hc:       hc,
		url:      u,
		streamID: streamID,
		cs:       cs,
		stopC:    make(chan struct{}),
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
			pl, plurl, err := f.getPlaylist(f.url)
			if err != nil {
				fetchErrorCount.WithLabelValues(f.streamID).Inc()
				f.l.WithError(err).Warn("getting playlist")
				continue
			}

			var td time.Duration

			for _, s := range pl.Segments() {
				if err := f.downloadSegment(plurl, s); err != nil {
					fetchErrorCount.WithLabelValues(f.streamID).Inc()
					f.l.WithError(err).Warn("downloading segment")
					continue
				}
				td = td + time.Duration(s.Duration*float64(time.Second))
			}

			// set the next fetch for when ~75% of this fetch is up. that should
			// give us time to fetch/retry without being aggressive.
			rsd := time.Duration(float64(td) * 0.75)
			if rsd < 1 {
				// we probably downloaded no chunks. Cannot reset a ticker to 0
				// and probably want to wait before retrying anyway, so reset to
				// like 5s.
				rsd = 5 * time.Second
			}
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

func (f *fetcher) getPlaylist(u *url.URL) (*m3u8.Playlist, *url.URL, error) {
	r, err := f.hc.Get(u.String())
	if err != nil {
		return nil, nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("wanted 200 from %s, got: %d", u.String(), r.StatusCode)
	}

	pl, err := m3u8.Read(r.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("reading playlist from %s: %v", u.String(), err)
	}

	if pl.IsMaster() {
		// master playlist links others...
		var best *m3u8.PlaylistItem
		for _, i := range pl.Items {
			if i, ok := i.(*m3u8.PlaylistItem); ok {
				if best == nil || i.Bandwidth > best.Bandwidth {
					best = i
				}
			}
		}
		if best == nil {
			return nil, nil, errors.New("can't find anything in the master playlist")
		}
		f.l.Debugf("%s is a master playlist, using entry with bandwidth %d", u.String(), best.Bandwidth)
		// recurse to get the actual items we want
		u, err := resolveSegmentURL(r.Request.URL, best.URI)
		if err != nil {
			return nil, nil, fmt.Errorf("resolving playlist url %s: %w", best.URI, err)
		}
		return f.getPlaylist(u)
	}

	return pl, r.Request.URL, nil
}

func (f *fetcher) downloadSegment(playlistURL *url.URL, s *m3u8.SegmentItem) error {
	segmentURL, err := resolveSegmentURL(playlistURL, s.Segment)
	if err != nil {
		return err
	}

	cn, err := chunkNameFromURL(segmentURL.String())
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

	f.l.Debugf("downloading chunk %s from %s", cn, segmentURL.String())
	r, err := f.hc.Get(segmentURL.String())
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("wanted 200 from %s, got: %d", segmentURL.String(), r.StatusCode)
	}

	if err := f.cs.WriteChunk(context.TODO(), cn, s.Duration, r.Body); err != nil {
		return fmt.Errorf("writing chunk: %v", err)
	}

	return nil
}

func resolveSegmentURL(playlistURL *url.URL, segment string) (*url.URL, error) {
	segmentURL, err := url.Parse(segment)
	if err != nil {
		return nil, fmt.Errorf("parsing segment %s as URL: %w", segment, err)
	}
	return playlistURL.ResolveReference(segmentURL), nil
}

func chunkNameFromURL(u string) (string, error) {
	pu, err := url.Parse(u)
	if err != nil {
		return "", fmt.Errorf("parsing %s: %v", u, err)
	}
	return filepath.Base(pu.Path), nil
}
