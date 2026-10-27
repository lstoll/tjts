package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
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

	ctx    context.Context
	cancel context.CancelFunc
}

func newFetcher(l logrus.FieldLogger, cs *stationChunkStore, streamID, streamURL string) (*fetcher, error) {
	hc := &http.Client{
		Timeout: time.Second * 5,
	}

	u, err := url.Parse(streamURL)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %v", streamURL, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &fetcher{
		l:        l,
		hc:       hc,
		url:      u,
		streamID: streamID,
		cs:       cs,
		ctx:      ctx, cancel: cancel,
	}, nil
}

func (f *fetcher) Run() error {
	f.l.Debug("Run started")

	// set the initial ticker to fire immeditely. We'll reset it once inspecting
	// the playlist we got
	ticker := time.NewTicker(time.Nanosecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.l.Debug("tick")

			// we don't hard error in here, assume we will retry/recover
			pl, plurl, err := f.getPlaylist(f.url)
			if err != nil {
				fetchErrorCount.WithLabelValues(f.streamID).Inc()
				f.l.WithError(err).Warn("getting playlist")
				ticker.Reset(5 * time.Second)
				continue
			}

			var td time.Duration

			for n, s := range pl.Segments() {
				if err := f.downloadSegment(plurl, s, pl.Sequence+n); err != nil {
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
			ticker.Reset(rsd)
		case <-f.ctx.Done():
			return nil
		}
	}
}

func (f *fetcher) Interrupt(_ error) {
	f.cancel()
}

func (f *fetcher) getPlaylist(u *url.URL) (*m3u8.Playlist, *url.URL, error) {
	return f.getPlaylistDepth(u, 0)
}

func (f *fetcher) getPlaylistDepth(u *url.URL, depth int) (*m3u8.Playlist, *url.URL, error) {
	if depth >= 5 {
		return nil, nil, errors.New("too many nested master playlists")
	}
	req, err := http.NewRequestWithContext(f.ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	r, err := f.hc.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("wanted 200 from %s, got: %d", u.String(), r.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil {
		return nil, nil, err
	}
	if len(body) > 1<<20 {
		return nil, nil, errors.New("playlist exceeds 1 MiB")
	}
	pl, err := m3u8.Read(bytes.NewReader(body))
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
		return f.getPlaylistDepth(u, depth+1)
	}

	for _, item := range pl.Items {
		switch item := item.(type) {
		case *m3u8.MapItem:
			return nil, nil, fmt.Errorf("HLS initialization maps are unsupported")
		case *m3u8.KeyItem:
			if item.Encryptable == nil || item.Encryptable.Method != "NONE" {
				return nil, nil, fmt.Errorf("encrypted HLS is unsupported")
			}
		case *m3u8.DiscontinuityItem:
			return nil, nil, fmt.Errorf("HLS discontinuities are unsupported")
		}
	}
	if pl.IFramesOnly {
		return nil, nil, fmt.Errorf("i-frame HLS playlists are unsupported")
	}
	return pl, r.Request.URL, nil
}

func (f *fetcher) downloadSegment(playlistURL *url.URL, s *m3u8.SegmentItem, sequence int) error {
	if s.ByteRange != nil {
		return fmt.Errorf("byte-range HLS segments are unsupported")
	}
	if !validChunkDuration(s.Duration) {
		return fmt.Errorf("invalid segment duration %v", s.Duration)
	}
	segmentURL, err := resolveSegmentURL(playlistURL, s.Segment)
	if err != nil {
		return err
	}

	cn, err := chunkNameFromURL(segmentURL.String())
	if err != nil {
		return err
	}

	// The basename alone is not an identity: different paths and media
	// sequences can reuse it. Ignore rotating authentication query parameters.
	identity := segmentURL.Host + segmentURL.EscapedPath()
	cn = fmt.Sprintf("%d-%x%s", sequence, sha256.Sum256([]byte(identity)), path.Ext(cn))
	if f.cs.ChunkExists(f.ctx, cn) {
		f.l.Debugf("chunk %s exists, skipping", cn)
		return nil
	}

	f.l.Debugf("downloading chunk %s from %s", cn, segmentURL.String())
	req, err := http.NewRequestWithContext(f.ctx, http.MethodGet, segmentURL.String(), nil)
	if err != nil {
		return err
	}
	r, err := f.hc.Do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("wanted 200 from %s, got: %d", segmentURL.String(), r.StatusCode)
	}

	if err := f.cs.WriteChunk(f.ctx, cn, s.Duration, r.Body); err != nil {
		return fmt.Errorf("writing chunk: %v", err)
	}

	return nil
}

func resolveSegmentURL(playlistURL *url.URL, segment string) (*url.URL, error) {
	segmentURL, err := url.Parse(segment)
	if err != nil {
		return nil, fmt.Errorf("parsing segment %s as URL: %w", segment, err)
	}
	resolved := playlistURL.ResolveReference(segmentURL)
	if !validHTTPURL(resolved.String()) {
		return nil, fmt.Errorf("invalid HTTP segment URL")
	}
	return resolved, nil
}

func chunkNameFromURL(u string) (string, error) {
	pu, err := url.Parse(u)
	if err != nil {
		return "", fmt.Errorf("parsing %s: %v", u, err)
	}
	name := path.Base(pu.Path)
	if name == "." || name == "/" || name == "" {
		return "", fmt.Errorf("segment URL has no filename")
	}
	return name, nil
}
