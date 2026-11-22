package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/sirupsen/logrus"
)

const (
	// abcPlaysURL is ABC's public play log. It supports from/to ranges, which
	// lets us backfill whatever we missed while down.
	abcPlaysURL = "https://music.abcradio.net.au/api/v1/plays/search.json"
	// abcPageSize is the largest page the API will return.
	abcPageSize = 100
	// abcMaxPages bounds a single fetch (24h of plays is ~4 pages).
	abcMaxPages = 20

	playsPollInterval = time.Minute
	// playsRefetchWindow re-reads recent history each poll, so late log
	// entries and corrections are picked up.
	playsRefetchWindow = 30 * time.Minute
	// playMaxLength caps how long a play is considered current when the log
	// has no duration for it.
	playMaxLength = 10 * time.Minute
	// playEndGrace keeps a track current slightly past its logged duration,
	// covering small timing drift between the log and the audio.
	playEndGrace = 15 * time.Second

	// playsObjectPrefix starts with an underscore, which stream IDs cannot, so
	// it can never collide with a stream's chunk prefix.
	playsObjectPrefix = "_meta/plays/"
)

// play is one logged track on a station.
type play struct {
	ID       string            `json:"id"`
	PlayedAt time.Time         `json:"playedAt"`
	Duration int               `json:"duration,omitempty"` // seconds; 0 if unknown
	Artist   string            `json:"artist"`
	Title    string            `json:"title"`
	Release  string            `json:"release,omitempty"`
	Artwork  string            `json:"artwork,omitempty"`
	Links    map[string]string `json:"links,omitempty"`
}

// String is the "Artist - Title" form used for ICY StreamTitle.
func (p play) String() string {
	switch {
	case p.Artist == "":
		return p.Title
	case p.Title == "":
		return p.Artist
	}
	return p.Artist + " - " + p.Title
}

// playIndex holds the known plays per stream, ordered by PlayedAt.
type playIndex struct {
	mu    sync.RWMutex
	plays map[string][]play
}

func newPlayIndex() *playIndex {
	return &playIndex{plays: make(map[string][]play)}
}

// Merge adds or replaces plays (matched by ID) and reports whether anything changed.
func (pi *playIndex) Merge(streamID string, ps []play) bool {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	cur := pi.plays[streamID]
	byID := make(map[string]int, len(cur))
	for i, p := range cur {
		byID[p.ID] = i
	}
	changed := false
	for _, p := range ps {
		if p.ID == "" || p.PlayedAt.IsZero() {
			continue
		}
		p.PlayedAt = p.PlayedAt.UTC()
		if i, ok := byID[p.ID]; ok {
			if !playEqual(cur[i], p) {
				cur[i] = p
				changed = true
			}
			continue
		}
		byID[p.ID] = len(cur)
		cur = append(cur, p)
		changed = true
	}
	if changed {
		sort.SliceStable(cur, func(i, j int) bool { return cur[i].PlayedAt.Before(cur[j].PlayedAt) })
		pi.plays[streamID] = cur
	}
	return changed
}

func playEqual(a, b play) bool {
	if a.ID != b.ID || !a.PlayedAt.Equal(b.PlayedAt) || a.Duration != b.Duration || a.Artist != b.Artist ||
		a.Title != b.Title || a.Release != b.Release || a.Artwork != b.Artwork || len(a.Links) != len(b.Links) {
		return false
	}
	for k, v := range a.Links {
		if b.Links[k] != v {
			return false
		}
	}
	return true
}

// Prune drops plays before cutoff and reports whether anything was removed.
func (pi *playIndex) Prune(streamID string, cutoff time.Time) bool {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	cur := pi.plays[streamID]
	n := sort.Search(len(cur), func(i int) bool { return !cur[i].PlayedAt.Before(cutoff) })
	if n == 0 {
		return false
	}
	pi.plays[streamID] = append([]play(nil), cur[n:]...)
	return true
}

// All returns a copy of the stream's plays, oldest first.
func (pi *playIndex) All(streamID string) []play {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return append([]play(nil), pi.plays[streamID]...)
}

// Latest returns the newest play time known for the stream.
func (pi *playIndex) Latest(streamID string) (time.Time, bool) {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	cur := pi.plays[streamID]
	if len(cur) == 0 {
		return time.Time{}, false
	}
	return cur[len(cur)-1].PlayedAt, true
}

// At returns the play that was on air at broadcast time t, if any. A play ends
// at the next play, or after its duration (plus grace) when talk follows it.
func (pi *playIndex) At(streamID string, t time.Time) (play, bool) {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	cur := pi.plays[streamID]
	i := sort.Search(len(cur), func(i int) bool { return cur[i].PlayedAt.After(t) }) - 1
	if i < 0 {
		return play{}, false
	}
	p := cur[i]
	end := p.PlayedAt.Add(playMaxLength)
	if p.Duration > 0 {
		end = p.PlayedAt.Add(time.Duration(p.Duration)*time.Second + playEndGrace)
	}
	if i+1 < len(cur) && cur[i+1].PlayedAt.Before(end) {
		end = cur[i+1].PlayedAt
	}
	if !t.Before(end) {
		return play{}, false
	}
	return p, true
}

// Recent returns up to limit plays that started in [from, to], newest first.
func (pi *playIndex) Recent(streamID string, from, to time.Time, limit int) []play {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	cur := pi.plays[streamID]
	var out []play
	for i := len(cur) - 1; i >= 0 && len(out) < limit; i-- {
		p := cur[i]
		if p.PlayedAt.After(to) {
			continue
		}
		if p.PlayedAt.Before(from) {
			break
		}
		out = append(out, p)
	}
	return out
}

// abcClient reads ABC's play log.
type abcClient struct {
	http    *http.Client
	baseURL string
}

func newABCClient() *abcClient {
	return &abcClient{http: &http.Client{Timeout: 30 * time.Second}, baseURL: abcPlaysURL}
}

type abcResponse struct {
	Total int `json:"total"`
	Items []struct {
		ARID       string    `json:"arid"`
		PlayedTime time.Time `json:"played_time"`
		Recording  struct {
			Title    string `json:"title"`
			Duration int    `json:"duration"`
			Artists  []struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"artists"`
			Releases []struct {
				Title   string       `json:"title"`
				Artwork []abcArtwork `json:"artwork"`
			} `json:"releases"`
			Artwork []abcArtwork `json:"artwork"`
		} `json:"recording"`
		Summary struct {
			Artist string            `json:"artist"`
			Title  string            `json:"title"`
			Links  map[string]string `json:"links"`
		} `json:"summary"`
	} `json:"items"`
}

type abcArtwork struct {
	URL   string `json:"url"`
	Sizes []struct {
		URL   string `json:"url"`
		Width int    `json:"width"`
	} `json:"sizes"`
}

// artworkURL picks the smallest rendition at least 300px wide, falling back
// to the largest available.
func artworkURL(arts []abcArtwork) string {
	for _, a := range arts {
		best, bestW := "", 0
		for _, s := range a.Sizes {
			if s.URL == "" {
				continue
			}
			if (bestW < 300 && s.Width > bestW) || (s.Width >= 300 && s.Width < bestW) {
				best, bestW = s.URL, s.Width
			}
		}
		if best != "" {
			return best
		}
		if a.URL != "" {
			return a.URL
		}
	}
	return ""
}

func (r abcResponse) plays() []play {
	out := make([]play, 0, len(r.Items))
	for _, it := range r.Items {
		p := play{
			ID:       it.ARID,
			PlayedAt: it.PlayedTime.UTC(),
			Duration: it.Recording.Duration,
			Artist:   it.Summary.Artist,
			Title:    it.Summary.Title,
			Links:    it.Summary.Links,
		}
		if p.Title == "" {
			p.Title = it.Recording.Title
		}
		if p.Artist == "" {
			for _, a := range it.Recording.Artists {
				if a.Type == "primary" || p.Artist == "" {
					p.Artist = a.Name
					break
				}
			}
		}
		if len(it.Recording.Releases) > 0 {
			p.Release = it.Recording.Releases[0].Title
			p.Artwork = artworkURL(it.Recording.Releases[0].Artwork)
		}
		if p.Artwork == "" {
			p.Artwork = artworkURL(it.Recording.Artwork)
		}
		out = append(out, p)
	}
	return out
}

// Plays returns all plays for station in [from, to], paging through results.
func (c *abcClient) Plays(ctx context.Context, station string, from, to time.Time) ([]play, error) {
	var out []play
	for page := 0; page < abcMaxPages; page++ {
		q := url.Values{
			"station": {station},
			"order":   {"asc"},
			"limit":   {strconv.Itoa(abcPageSize)},
			"offset":  {strconv.Itoa(page * abcPageSize)},
			"from":    {from.UTC().Format(time.RFC3339)},
			"to":      {to.UTC().Format(time.RFC3339)},
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetching plays: %w", err)
		}
		var r abcResponse
		err = func() error {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("fetching plays: status %d", resp.StatusCode)
			}
			return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&r)
		}()
		if err != nil {
			return nil, err
		}
		out = append(out, r.plays()...)
		if len(r.Items) < abcPageSize || (page+1)*abcPageSize >= r.Total {
			return out, nil
		}
	}
	return out, nil
}

// playStore persists a stream's plays as one small JSON object in S3.
type playStore struct {
	client *s3.Client
	bucket string
}

type storedPlays struct {
	Plays []play `json:"plays"`
}

func playsObjectKey(streamID string) string { return playsObjectPrefix + streamID + ".json" }

// Load returns the stored plays for a stream, or none if nothing is stored yet.
func (s *playStore) Load(ctx context.Context, streamID string) ([]play, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(playsObjectKey(streamID)),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		var re *smithyhttp.ResponseError
		if errors.As(err, &nsk) || (errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get plays for %s: %w", streamID, err)
	}
	defer out.Body.Close()
	var sp storedPlays
	if err := json.NewDecoder(io.LimitReader(out.Body, 16<<20)).Decode(&sp); err != nil {
		return nil, fmt.Errorf("decode plays for %s: %w", streamID, err)
	}
	return sp.Plays, nil
}

// Save replaces the stored plays for a stream.
func (s *playStore) Save(ctx context.Context, streamID string, ps []play) error {
	b, err := json.Marshal(storedPlays{Plays: ps})
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(playsObjectKey(streamID)),
		Body:        bytes.NewReader(b),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("put plays for %s: %w", streamID, err)
	}
	return nil
}

type playsSource interface {
	Plays(ctx context.Context, station string, from, to time.Time) ([]play, error)
}

type playsPersister interface {
	Load(ctx context.Context, streamID string) ([]play, error)
	Save(ctx context.Context, streamID string, ps []play) error
}

// playsFetcher keeps one stream's play index current from the ABC API,
// persisting it to S3 so history survives restarts and API gaps.
type playsFetcher struct {
	l         logrus.FieldLogger
	streamID  string
	station   string
	retention time.Duration

	idx   *playIndex
	src   playsSource
	store playsPersister

	ctx    context.Context
	cancel context.CancelFunc
}

func newPlaysFetcher(l logrus.FieldLogger, streamID, station string, retention time.Duration, idx *playIndex, src playsSource, store playsPersister) *playsFetcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &playsFetcher{
		l: l, streamID: streamID, station: station, retention: retention,
		idx: idx, src: src, store: store,
		ctx: ctx, cancel: cancel,
	}
}

// Load restores persisted plays. Failure is not fatal: the API backfills.
func (f *playsFetcher) Load(ctx context.Context) {
	ps, err := f.store.Load(ctx, f.streamID)
	if err != nil {
		playsErrorCount.WithLabelValues(f.streamID, "load").Inc()
		f.l.WithError(err).Warn("loading stored plays; will backfill from API")
		return
	}
	f.idx.Merge(f.streamID, ps)
	f.idx.Prune(f.streamID, time.Now().Add(-f.retention))
}

func (f *playsFetcher) Run() error {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-f.ctx.Done():
			return nil
		case <-t.C:
		}
		if err := f.poll(f.ctx, time.Now()); err != nil && f.ctx.Err() == nil {
			f.l.WithError(err).Warn("updating plays")
		}
		t.Reset(playsPollInterval)
	}
}

func (f *playsFetcher) Interrupt(_ error) { f.cancel() }

func (f *playsFetcher) poll(ctx context.Context, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	oldest := now.Add(-f.retention)
	from := oldest
	if latest, ok := f.idx.Latest(f.streamID); ok {
		if r := latest.Add(-playsRefetchWindow); r.After(oldest) {
			from = r
		}
	}
	ps, err := f.src.Plays(ctx, f.station, from, now.Add(time.Minute))
	if err != nil {
		playsErrorCount.WithLabelValues(f.streamID, "fetch").Inc()
		return err
	}
	changed := f.idx.Merge(f.streamID, ps)
	if f.idx.Prune(f.streamID, oldest) {
		changed = true
	}
	if !changed {
		return nil
	}
	if err := f.store.Save(ctx, f.streamID, f.idx.All(f.streamID)); err != nil {
		playsErrorCount.WithLabelValues(f.streamID, "save").Inc()
		return err
	}
	return nil
}
