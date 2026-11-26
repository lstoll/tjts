package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

var t0 = time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)

func testPlays() []play {
	return []play{
		{ID: "a", PlayedAt: t0, Duration: 180, Artist: "A", Title: "One"},
		{ID: "b", PlayedAt: t0.Add(3 * time.Minute), Duration: 120, Artist: "B", Title: "Two"},
		// talk gap from 5:15 to 10:00
		{ID: "c", PlayedAt: t0.Add(10 * time.Minute), Artist: "C", Title: "No duration"},
		{ID: "d", PlayedAt: t0.Add(30 * time.Minute), Duration: 60, Artist: "D", Title: "Four"},
	}
}

func TestPlayIndexAt(t *testing.T) {
	pi := newPlayIndex()
	// Merge out of order to check sorting.
	ps := testPlays()
	pi.Merge("s", []play{ps[3], ps[1]})
	pi.Merge("s", []play{ps[0], ps[2]})
	for _, tc := range []struct {
		at   time.Duration
		want string
	}{
		{-time.Second, ""},
		{0, "a"},
		{2*time.Minute + 59*time.Second, "a"},
		{3 * time.Minute, "b"},
		{5*time.Minute + 10*time.Second, "b"}, // within grace
		{6 * time.Minute, ""},                 // talk
		{10 * time.Minute, "c"},
		{19 * time.Minute, "c"}, // no duration: capped at playMaxLength
		{21 * time.Minute, ""},
		{30 * time.Minute, "d"},
		{time.Hour, ""},
	} {
		p, ok := pi.At("s", t0.Add(tc.at))
		if got := p.ID; got != tc.want || ok != (tc.want != "") {
			t.Errorf("At(+%s) = %q, want %q", tc.at, got, tc.want)
		}
	}
	if _, ok := pi.At("other", t0); ok {
		t.Error("found play on unknown stream")
	}
}

func TestPlayIndexMergePruneRecent(t *testing.T) {
	pi := newPlayIndex()
	if !pi.Merge("s", testPlays()) {
		t.Fatal("first merge reported no change")
	}
	if pi.Merge("s", testPlays()) {
		t.Fatal("identical merge reported a change")
	}
	fixed := testPlays()[1]
	fixed.Title = "Two (corrected)"
	if !pi.Merge("s", []play{fixed, {ID: ""}}) {
		t.Fatal("correction not merged")
	}
	if got := pi.All("s"); len(got) != 4 || got[1].Title != "Two (corrected)" {
		t.Fatalf("bad merge result: %+v", got)
	}

	recent := pi.Recent("s", t0.Add(time.Minute), t0.Add(15*time.Minute), 10)
	if len(recent) != 2 || recent[0].ID != "c" || recent[1].ID != "b" {
		t.Fatalf("recent = %+v", recent)
	}
	if got := pi.Recent("s", t0, t0.Add(time.Hour), 1); len(got) != 1 || got[0].ID != "d" {
		t.Fatalf("recent limit = %+v", got)
	}

	if got := pi.Upcoming("s", t0.Add(3*time.Minute), 10); len(got) != 2 || got[0].ID != "c" || got[1].ID != "d" {
		t.Fatalf("upcoming = %+v", got)
	}
	if got := pi.Upcoming("s", t0.Add(-time.Second), 1); len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("upcoming limit = %+v", got)
	}
	if got := pi.Upcoming("s", t0.Add(time.Hour), 10); len(got) != 0 {
		t.Fatalf("upcoming past end = %+v", got)
	}

	if pi.Prune("s", t0) {
		t.Fatal("prune at oldest removed something")
	}
	if !pi.Prune("s", t0.Add(5*time.Minute)) {
		t.Fatal("prune removed nothing")
	}
	if got := pi.All("s"); len(got) != 2 || got[0].ID != "c" {
		t.Fatalf("after prune: %+v", got)
	}
}

func TestPlayString(t *testing.T) {
	for _, tc := range []struct {
		p    play
		want string
	}{
		{play{Artist: "A", Title: "T"}, "A - T"},
		{play{Title: "T"}, "T"},
		{play{Artist: "A"}, "A"},
	} {
		if got := tc.p.String(); got != tc.want {
			t.Errorf("%+v = %q, want %q", tc.p, got, tc.want)
		}
	}
}

// abcItem renders one API item in the shape ABC returns.
func abcItem(i int, at time.Time) map[string]any {
	return map[string]any{
		"arid":        fmt.Sprintf("p%d", i),
		"played_time": at.Format("2006-01-02T15:04:05+00:00"),
		"summary": map[string]any{
			"artist": fmt.Sprintf("Artist %d", i), "title": fmt.Sprintf("Song %d", i),
			"links": map[string]string{
				"Spotify": "https://open.spotify.com/search/x",
				"Apple":   "https://music.apple.com/au/search?term=Artist%20Song",
			},
		},
		"release": map[string]any{
			"title":        "Album",
			"release_year": "2024",
		},
		"recording": map[string]any{
			"title": fmt.Sprintf("Song %d", i), "duration": 200,
			"artists": []map[string]any{{"name": "Artist", "type": "primary", "is_australian": i%2 == 1}},
			"releases": []map[string]any{{
				"title":        "Album",
				"release_year": nil,
				"artwork": []map[string]any{{
					"url": "https://example.com/full.jpg",
					"sizes": []map[string]any{
						{"url": "https://example.com/100.jpg", "width": 100},
						{"url": "https://example.com/1000.jpg", "width": 1000},
						{"url": "https://example.com/340.jpg", "width": 340},
						{"url": "https://example.com/580.jpg", "width": 580},
					},
				}},
			}},
		},
	}
}

func TestABCClientPaging(t *testing.T) {
	const total = 230
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		q := r.URL.Query()
		if q.Get("station") != "doublej" || q.Get("order") != "asc" || q.Get("from") != t0.Format(time.RFC3339) {
			t.Errorf("unexpected query %s", r.URL.RawQuery)
		}
		off, _ := strconv.Atoi(q.Get("offset"))
		var items []map[string]any
		for i := off; i < total && i < off+abcPageSize; i++ {
			items = append(items, abcItem(i, t0.Add(time.Duration(i)*4*time.Minute)))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"total": total, "offset": off, "limit": abcPageSize, "items": items})
	}))
	defer srv.Close()
	c := &abcClient{http: srv.Client(), baseURL: srv.URL}
	ps, err := c.Plays(context.Background(), "doublej", t0, t0.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != total || requests != 3 {
		t.Fatalf("got %d plays in %d requests", len(ps), requests)
	}
	p := ps[5]
	if p.ID != "p5" || !p.PlayedAt.Equal(t0.Add(20*time.Minute)) || p.Artist != "Artist 5" || p.Title != "Song 5" ||
		p.Duration != 200 || p.Release != "Album" || p.Year != "2024" || !p.Australian ||
		p.Artwork != "https://example.com/340.jpg" || p.Links["Spotify"] == "" ||
		p.Links["Apple"] != "https://music.apple.com/search?term=Artist%20Song" {
		t.Fatalf("bad parse: %+v", p)
	}
	if ps[4].Australian {
		t.Fatalf("expected non-Australian play: %+v", ps[4])
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer bad.Close()
	c = &abcClient{http: bad.Client(), baseURL: bad.URL}
	if _, err := c.Plays(context.Background(), "doublej", t0, t0.Add(time.Hour)); err == nil {
		t.Fatal("expected error on bad status")
	}
}

func TestPlayStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	chunks, backend := testS3(t)
	ps := &playStore{client: chunks.client, bucket: "test"}

	got, err := ps.Load(ctx, "radio")
	if err != nil || got != nil {
		t.Fatalf("missing object: %v %v", got, err)
	}
	if err := ps.Save(ctx, "radio", testPlays()); err != nil {
		t.Fatal(err)
	}
	if _, ok := backend.objects["_meta/plays/radio.json"]; !ok {
		t.Fatalf("unexpected keys: %v", backend.objects)
	}
	got, err = ps.Load(ctx, "radio")
	if err != nil || len(got) != 4 || !got[0].PlayedAt.Equal(t0) || got[3].Title != "Four" {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	// The plays object must not trip up the chunk index rebuild.
	if err := chunks.LoadStream(ctx, "radio"); err != nil {
		t.Fatalf("chunk listing broke: %v", err)
	}
}

type fakeSource struct {
	plays      []play
	err        error
	from, to   time.Time
	fetchCount int
}

func (f *fakeSource) Plays(_ context.Context, _ string, from, to time.Time) ([]play, error) {
	f.fetchCount++
	f.from, f.to = from, to
	return f.plays, f.err
}

type fakePersister struct {
	stored []play
	saves  int
	err    error
}

func (f *fakePersister) Load(context.Context, string) ([]play, error) { return f.stored, f.err }
func (f *fakePersister) Save(_ context.Context, _ string, ps []play) error {
	f.saves++
	f.stored = ps
	return f.err
}

func TestPlaysFetcherPoll(t *testing.T) {
	ctx := context.Background()
	l := logrus.New()
	l.SetOutput(io.Discard)
	now := t0.Add(time.Hour)
	nowFn = func() time.Time { return now }
	defer func() { nowFn = time.Now }()
	all := testPlays()

	src := &fakeSource{}
	store := &fakePersister{stored: all[:2]}
	pi := newPlayIndex()
	f := newPlaysFetcher(l, "s", "doublej", 24*time.Hour, pi, src, store)

	// Load restores persisted plays without saving them back.
	f.Load(ctx)
	if len(pi.All("s")) != 2 || store.saves != 0 {
		t.Fatal("load did not restore")
	}

	// Poll refetches from shortly before the newest known play.
	src.plays = all
	if err := f.poll(ctx, now); err != nil {
		t.Fatal(err)
	}
	if want := all[1].PlayedAt.Add(-playsRefetchWindow); !src.from.Equal(want) {
		t.Fatalf("from = %s", src.from)
	}
	if store.saves != 1 || len(store.stored) != 4 {
		t.Fatalf("saves=%d stored=%d", store.saves, len(store.stored))
	}

	// Nothing new: no write.
	if err := f.poll(ctx, now); err != nil || store.saves != 1 {
		t.Fatalf("unchanged poll saved: %v %d", err, store.saves)
	}

	// Retention pruning counts as a change and is persisted (keeps only "d").
	if err := f.poll(ctx, t0.Add(24*time.Hour+15*time.Minute)); err != nil || store.saves != 2 || len(store.stored) != 1 {
		t.Fatalf("prune: %v saves=%d stored=%d", err, store.saves, len(store.stored))
	}

	// Errors surface, and a failed load is not fatal.
	src.err = errors.New("down")
	if err := f.poll(ctx, now); err == nil {
		t.Fatal("expected fetch error")
	}
	store.err = errors.New("s3 down")
	f.Load(ctx)
}

func TestPlaysFetcherStartsAtRetention(t *testing.T) {
	l := logrus.New()
	l.SetOutput(io.Discard)
	src := &fakeSource{}
	f := newPlaysFetcher(l, "s", "doublej", 25*time.Hour, newPlayIndex(), src, &fakePersister{})
	if err := f.poll(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if !src.from.Equal(t0.Add(-25 * time.Hour)) {
		t.Fatalf("empty index should backfill whole retention, from=%s", src.from)
	}
}

func TestNormalizeLinks(t *testing.T) {
	got := normalizeLinks(map[string]string{
		"Apple":   "https://music.apple.com/au/search?term=BICEP%20%26%20SOARR%20Air",
		"Album":   "https://music.apple.com/au/album/x/123",
		"Spotify": "https://open.spotify.com/search/BICEP%20%26%20SOARR%20Air",
		"Bare":    "https://music.apple.com/search?term=x",
	})
	want := map[string]string{
		"Apple":   "https://music.apple.com/search?term=BICEP%20%26%20SOARR%20Air",
		"Album":   "https://music.apple.com/album/x/123",
		"Spotify": "https://open.spotify.com/search/BICEP%20%26%20SOARR%20Air",
		"Bare":    "https://music.apple.com/search?term=x",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	if normalizeLinks(nil) != nil {
		t.Error("nil links should stay nil")
	}
}
