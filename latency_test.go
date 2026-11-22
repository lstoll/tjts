package main

import (
	"strings"
	"testing"
	"time"

	"github.com/etherlabsio/go-m3u8/m3u8"
)

func TestSegmentTimes(t *testing.T) {
	pl, err := m3u8.Read(strings.NewReader(`#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:11
#EXT-X-MEDIA-SEQUENCE:5863793
#EXT-X-PROGRAM-DATE-TIME:2026-11-22T00:29:42.987Z
#EXTINF:9.9846,
v0_221_d5863793.ts
#EXTINF:10.0078,
v0_221_d5863794.ts
#EXTINF:10,
v0_221_d5863795.ts
`))
	if err != nil {
		t.Fatal(err)
	}
	got := segmentTimes(pl)
	base := time.Date(2026, 11, 22, 0, 29, 42, 987000000, time.UTC)
	want := []time.Time{base, base.Add(9984600 * time.Microsecond), base.Add((9984600 + 10007800) * time.Microsecond)}
	if len(got) != len(pl.Segments()) {
		t.Fatalf("got %d times for %d segments", len(got), len(pl.Segments()))
	}
	for i := range want {
		if d := got[i].Sub(want[i]); d < -time.Millisecond || d > time.Millisecond {
			t.Errorf("segment %d: %s, want %s", i, got[i], want[i])
		}
	}

	pl, err = m3u8.Read(strings.NewReader("#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXTINF:10,\na.ts\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := segmentTimes(pl); len(got) != 1 || !got[0].IsZero() {
		t.Fatalf("no date-time should give zero times, got %v", got)
	}
}

func TestLatencyTracker(t *testing.T) {
	var nilTracker *latencyTracker
	if nilTracker.Latency("s") != defaultSegmentLatency {
		t.Fatal("nil tracker should use default")
	}
	l := newLatencyTracker()
	if l.Latency("s") != defaultSegmentLatency {
		t.Fatal("unmeasured stream should use default")
	}
	for _, d := range []time.Duration{31, 38, 33, -5, 900, 35, 32} {
		l.Observe("s", d*time.Second)
	}
	// -5s and 900s are rejected; median of 31,32,33,35,38 is 33.
	if got := l.Latency("s"); got != 33*time.Second {
		t.Fatalf("latency = %s", got)
	}
	for range latencySamples {
		l.Observe("s", 40*time.Second)
	}
	if got := l.Latency("s"); got != 40*time.Second {
		t.Fatalf("old samples not dropped: %s", got)
	}
}

func TestIcyTitleAtTracksPlayhead(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	nowFn = func() time.Time { return now }
	defer func() { nowFn = time.Now }()

	lat := newLatencyTracker()
	lat.Observe("s", 30*time.Second)
	plays := newPlayIndex()
	// Track change on air at 10:59:00 broadcast time.
	change := now.Add(-time.Hour - time.Minute)
	plays.Merge("s", []play{
		{ID: "a", PlayedAt: change.Add(-5 * time.Minute), Duration: 300, Title: "Before"},
		{ID: "b", PlayedAt: change, Duration: 300, Title: "After"},
	})
	srv := &icyServer{plays: plays, latency: lat}
	np := &configNowPlaying{Station: "s"}

	// The chunk's audio went to air 5s before the change (fetched 30s later).
	c := recordedChunk{FetchedAt: change.Add(-5*time.Second + 30*time.Second), Duration: 10}
	streamStart := now.Add(-10 * time.Second)

	// We are writing this chunk 30s ahead of the playhead: the bytes cross
	// the change, but the listener is still hearing the earlier track.
	if got := srv.titleAt("s", "Station", np, c, 40*time.Second, streamStart)(0.9); got == "After" {
		t.Fatalf("title for audio still in the buffer: %q", got)
	}
	// Caught up to real time (not ahead), the title follows the bytes.
	if got := srv.titleAt("s", "Station", np, c, 5*time.Second, streamStart)(0.9); got != "After" {
		t.Fatalf("title at playhead = %q, want After", got)
	}
	if got := srv.titleAt("s", "Station", nil, c, 0, streamStart)(0); got != "Station" {
		t.Fatalf("no now-playing config = %q", got)
	}
}
