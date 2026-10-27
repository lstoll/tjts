package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func testPlaylist(t *testing.T, now time.Time) *playlist {
	t.Helper()
	store, _ := testS3(t)
	for n := range 20 {
		if err := store.idx.RecordChunk(context.Background(), "radio", fmt.Sprintf("%d.ts", n), 10.0078, now.Add(time.Duration(n-20)*10*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	return newPlaylist(logrus.New(), []configStream{{ID: "radio", Name: "Radio", BaseTimezone: "Europe/Berlin"}}, store.idx, store, newHLSSessions())
}

func TestHLSSessionLifecycle(t *testing.T) {
	now := time.Now()
	p := testPlaylist(t, now)
	var sids []string
	for range 2 {
		rec := httptest.NewRecorder()
		p.ServePlaylist(rec, httptest.NewRequest("GET", "/m3u8?stream=radio&tz=Europe%2FBerlin", nil))
		if rec.Code != http.StatusSeeOther || rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("entry: %d %v", rec.Code, rec.Header())
		}
		u, err := url.Parse(rec.Header().Get("Location"))
		if err != nil {
			t.Fatal(err)
		}
		sids = append(sids, u.Query().Get("sid"))
	}
	if sids[0] == sids[1] {
		t.Fatal("listeners share session")
	}
	body, status := p.sessionPlaylist(context.Background(), sids[0], now)
	if status != 200 || !strings.Contains(body, "#EXT-X-TARGETDURATION:10") {
		t.Fatalf("playlist: %d %s", status, body)
	}
	first := p.hlsSess.Get(context.Background(), sids[0])
	if first.LatestSequence >= 20 {
		t.Fatal("started at live edge without playback buffer")
	}
	var wg sync.WaitGroup
	for n := range 16 {
		wg.Go(func() {
			if _, status := p.sessionPlaylist(context.Background(), sids[0], now.Add(time.Duration(n)*time.Second)); status != 200 {
				t.Errorf("poll: %d", status)
			}
		})
	}
	wg.Wait()
	after := p.hlsSess.Get(context.Background(), sids[0])
	if after.LatestSequence <= first.LatestSequence {
		t.Fatal("playlist did not advance")
	}
	if p.hlsSess.Get(context.Background(), sids[1]).LatestSequence != 0 {
		t.Fatal("one listener advanced another")
	}
	for _, query := range []string{"?stream=absent&tz=Europe%2FBerlin", "?stream=radio&tz=NotATimezone", "?sid=unknown"} {
		rec := httptest.NewRecorder()
		p.ServePlaylist(rec, httptest.NewRequest("GET", "/m3u8"+query, nil))
		if rec.Code < 400 {
			t.Fatalf("invalid query accepted: %s", query)
		}
	}
	if len(p.hlsSess.m) != 2 {
		t.Fatal("invalid entry allocated a session")
	}
	// Index deletion can create a sequence gap; resuming must reset its clock.
	p.hlsSess.Set(context.Background(), "gap", sessionData{StreamID: "radio", Timezone: "Europe/Berlin", LatestSequence: 1, IntroducedAt: now.Add(-time.Hour)})
	chunks, _ := p.indexer.Chunks(context.Background(), "radio", 1, 3)
	for _, c := range chunks {
		p.indexer.Remove(c)
	}
	if _, status := p.sessionPlaylist(context.Background(), "gap", now); status != 200 {
		t.Fatal(status)
	}
	gap := p.hlsSess.Get(context.Background(), "gap")
	if gap.LatestSequence != 4 || !gap.IntroducedAt.Equal(now) {
		t.Fatalf("gap recovery: %+v", gap)
	}
}

func TestTimezoneOffsetsFollowSeasons(t *testing.T) {
	for _, tc := range []struct {
		date, base, user string
		hours            int
	}{
		{"2026-01-15", "Australia/Sydney", "Europe/Berlin", 10},
		{"2026-07-15", "Australia/Sydney", "Europe/Berlin", 8},
		{"2026-10-15", "Australia/Sydney", "Europe/Berlin", 9},
		{"2026-07-15", "Europe/Berlin", "Australia/Sydney", 16},
	} {
		now, err := time.Parse("2006-01-02", tc.date)
		if err != nil {
			t.Fatal(err)
		}
		got, err := offsetForTimezoneAt(tc.base, tc.user, now)
		if err != nil || got != time.Duration(tc.hours)*time.Hour {
			t.Fatalf("%+v: %s %v", tc, got, err)
		}
	}
}

func TestHLSTargetDuration(t *testing.T) {
	for _, tc := range []struct {
		duration float64
		target   int
	}{{9.8, 10}, {10.0078, 10}, {10.6, 11}, {0.01, 1}} {
		if got := maxDuration([]recordedChunk{{Duration: tc.duration}}); got != tc.target {
			t.Fatalf("duration %v: got %d, want %d", tc.duration, got, tc.target)
		}
	}
}

func TestHLSRetainsThreeTargetDurations(t *testing.T) {
	chunks := []recordedChunk{{Duration: 9.9846}, {Duration: 9.9846}, {Duration: 9.9846}, {Duration: 9.9846}}
	if got := hlsWindowSize(chunks, 10); got != 4 {
		t.Fatalf("window size %d, want 4", got)
	}
	if got := hlsWindowSize(chunks[:3], 10); got != 0 {
		t.Fatalf("insufficient window accepted: %d", got)
	}
}
