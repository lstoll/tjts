package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestWeb(t *testing.T) {
	streams := []configStream{
		{
			ID:         "one",
			Name:       "Stream One",
			NowPlaying: &configNowPlaying{Station: "one"},
		},
		{
			ID:   "two",
			Name: "Stream Two",
		},
	}

	i := newIndex(logrus.New(), streams)

	rec := httptest.NewRecorder()

	i.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if rec.Result().StatusCode != http.StatusOK {
		t.Errorf("want ok, got: %d", rec.Result().StatusCode)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `data-nowplaying="/nowplaying?stream=one"`) || !strings.Contains(body, `data-nowplaying=""`) {
		t.Errorf("now-playing wiring missing from page: %s", body)
	}
}

func TestNowPlayingEndpoint(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	nowFn = func() time.Time { return now }
	defer func() { nowFn = time.Now }()

	lag := 20 * time.Second
	streams := []configStream{
		{ID: "dj", Name: "Double J", BaseTimezone: "Australia/Sydney", NowPlaying: &configNowPlaying{Station: "doublej", Lag: lag}},
		{ID: "plain", Name: "Plain", BaseTimezone: "Australia/Sydney"},
	}
	// A Perth listener is 3h behind Sydney in November (AEDT vs AWST).
	shift := 3*time.Hour + lag
	listening := now.Add(-shift)
	plays := newPlayIndex()
	plays.Merge("dj", []play{
		{ID: "old", PlayedAt: listening.Add(-10 * time.Minute), Duration: 200, Title: "Old"},
		{ID: "cur", PlayedAt: listening.Add(-time.Minute), Duration: 200, Title: "Current"},
		{ID: "future", PlayedAt: listening.Add(time.Minute), Duration: 200, Title: "Not yet heard"},
	})
	srv := &nowPlayingServer{streams: streams, plays: plays, retention: 25 * time.Hour}

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/nowplaying?stream=dj&tz=Australia%2FPerth", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var resp nowPlayingResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Now == nil || resp.Now.ID != "cur" || !resp.Now.HeardAt.Equal(now.Add(-time.Minute)) {
		t.Fatalf("now = %+v", resp.Now)
	}
	if len(resp.Recent) != 1 || resp.Recent[0].ID != "old" {
		t.Fatalf("recent = %+v", resp.Recent)
	}

	for _, q := range []string{"stream=plain&tz=UTC", "stream=nope&tz=UTC", "stream=dj", "stream=dj&tz=Not/AZone"} {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest("GET", "/nowplaying?"+q, nil))
		if rec.Code == http.StatusOK {
			t.Errorf("%s: expected error", q)
		}
	}
}
