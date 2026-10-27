package main

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestResolveSegmentURL(t *testing.T) {
	u, err := url.Parse("https://server/stream/playlist.m3u8")
	if err != nil {
		t.Fatal(err)
	}

	res, err := resolveSegmentURL(u, "https://absolute/url.aac")
	if err != nil {
		t.Fatal(err)
	}

	if res.String() != "https://absolute/url.aac" {
		t.Errorf("should resolve to https://absolute/url.aac , got: %s", res.String())
	}

	res, err = resolveSegmentURL(u, "file.aac")
	if err != nil {
		t.Fatal(err)
	}

	if res.String() != "https://server/stream/file.aac" {
		t.Errorf("should resolve to https://server/stream/file.aac , got: %s", res.String())
	}
}

func TestFetcherRejectsUnsupportedPlaylistsAndRecursion(t *testing.T) {
	for _, suffix := range []string{
		"#EXT-X-STREAM-INF:BANDWIDTH=1\nplaylist.m3u8\n",
		"#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:10,\none.m4s\n",
		"#EXT-X-KEY:METHOD=AES-128,URI=\"key\"\n#EXTINF:10,\none.ts\n",
	} {
		f, err := newFetcher(logrus.New(), nil, "radio", "https://upstream.invalid/playlist.m3u8")
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		f.hc.Transport = icyTestTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: 200, Request: r, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("#EXTM3U\n" + suffix))}, nil
		})
		if _, _, err := f.getPlaylist(f.url); err == nil {
			t.Fatalf("accepted unsupported playlist %s", suffix)
		}
		if calls > 5 {
			t.Fatalf("unbounded recursion: %d", calls)
		}
	}
}

func TestFetcherFailureBackoffAndShutdown(t *testing.T) {
	f, err := newFetcher(logrus.New(), nil, "radio", "https://upstream.invalid/playlist.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	calls := make(chan struct{}, 10)
	f.hc.Transport = icyTestTransport(func(r *http.Request) (*http.Response, error) {
		calls <- struct{}{}
		return &http.Response{StatusCode: 503, Request: r, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
	})
	done := make(chan error, 1)
	go func() { done <- f.Run() }()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("fetcher did not start")
	}
	select {
	case <-calls:
		t.Error("failed fetch retried without backoff")
	case <-time.After(50 * time.Millisecond):
	}
	f.Interrupt(nil)
	f.Interrupt(nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fetcher shutdown blocked")
	}
}
