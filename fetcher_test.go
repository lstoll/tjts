package main

import (
	"net/url"
	"testing"
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
