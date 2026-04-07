package main

import (
	"testing"
	"time"
)

func TestChunkKeyRoundTrip(t *testing.T) {
	const (
		stream = "doublej"
		dur    = 6.006
		cid    = "segment-001.ts"
	)
	ts := time.Date(2026, 4, 7, 12, 30, 45, 123456789, time.UTC)
	key := encodeObjectKey(stream, ts, dur, cid)
	gotStream, gotTime, gotDur, gotCID, err := decodeObjectKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if gotStream != stream || gotCID != cid {
		t.Fatalf("decode mismatch: %s %q", gotStream, gotCID)
	}
	if !gotTime.Equal(ts) {
		t.Fatalf("time: want %v got %v", ts, gotTime)
	}
	if gotDur < dur-0.001 || gotDur > dur+0.001 {
		t.Fatalf("duration: want ~%v got %v", dur, gotDur)
	}
}
