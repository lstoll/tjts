package main

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestLastChunkTime(t *testing.T) {
	ctx := context.Background()

	idx := newChunkIndex()
	t0 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	if err := idx.RecordChunk(ctx, "stream-1", "chunk-1", 10, t0.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := idx.RecordChunk(ctx, "stream-1", "chunk-2", 10, t0.Add(200*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := idx.RecordChunk(ctx, "stream-2", "chunk-1", 10, t0.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := idx.RecordChunk(ctx, "stream-2", "chunk-2", 10, t0.Add(90*time.Second)); err != nil {
		t.Fatal(err)
	}

	mc := newMetricsCollector(idx)

	lcts := mc.lastChunkTimes(ctx)

	want := map[string]time.Time{
		"stream-1": t0.Add(200 * time.Second),
		"stream-2": t0.Add(90 * time.Second),
	}
	if !reflect.DeepEqual(lcts, want) {
		t.Errorf("unexpected data: %#v want %#v", lcts, want)
	}
}
