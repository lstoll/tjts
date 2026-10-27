package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type recordingDeleter struct {
	keys []string
}

func (r *recordingDeleter) DeleteObject(_ context.Context, key string) error {
	r.keys = append(r.keys, key)
	return nil
}

func TestGC(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	idx := newChunkIndex()
	hlsSess := newHLSSessions()

	sid1 := uuid.New().String()
	sid2 := uuid.New().String()
	hlsSess.replaceEntry(sid1, sessionData{StreamID: "keep"}, now)
	hlsSess.replaceEntry(sid2, sessionData{StreamID: "old"}, now.Add(-48*time.Hour))

	if err := idx.RecordChunk(ctx, "ts", "one", 10, now.Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := idx.RecordChunk(ctx, "ts", "two", 10, now); err != nil {
		t.Fatal(err)
	}

	del := &recordingDeleter{}
	gc := newGarbageCollector(logrus.New(), idx, del, hlsSess)

	if err := gc.collect(); err != nil {
		t.Fatal(err)
	}

	cs, err := idx.Chunks(ctx, "ts", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 1 || cs[0].ChunkID != "two" {
		t.Errorf("post GC want chunk two only, got %#v", cs)
	}

	if len(del.keys) != 1 {
		t.Errorf("want 1 object delete, got %d %v", len(del.keys), del.keys)
	}

	sd1 := hlsSess.Get(ctx, sid1)
	if sd1.StreamID != "keep" {
		t.Error("fresh session should survive GC")
	}
	sd2 := hlsSess.Get(ctx, sid2)
	if sd2.StreamID != "" {
		t.Error("stale session should be pruned")
	}
}

func TestGCDrainsBacklogAndHonorsRetention(t *testing.T) {
	ctx := context.Background()
	idx := newChunkIndex()
	now := time.Now()
	for n := range expiredChunksMax + 3 {
		if err := idx.RecordChunk(ctx, "radio", fmt.Sprintf("%d", n), 10, now.Add(-3*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if err := idx.RecordChunk(ctx, "radio", "keep", 10, now); err != nil {
		t.Fatal(err)
	}
	del := &recordingDeleter{}
	gc := newGarbageCollector(logrus.New(), idx, del, newHLSSessions())
	gc.maxAge = time.Hour
	if err := gc.collect(); err != nil {
		t.Fatal(err)
	}
	if len(del.keys) != expiredChunksMax+3 {
		t.Fatalf("backlog: deleted %d", len(del.keys))
	}
	chunks, _ := idx.Chunks(ctx, "radio", 1, 5000)
	if len(chunks) != 1 || chunks[0].ChunkID != "keep" {
		t.Fatalf("remaining chunks: %#v", chunks)
	}
	idx.Remove(chunks[0])
	next := idx.NextSequence("radio")
	if next != expiredChunksMax+5 {
		t.Fatalf("sequence reused after GC: %d", next)
	}
	// Interrupt is safe both before Run and more than once.
	gc.Interrupt(nil)
	gc.Interrupt(nil)
	done := make(chan error, 1)
	go func() { done <- gc.Run() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("GC shutdown blocked")
	}
}
