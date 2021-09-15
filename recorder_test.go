package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestDBOpen(t *testing.T) {
	r, err := newRecorder(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
}

func TestChunkFrom(t *testing.T) {
	ctx := context.Background()

	const (
		testStreamID = "sid"
	)

	r, err := newRecorder(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	now := time.Now()

	for i := 1; i <= 20; i++ {
		if err := r.RecordChunk(ctx, testStreamID, fmt.Sprintf("chunk-%d", i), now.Add(time.Second*10*time.Duration(i))); err != nil {
			t.Fatal(err)
		}
	}

	cs, err := r.ChunksBefore(ctx, testStreamID, now.Add(101*time.Second), 3)
	if err != nil {
		t.Fatal(err)
	}

	if len(cs) != 3 {
		log.Fatalf("want 3 chunks, got: %d", len(cs))
	}

	want := []string{"chunk-8", "chunk-9", "chunk-10"}
	if !reflect.DeepEqual(want, cs) {
		t.Errorf("want %#v got %#v", want, cs)
	}
}
