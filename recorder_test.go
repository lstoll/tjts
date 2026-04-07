package main

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestSequenceFor(t *testing.T) {
	ctx := context.Background()

	const testStreamID = "sid"

	idx := newChunkIndex()

	now := time.Now()

	for i := 1; i <= 20; i++ {
		if err := idx.RecordChunk(ctx, testStreamID, fmt.Sprintf("chunk-%d", i), 10, now.Add(time.Second*10*time.Duration(i))); err != nil {
			t.Fatal(err)
		}
	}

	now = now.Add(20 * 10 * time.Second)

	seq, err := idx.SequenceFor(ctx, testStreamID, now.Add(-101*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if seq != 9 {
		t.Fatalf("want seq 9, got: %d", seq)
	}

	seq, err = idx.SequenceFor(ctx, testStreamID, now.Add(-1*time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	if seq != 1 {
		t.Fatalf("want seq 1, got: %d", seq)
	}
}

func TestChunks(t *testing.T) {
	ctx := context.Background()

	const testStreamID = "sid"

	idx := newChunkIndex()

	now := time.Now()

	for i := 1; i <= 20; i++ {
		if err := idx.RecordChunk(ctx, testStreamID, fmt.Sprintf("chunk-%d", i), 10, now.Add(time.Second*10*time.Duration(i))); err != nil {
			t.Fatal(err)
		}
	}

	cs, err := idx.Chunks(ctx, testStreamID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range cs {
		got = append(got, c.ChunkID)
	}

	if len(got) != 3 {
		t.Fatalf("want 3 chunks, got: %d", len(cs))
	}

	want := []string{"chunk-10", "chunk-11", "chunk-12"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %#v got %#v", want, got)
	}
}

func TestChunkRecording(t *testing.T) {
	ctx := context.Background()

	const (
		streamOne = "s-1"
		streamTwo = "s-2"
	)

	idx := newChunkIndex()

	now := time.Now()

	done := make(chan struct{})
	var ierrs []error

	for _, sn := range []string{streamOne, streamTwo} {
		go func(sn string) {
			defer func() { done <- struct{}{} }()
			for i := 1; i <= 5; i++ {
				if err := idx.RecordChunk(ctx, sn, fmt.Sprintf("chunk-%d", i), 10, now.Add(time.Second*10*time.Duration(i))); err != nil {
					ierrs = append(ierrs, err)
				}
			}
		}(sn)
	}

	<-done
	<-done

	for _, err := range ierrs {
		t.Fatal(err)
	}

	for _, sn := range []string{streamOne, streamTwo} {
		cs, err := idx.Chunks(ctx, sn, 1, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(cs) != 5 {
			t.Fatalf("stream %s: want 5 chunks, got %d", sn, len(cs))
		}
		for j, c := range cs {
			if c.Sequence != j+1 {
				t.Errorf("stream %s: want sequence %d at %d, got %d", sn, j+1, j, c.Sequence)
			}
		}
	}
}
