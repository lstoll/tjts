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

	// move now forward until the "end" of the recorded chunks
	now = now.Add(20 * 10 * time.Second)

	cs, err := r.ChunksBefore(ctx, testStreamID, now.Add(-101*time.Second), 3)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range cs {
		got = append(got, c.ChunkID)
	}

	if len(got) != 3 {
		log.Fatalf("want 3 chunks, got: %d", len(cs))
	}

	want := []string{"chunk-7", "chunk-8", "chunk-9"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %#v got %#v", want, got)
	}

	got = []string{}

	cs, err = r.ChunksBefore(ctx, testStreamID, now.Add(-1*time.Hour), 3)
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range cs {
		got = append(got, c.ChunkID)
	}

	if len(got) != 3 {
		log.Fatalf("want 3 chunks, got: %d", len(got))
	}

	want = []string{"chunk-1", "chunk-2", "chunk-3"}
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

	dbPath := t.TempDir() + "/db"
	r, err := newRecorder(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	now := time.Now()

	// try and force a bit of concurrency by inserting in goroutines
	done := make(chan struct{})

	var ierrs []error

	for _, sn := range []string{streamOne, streamTwo} {
		go func(sn string) {
			defer func() { done <- struct{}{} }()
			for i := 1; i <= 5; i++ {
				if err := r.RecordChunk(ctx, sn, fmt.Sprintf("chunk-%d", i), now.Add(time.Second*10*time.Duration(i))); err != nil {
					ierrs = append(ierrs, err)
				}
			}
		}(sn)
	}

	<-done
	<-done

	for _, err := range ierrs {
		log.Fatal(err)
	}

	for _, sn := range []string{streamOne, streamTwo} {
		var sids []int

		rows, err := r.db.QueryContext(ctx, "select sequence from chunks where stream_id = $1", sn)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var i int
			if err := rows.Scan(&i); err != nil {
				t.Fatal(err)
			}
			sids = append(sids, i)
		}

		want := []int{1, 2, 3, 4, 5}
		if !reflect.DeepEqual(want, sids) {
			t.Errorf("stream %s: want %v got %v", sn, want, sids)
		}
	}
}
