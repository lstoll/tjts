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

	const (
		testStreamID = "sid"
	)

	db, err := newDB(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := &recorder{db: db}

	now := time.Now()

	for i := 1; i <= 20; i++ {
		if err := r.RecordChunk(ctx, testStreamID, fmt.Sprintf("chunk-%d", i), 10, now.Add(time.Second*10*time.Duration(i))); err != nil {
			t.Fatal(err)
		}
	}

	// move now forward until the "end" of the recorded chunks
	now = now.Add(20 * 10 * time.Second)

	seq, err := r.SequenceFor(ctx, testStreamID, now.Add(-101*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if seq != 9 {
		t.Fatalf("want seq 9, got: %d", seq)
	}

	seq, err = r.SequenceFor(ctx, testStreamID, now.Add(-1*time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	if seq != 1 {
		t.Fatalf("want seq 1, got: %d", seq)
	}
}

func TestChunks(t *testing.T) {
	ctx := context.Background()

	const (
		testStreamID = "sid"
	)

	db, err := newDB(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := &recorder{db: db}

	now := time.Now()

	for i := 1; i <= 20; i++ {
		if err := r.RecordChunk(ctx, testStreamID, fmt.Sprintf("chunk-%d", i), 10, now.Add(time.Second*10*time.Duration(i))); err != nil {
			t.Fatal(err)
		}
	}

	// move now forward until the "end" of the recorded chunks
	now = now.Add(20 * 10 * time.Second)

	cs, err := r.Chunks(ctx, testStreamID, 10, 3)
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

	db, err := newDB(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := &recorder{db: db}

	now := time.Now()

	// try and force a bit of concurrency by inserting in goroutines
	done := make(chan struct{})

	var ierrs []error

	for _, sn := range []string{streamOne, streamTwo} {
		go func(sn string) {
			defer func() { done <- struct{}{} }()
			for i := 1; i <= 5; i++ {
				if err := r.RecordChunk(ctx, sn, fmt.Sprintf("chunk-%d", i), 10, now.Add(time.Second*10*time.Duration(i))); err != nil {
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
		if err := rows.Err(); err != nil {
			t.Fatalf("in result iteration: %v", err)
		}

		want := []int{1, 2, 3, 4, 5}
		if !reflect.DeepEqual(want, sids) {
			t.Errorf("stream %s: want %v got %v", sn, want, sids)
		}
	}
}
