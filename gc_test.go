package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

func TestGC(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	const (
		testStreamID = "sid"
	)

	db, err := newDB(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rec := newRecorder(db)
	dcs := newDiskChunkStore(rec, t.TempDir(), "/lol")

	// write some shit

	for _, upd := range []time.Time{now, now.Add(-48 * time.Hour)} {
		if _, err := db.ExecContext(ctx,
			`insert into sessions (id, data, updated_at) values ($1, '{}', $2)`,
			uuid.New().String(), upd.UTC()); err != nil {
			t.Fatal(err)
		}
	}

	for _, dat := range []string{"one", "two"} {
		fs, err := dcs.FetcherStore("ts")
		if err != nil {
			t.Fatal(err)
		}
		if err := fs.WriteChunk(ctx, dat, 10, bytes.NewReader([]byte(dat))); err != nil {
			t.Fatal(err)
		}

	}
	if _, err := db.ExecContext(ctx,
		`update chunks set fetched_at = $1 where stream_id = 'ts' and sequence = 1`,
		now.Add(-48*time.Hour).UTC()); err != nil {
		t.Fatal(err)
	}

	gc := newGarbageCollector(logrus.New(), db, dcs)

	if err := gc.collect(); err != nil {
		t.Fatal(err)
	}

	var c1 int

	if err := db.QueryRowContext(ctx, `select count(*) from chunks`).Scan(&c1); err != nil {
		t.Fatal(err)
	}
	if c1 != 1 {
		t.Error("post GC should be one chunk")
	}

	// TODO - why does two scans hang?

	var c2 int

	if err := db.QueryRowContext(ctx, `select count(*) from sessions`).Scan(&c2); err != nil {
		t.Fatal(err)
	}

	if c2 != 1 {
		t.Error("post GC should be one session")
	}
}
