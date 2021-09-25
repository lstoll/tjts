package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestLastChunkTime(t *testing.T) {
	ctx := context.Background()

	db, err := newDB(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := &recorder{db: db}

	now := time.Now().Truncate(time.Second)

	for _, sid := range []int{1, 2} {
		// shuffle insertion order, to make sure when we query we truly get the
		// latest, and not just the last inserted or something
		var idxs []int
		for i := 1; i <= 20; i++ {
			idxs = append(idxs, i)
		}
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		rnd.Shuffle(len(idxs), func(i, j int) { idxs[i], idxs[j] = idxs[j], idxs[i] })
		for _, i := range idxs {
			if err := r.RecordChunk(ctx, fmt.Sprintf("stream-%d", sid), fmt.Sprintf("chunk-%d", i), 10, now.Add(time.Second*10*time.Duration(i))); err != nil {
				t.Fatal(err)
			}
		}
		now = now.Add(time.Hour)
	}

	mc := newMetricsCollector(db)

	lcts, err := mc.lastChunkTimes(ctx)
	if err != nil {
		log.Fatal(err)
	}

	if len(lcts) != 2 {
		log.Printf("want 2 times, got: %d", len(lcts))
	}

	if !reflect.DeepEqual(lcts, map[string]time.Time{
		"stream-1": now.Add(-2 * time.Hour).Add(20 * 10 * time.Second),
		"stream-2": now.Add(-1 * time.Hour).Add(20 * 10 * time.Second),
	}) {
		t.Errorf("unexpected data: %#v", lcts)
	}
}
