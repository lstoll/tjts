package main

import (
	"context"
	"testing"
)

func TestSession(t *testing.T) {
	ctx := context.Background()

	db, err := newDB(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &sessionStore{db: db}

	if _, err := s.Get(ctx, "123"); err != nil {
		t.Fatal(err)
	}

	if err := s.Set(ctx, "123", sessionData{LatestSequence: 5}); err != nil {
		t.Fatal(err)
	}

	sd, err := s.Get(ctx, "123")
	if err != nil {
		t.Fatal(err)
	}

	if sd.LatestSequence != 5 {
		t.Error("session roundtrip failed")
	}
}
