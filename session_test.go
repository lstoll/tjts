package main

import (
	"context"
	"testing"
)

func TestSessionRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newHLSSessions()
	s.Set(ctx, "123", sessionData{LatestSequence: 5, StreamID: "x", Timezone: "y"})
	d := s.Get(ctx, "123")
	if d.LatestSequence != 5 || d.StreamID != "x" {
		t.Fatalf("got %#v", d)
	}
}
