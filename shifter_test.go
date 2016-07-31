package tjts

import (
	"fmt"
	"testing"
	"time"
)

func TestStreamFrom(t *testing.T) {
	s := &streamStore{
		CurrPos: 2,
		Len:     5,
	}
	m := &memShifter{
		store: s,
	}

	n := time.Now()
	s.TimeStore = []time.Time{n.Add(-2 * time.Second), n.Add(-1 * time.Second), n, n.Add(-4 * time.Second), n.Add(-3 * time.Second)}
	s.ChunkStore = [][]byte{[]byte("2"), []byte("1"), []byte("0"), []byte("4"), []byte("3")}

	fmt.Printf("++ %#v\n", s)

	_, _ = m.StreamFrom(10 * time.Second)
	sub := m.subscribers[0]
	// should be pos 3, the "oldes"
	if sub.currPos != 3 {
		t.Errorf("Expected pos 3, got %d", sub.currPos)
	}
	m.subscribers = []*subscriber{}

	_, _ = m.StreamFrom(0 * time.Second)
	sub = m.subscribers[0]
	// should be pos 2, the current
	if sub.currPos != 2 {
		t.Errorf("Expected pos 2, got %d", sub.currPos)
	}
	m.subscribers = []*subscriber{}

	_, _ = m.StreamFrom(2 * time.Second)
	sub = m.subscribers[0]
	// should be pos 1, the first one "after"
	if sub.currPos != 1 {
		t.Errorf("Expected pos 0, got %d", sub.currPos)
	}
	m.subscribers = []*subscriber{}

}
