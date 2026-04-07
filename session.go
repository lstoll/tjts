package main

import (
	"context"
	"sync"
	"time"
)

// sessionData is per-listener HLS state (one random sid per player), held only in RAM.
// Process restart or pod loss clears all sessions; clients re-hit ?stream=&tz= like a new ICY connection.
type sessionData struct {
	LatestSequence int
	IntroducedAt   time.Time
	StreamID       string
	Timezone       string
}

type sessionEntry struct {
	data      sessionData
	updatedAt time.Time
}

// hlsSessions is an in-memory sid → state map (not S3, not disk).
type hlsSessions struct {
	mu sync.RWMutex
	m  map[string]sessionEntry
}

func newHLSSessions() *hlsSessions {
	return &hlsSessions{m: make(map[string]sessionEntry)}
}

// Get returns session data or empty values if sid is unknown.
func (s *hlsSessions) Get(_ context.Context, sid string) sessionData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.m[sid]
	if !ok {
		return sessionData{}
	}
	return e.data
}

// Set replaces state for sid and refreshes updatedAt (for idle GC).
func (s *hlsSessions) Set(_ context.Context, sid string, d sessionData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[sid] = sessionEntry{data: d, updatedAt: time.Now().UTC()}
}

// replaceEntry overwrites a session (tests / simulating age).
func (s *hlsSessions) replaceEntry(sid string, d sessionData, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[sid] = sessionEntry{data: d, updatedAt: at.UTC()}
}

// pruneSessions removes sessions not updated since cutoff.
func (s *hlsSessions) pruneSessions(cutoff time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff = cutoff.UTC()
	n := 0
	for id, e := range s.m {
		if e.updatedAt.Before(cutoff) {
			delete(s.m, id)
			n++
		}
	}
	return n
}
