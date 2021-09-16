package main

import (
	"context"
	"time"
)

// sessionData is the persisted state for a session
//
// TODO - back this on to something persistent, like the DB
type sessionData struct {
	// Offset tracks the time the stream is lagged
	Offset time.Duration

	// The goal is to keep things ticking forward, like after we show a new
	// sequence, when it's past it's play time we should introduce a new one.

	LatestSequence int
	IntroducedAt   time.Time

	StreamID string
	Timezone string
}

type sessionStore struct {
	sessions map[string]sessionData
}

func newSessionStore() (*sessionStore, error) {
	return &sessionStore{
		sessions: map[string]sessionData{},
	}, nil
}

// Get fetches or initializes session data for a given session
func (s *sessionStore) Get(ctx context.Context, sid string) (sessionData, error) {
	return s.sessions[sid], nil
}

// Sets the data for the given session ID
func (s *sessionStore) Set(ctx context.Context, sid string, d sessionData) error {
	s.sessions[sid] = d
	return nil
}
