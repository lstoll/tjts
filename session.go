package main

import (
	"context"
	"time"
)

// sessionData is the persisted state for a session
//
// TODO - back this on to something persistent, like the DB
type sessionData struct {
	// LatestSequence shows the newest sequence we've served. This won't be the
	// latest overall, but the sequence for the first media segment in the
	// playlist (i.e what we set EXT-X-MEDIA-SEQUENCE to)
	LatestSequence int
	// IntroducedAt records when we first saw it
	IntroducedAt time.Time

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
