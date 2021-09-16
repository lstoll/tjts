package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// sessionData is the persisted state for a session
//
// TODO - back this on to something persistent, like the DB
type sessionData struct {
	// LatestSequence shows the newest sequence we've served. This won't be the
	// latest overall, but the sequence for the first media segment in the
	// playlist (i.e what we set EXT-X-MEDIA-SEQUENCE to)
	LatestSequence int `json:"latestSequence"`
	// IntroducedAt records when we first saw it
	IntroducedAt time.Time `json:"introducedAt"`

	StreamID string `json:"streamID"`
	Timezone string `json:"timezone"`
}

type sessionStore struct {
	db *sql.DB
}

func newSessionStore(db *sql.DB) *sessionStore {
	return &sessionStore{
		db: db,
	}
}

// Get fetches or initializes session data for a given session
func (s *sessionStore) Get(ctx context.Context, sid string) (sessionData, error) {
	var data []byte

	err := s.db.QueryRowContext(ctx, `select data from sessions where id = $1`, sid).Scan(&data)
	if err == sql.ErrNoRows {
		return sessionData{}, nil
	} else if err != nil {
		return sessionData{}, fmt.Errorf("getting session %s: %v", sid, err)
	}

	sd := sessionData{}

	if err := json.Unmarshal(data, &sd); err != nil {
		return sessionData{}, fmt.Errorf("unmarshaling session %s: %v", sid, err)
	}

	return sd, nil
}

// Sets the data for the given session ID
func (s *sessionStore) Set(ctx context.Context, sid string, d sessionData) error {
	data, err := json.Marshal(&d)
	if err != nil {
		return fmt.Errorf("marshaling data for session %s: %v", sid, err)
	}

	if _, err := s.db.ExecContext(ctx,
		`insert or replace into sessions (id, data, updated_at) values ($1, $2, $3)`,
		sid, data, time.Now()); err != nil {
		return fmt.Errorf("upserting session %s: %v", sid, err)
	}

	return nil

}
