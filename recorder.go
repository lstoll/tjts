package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// recorder maintains state metadata. It uses a sqlite DB - this is overkill for
// now, but I do plan on adding additional stuff like tracking played songs so
// it'll be a good base.
type recorder struct {
	db *sql.DB
}

func newRecorder(db *sql.DB) (*recorder, error) {
	return &recorder{db: db}, nil
}

func (r *recorder) RecordChunk(ctx context.Context, streamID, chunkID string, duration float64, timestamp time.Time) error {
	// sequence ID has to be incrementing, per stream_id
	// https://developer.apple.com/documentation/http_live_streaming/example_playlists_for_http_live_streaming/live_playlist_sliding_window_construction?language=objc

	if streamID == "" || chunkID == "" {
		return fmt.Errorf("streamID and chunkID cannot be empty")
	}

	_, err := r.db.ExecContext(ctx,
		`insert into chunks(sequence, stream_id, chunk_id, duration, fetched_at)
		select coalesce(max(sequence),0) + 1 as sequence, $1, $2, $3, $4 from chunks where stream_id = $5`,
		streamID, chunkID, duration, timestamp.UTC(), streamID)
	if err != nil {
		return fmt.Errorf("inserting chunk %s/%v: %v", streamID, chunkID, err)
	}
	return nil
}

type recordedChunk struct {
	Sequence  int
	ChunkID   string
	Duration  float64
	FetchedAt time.Time
}

// SequenceFor returns the appropriate streaming start sequence for the given
// stream. This will either be the sequence right before the before time, of if
// we don't have one of those it will be the oldest sequence.
func (r *recorder) SequenceFor(ctx context.Context, streamID string, before time.Time) (int, error) {
	before = before.UTC()

	var seq int
	err := r.db.QueryRowContext(ctx,
		`select sequence from chunks where stream_id = $1 and fetched_at < $2 order by fetched_at desc limit 1`,
		streamID, before).Scan(&seq)
	if err == sql.ErrNoRows {
		if err := r.db.QueryRowContext(ctx,
			`select sequence from chunks where stream_id = $1 order by fetched_at asc limit 1`,
			streamID).Scan(&seq); err != nil {
			return -1, fmt.Errorf("getting oldest sequence: %v", err)
		}
	} else if err != nil {
		return -1, fmt.Errorf("getting sequence before time: %v", err)
	}
	return seq, nil
}

func (r *recorder) Chunks(ctx context.Context, streamID string, startSequence int, num int) ([]recordedChunk, error) {
	rows, err := r.db.QueryContext(ctx,
		`select sequence, chunk_id, duration, fetched_at from chunks where stream_id = $1 and sequence >= $2 order by sequence asc limit $3`,
		streamID, startSequence, num,
	)
	if err != nil {
		return nil, fmt.Errorf("fetching sequences: %v", err)
	}

	var ret []recordedChunk

	defer rows.Close()
	for rows.Next() {
		var r recordedChunk

		if err := rows.Scan(&r.Sequence, &r.ChunkID, &r.Duration, &r.FetchedAt); err != nil {
			return nil, fmt.Errorf("scanning row: %v", err)
		}

		ret = append(ret, r)
	}

	return ret, nil
}
