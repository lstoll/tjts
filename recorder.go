package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// recorder maintains state metadata. It uses a sqlite DB - this is overkill for
// now, but I do plan on adding additional stuff like tracking played songs so
// it'll be a good base.
type recorder struct {
	db *sql.DB
}

func newRecorder(dbPath string) (*recorder, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening DB at %s: %v", dbPath, err)
	}

	if err := integrityCheck(context.TODO(), db); err != nil {
		return nil, fmt.Errorf("integrity check of %s failed: %v", dbPath, err)
	}

	r := &recorder{
		db: db,
	}

	if err := r.migrate(context.TODO()); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating db: %v", err)
	}

	return r, nil
}

func (r *recorder) Close() {
	r.db.Close()
}

func (r *recorder) RecordChunk(ctx context.Context, streamID, chunkID string, duration float64, timestamp time.Time) error {
	// sequence ID has to be incrementing, per stream_id
	// https://developer.apple.com/documentation/http_live_streaming/example_playlists_for_http_live_streaming/live_playlist_sliding_window_construction?language=objc

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

func (r *recorder) ChunksBefore(ctx context.Context, streamID string, before time.Time, num int) ([]recordedChunk, error) {
	before = before.UTC()

	// We always want something. If we don't find anything matching the query, return the oldest

	var count int
	if err := r.db.QueryRowContext(ctx,
		`select count(sequence) from chunks where stream_id = $1 and fetched_at < $2`,
		streamID, before).Scan(&count); err != nil {
		return nil, fmt.Errorf("counting rows: %v", err)
	}

	var (
		rows    *sql.Rows
		err     error
		reverse bool
	)

	if count < num {
		// just get the oldest
		rows, err = r.db.QueryContext(ctx,
			`select sequence, chunk_id, duration, fetched_at from chunks where stream_id = $1 order by fetched_at asc limit $2`,
			streamID, num,
		)
		reverse = false // already ascending
	} else {
		// get what we want
		rows, err = r.db.QueryContext(ctx,
			`select sequence, chunk_id, duration, fetched_at from chunks where stream_id = $1 and fetched_at < $2 order by fetched_at desc limit $3`,
			streamID, before.UTC(), num,
		)
		reverse = true // we had to descend to get the time, so need to flip 'em
	}
	if err != nil {
		return nil, fmt.Errorf("getting chunks: %v", err)
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

	if reverse {
		for i, j := 0, len(ret)-1; i < j; i, j = i+1, j-1 {
			ret[i], ret[j] = ret[j], ret[i]
		}
	}

	return ret, nil
}

func (r *recorder) execTx(ctx context.Context, f func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := f(ctx, tx); err != nil {
		// Not much we can do about an error here, but at least the database will
		// eventually cancel it on its own if it fails
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func integrityCheck(ctx context.Context, conn *sql.DB) error {
	// https://www.sqlite.org/pragma.html#pragma_integrity_check
	rows, err := conn.QueryContext(ctx, "PRAGMA integrity_check;")
	if err != nil {
		return err
	}
	defer rows.Close()

	var res []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return err
		}
		res = append(res, val)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(res) == 1 && res[0] == "ok" {
		return nil
	}

	return fmt.Errorf("integrity problems: %s", strings.Join(res, ", "))
}
