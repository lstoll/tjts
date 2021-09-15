package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

var (
	_ chunkIndex    = (*recorder)(nil)
	_ chunkRecorder = (*recorder)(nil)
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

func (r *recorder) RecordChunk(ctx context.Context, streamID, chunkID string, timestamp time.Time) error {
	_, err := r.db.ExecContext(ctx, `insert into chunks(stream_id, chunk_id, fetched_at) values ($1, $2, $3)`,
		streamID, chunkID, timestamp.UTC())
	if err != nil {
		return fmt.Errorf("inserting chunk %s/%v: %v", streamID, chunkID, err)
	}
	return nil
}

func (r *recorder) ChunksBefore(ctx context.Context, streamID string, before time.Time, num int) ([]string, error) {
	var chunks []string

	rows, err := r.db.QueryContext(ctx,
		`select chunk_id from chunks where stream_id = $1 and fetched_at < $2 order by fetched_at desc limit $3`,
		streamID, before.UTC(), num,
	)
	if err != nil {
		return nil, fmt.Errorf("getting chunks: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var chunkID string

		if err := rows.Scan(&chunkID); err != nil {
			return nil, fmt.Errorf("scanning row: %v", err)
		}

		chunks = append(chunks, chunkID)
	}

	// now reverse them, as we want them in playable order
	for i, j := 0, len(chunks)-1; i < j; i, j = i+1, j-1 {
		chunks[i], chunks[j] = chunks[j], chunks[i]
	}

	return chunks, nil
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
