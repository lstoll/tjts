package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	gcInterval = 1 * time.Hour
	// time since last update we delete a session
	sessMaxAge = 12 * time.Hour
	// age at which we purge a chunk
	chunkMaxAge = 12 * time.Hour
)

// garbageCollector periodically cleans up old data
type garbageCollector struct {
	l logrus.FieldLogger

	db  *sql.DB
	dcs *diskChunkStore

	ticker *time.Ticker
	stopC  chan struct{}
}

func newGarbageCollector(l logrus.FieldLogger, db *sql.DB, dcs *diskChunkStore) *garbageCollector {
	return &garbageCollector{
		l: l,

		db:  db,
		dcs: dcs,

		stopC: make(chan struct{}),
	}
}

func (g *garbageCollector) Run() error {
	if err := g.collect(); err != nil {
		return err
	}

	g.ticker = time.NewTicker(gcInterval)

	for {
		select {
		case <-g.ticker.C:
			if err := g.collect(); err != nil {
				return fmt.Errorf("running gc: %v", err)
			}
		case <-g.stopC:
			return nil
		}
	}
}

func (g *garbageCollector) Interrupt(_ error) {
	g.stopC <- struct{}{}
}

func (g *garbageCollector) collect() error {
	ctx := context.Background()
	now := time.Now()

	g.l.Debug("GC sessions")

	_, err := g.db.ExecContext(ctx,
		`delete from sessions where updated_at < $1`,
		now.Add(-sessMaxAge).UTC())
	if err != nil {
		return fmt.Errorf("gc sessions: %v", err)
	}

	g.l.Debug("GC chunks")

	rows, err := g.db.QueryContext(ctx,
		`select id, stream_id, chunk_id from chunks where fetched_at < $1`,
		now.Add(-chunkMaxAge).UTC(),
	)
	if err != nil {
		return fmt.Errorf("fetching expired chunks: %v", err)
	}
	defer rows.Close()

	type delRow struct {
		ID       int
		StreamID string
		ChunkID  string
	}
	var delrows []delRow

	// TODO - consider if we might need batching here

	for rows.Next() {
		dr := delRow{}

		if err := rows.Scan(&dr.ID, &dr.StreamID, &dr.ChunkID); err != nil {
			return fmt.Errorf("scanning row: %v", err)
		}

		delrows = append(delrows, dr)
	}

	for _, dr := range delrows {
		if err := execTx(ctx, g.db, func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `delete from chunks where id = $1`, dr.ID); err != nil {
				return err
			}
			if err := g.dcs.DeleteChunk(ctx, dr.StreamID, dr.ChunkID); err != nil {
				return fmt.Errorf("deleting chunk %s/%s: %v", dr.StreamID, dr.ChunkID, err)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}
