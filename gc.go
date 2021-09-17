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
	chunkMaxAge = 24 * time.Hour

	// max number of expired chunks we act on at once
	expiredChunksMax = 1000
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

	if err := g.gcSessions(ctx); err != nil {
		return err
	}

	ecs, err := g.expiredChunks(ctx)
	if err != nil {
		return err
	}
	if len(ecs) < 1 {
		return nil
	}

	if err := g.gcChunks(ctx, ecs); err != nil {
		return err
	}

	return nil
}

func (g *garbageCollector) gcSessions(ctx context.Context) error {
	g.l.Debug("start gc sessions")
	res, err := g.db.ExecContext(ctx,
		`delete from sessions where updated_at < $1`,
		time.Now().Add(-sessMaxAge).UTC())
	if err != nil {
		return fmt.Errorf("gc sessions: %v", err)
	}
	ri, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting session delete rows affected: %d", err)
	}
	g.l.Debugf("gc'd %d sessions", ri)
	return nil
}

type delRow struct {
	ID       int
	StreamID string
	ChunkID  string
}

func (g *garbageCollector) expiredChunks(ctx context.Context) ([]delRow, error) {
	g.l.Debug("check expired chunks")

	rows, err := g.db.QueryContext(ctx,
		`select id, stream_id, chunk_id from chunks where fetched_at < $1 limit $2`,
		time.Now().Add(-chunkMaxAge).UTC(), expiredChunksMax,
	)
	if err != nil {
		return nil, fmt.Errorf("fetching expired chunks: %v", err)
	}
	defer rows.Close()

	var delrows []delRow

	for rows.Next() {
		dr := delRow{}

		if err := rows.Scan(&dr.ID, &dr.StreamID, &dr.ChunkID); err != nil {
			return nil, fmt.Errorf("scanning row: %v", err)
		}

		delrows = append(delrows, dr)
	}

	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("closing rows: %v", err)
	}

	g.l.Debugf("found %d expired chunks (max %d)", len(delrows), expiredChunksMax)

	return delrows, nil
}
func (g *garbageCollector) gcChunks(ctx context.Context, delrows []delRow) error {
	g.l.Debug("start gc chunks")

	for _, dr := range delrows {
		if err := execTx(ctx, g.db, func(ctx context.Context, tx *sql.Tx) error {
			res, err := tx.ExecContext(ctx, `delete from chunks where id = $1`, dr.ID)
			if err != nil {
				return err
			}
			if err := g.dcs.DeleteChunk(ctx, dr.StreamID, dr.ChunkID); err != nil {
				return fmt.Errorf("deleting chunk %s/%s: %v", dr.StreamID, dr.ChunkID, err)
			}
			ri, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("getting delete chunk rows affected: %d", err)
			}
			g.l.Debugf("deleting chunk %s/%s: %d rows affected", dr.StreamID, dr.ChunkID, ri)
			return nil
		}); err != nil {
			return err
		}
	}

	g.l.Debug("end gc chunks")

	return nil
}
