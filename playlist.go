package main

import (
	"context"
	"time"
)

type chunkIndex interface {
	// ChunksBefore should return the most recent num chunks before the given
	// time. These should be in playable order, i.e ascending in time
	ChunksBefore(ctx context.Context, streamID string, before time.Time, num int) ([]string, error)
}

// playlist generates time shifted hls playlists from content
type playlist struct {
}
