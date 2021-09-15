package main

import (
	"context"
	"time"
)

type chunkIndex interface {
	ChunksFrom(ctx context.Context, streamID string, from time.Time, num int) ([]string, error)
}

// playlist generates time shifted hls playlists from content
type playlist struct {
}
