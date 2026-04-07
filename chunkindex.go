package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// recordedChunk is one HLS segment we track (in memory and as an S3 object).
type recordedChunk struct {
	Sequence  int
	ChunkID   string
	Duration  float64
	FetchedAt time.Time
	ObjectKey string
}

// chunkIndex holds per-stream segment metadata in memory. It is rebuilt from S3
// listings on startup and updated as new segments are written.
type chunkIndex struct {
	mu sync.RWMutex
	// stream id -> ordered chunks (by sequence ascending)
	streams map[string][]recordedChunk
	// stream id -> set of logical chunk ids (URL basename) already stored
	logical map[string]map[string]struct{}
}

func newChunkIndex() *chunkIndex {
	return &chunkIndex{
		streams: make(map[string][]recordedChunk),
		logical: make(map[string]map[string]struct{}),
	}
}

func (c *chunkIndex) ensureStream(streamID string) {
	if c.logical[streamID] == nil {
		c.logical[streamID] = make(map[string]struct{})
	}
}

// ReplaceStream sets the in-memory index for a stream (e.g. after ListObjects).
func (c *chunkIndex) ReplaceStream(streamID string, chunks []recordedChunk) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureStream(streamID)
	cp := append([]recordedChunk(nil), chunks...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Sequence < cp[j].Sequence })
	c.streams[streamID] = cp
	seen := make(map[string]struct{})
	for _, ch := range cp {
		seen[ch.ChunkID] = struct{}{}
	}
	c.logical[streamID] = seen
}

// HasLogical returns whether we already stored this logical chunk name for the stream.
func (c *chunkIndex) HasLogical(streamID, logicalChunkID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.logical[streamID]
	if !ok {
		return false
	}
	_, ok = s[logicalChunkID]
	return ok
}

// GetChunk returns the recordedChunk for a given streamID and logicalChunkID.
func (c *chunkIndex) GetChunk(streamID, logicalChunkID string) (recordedChunk, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ch := c.streams[streamID]
	for _, rc := range ch {
		if rc.ChunkID == logicalChunkID {
			return rc, true
		}
	}
	return recordedChunk{}, false
}

// NextSequence returns the next sequence number for a new chunk (1-based).
func (c *chunkIndex) NextSequence(streamID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.streams[streamID]
	if len(ch) == 0 {
		return 1
	}
	return ch[len(ch)-1].Sequence + 1
}

// Append adds a chunk after a successful S3 upload.
func (c *chunkIndex) Append(streamID string, rc recordedChunk) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureStream(streamID)
	c.streams[streamID] = append(c.streams[streamID], rc)
	c.logical[streamID][rc.ChunkID] = struct{}{}
}

// SequenceFor returns the sequence of the newest chunk with FetchedAt strictly before before
// (shifted “listener now” in UTC). Used the same way by ICY and HLS.
//
// If before is earlier than every chunk we hold (fresh server, short retention, or GC already
// dropped history), no chunk satisfies Before(before) and we return the oldest chunk in the
// buffer — the closest we can get without a full backlog. ICY and HLS both behave this way.
func (c *chunkIndex) SequenceFor(_ context.Context, streamID string, before time.Time) (int, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ch := c.streams[streamID]
	if len(ch) == 0 {
		return -1, fmt.Errorf("no chunks for stream %s", streamID)
	}
	before = before.UTC()
	for i := len(ch) - 1; i >= 0; i-- {
		if ch[i].FetchedAt.Before(before) {
			return ch[i].Sequence, nil
		}
	}
	return ch[0].Sequence, nil
}

// Chunks returns up to num segments with sequence >= startSequence, in order.
func (c *chunkIndex) Chunks(_ context.Context, streamID string, startSequence int, num int) ([]recordedChunk, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ch := c.streams[streamID]
	var out []recordedChunk
	for _, rc := range ch {
		if rc.Sequence < startSequence {
			continue
		}
		out = append(out, rc)
		if len(out) >= num {
			break
		}
	}
	return out, nil
}

// RecordChunk appends metadata (tests; production writes via S3 then Append).
func (c *chunkIndex) RecordChunk(_ context.Context, streamID, chunkID string, duration float64, fetchedAt time.Time) error {
	if streamID == "" || chunkID == "" {
		return fmt.Errorf("streamID and chunkID cannot be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureStream(streamID)
	ch := c.streams[streamID]
	seq := 1
	if len(ch) > 0 {
		seq = ch[len(ch)-1].Sequence + 1
	}
	key := encodeObjectKey(streamID, fetchedAt.UTC(), duration, chunkID)
	rc := recordedChunk{
		Sequence:  seq,
		ChunkID:   chunkID,
		Duration:  duration,
		FetchedAt: fetchedAt.UTC(),
		ObjectKey: key,
	}
	c.streams[streamID] = append(ch, rc)
	c.logical[streamID][chunkID] = struct{}{}
	return nil
}

// LastFetchedByStream returns the newest FetchedAt per stream (for metrics).
func (c *chunkIndex) LastFetchedByStream() map[string]time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]time.Time)
	for sid, ch := range c.streams {
		if len(ch) == 0 {
			continue
		}
		var latest time.Time
		for _, x := range ch {
			if x.FetchedAt.After(latest) {
				latest = x.FetchedAt
			}
		}
		out[sid] = latest
	}
	return out
}

// ExpiredChunks returns chunks with FetchedAt before cutoff (oldest first globally), at most limit.
func (c *chunkIndex) ExpiredChunks(cutoff time.Time, limit int) []recordedChunk {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cutoff = cutoff.UTC()
	var all []recordedChunk
	for _, ch := range c.streams {
		for _, rc := range ch {
			if rc.FetchedAt.Before(cutoff) {
				all = append(all, rc)
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].FetchedAt.Before(all[j].FetchedAt) })
	if len(all) > limit {
		all = all[:limit]
	}
	return all
}

// Remove removes a chunk from the index (after object delete).
func (c *chunkIndex) Remove(rc recordedChunk) {
	c.mu.Lock()
	defer c.mu.Unlock()
	streamID := streamIDFromObjectKey(rc.ObjectKey)
	ch := c.streams[streamID]
	if len(ch) == 0 {
		return
	}
	out := ch[:0]
	for _, x := range ch {
		if x.ObjectKey == rc.ObjectKey && x.Sequence == rc.Sequence {
			continue
		}
		out = append(out, x)
	}
	if len(out) == 0 {
		delete(c.streams, streamID)
	} else {
		c.streams[streamID] = out
	}
	if c.logical[streamID] != nil {
		delete(c.logical[streamID], rc.ChunkID)
	}
}

func streamIDFromObjectKey(objectKey string) string {
	i := strings.IndexByte(objectKey, '/')
	if i <= 0 {
		return ""
	}
	return objectKey[:i]
}
