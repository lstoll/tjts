package main

import (
	"slices"
	"sync"
	"time"

	"github.com/etherlabsio/go-m3u8/m3u8"
)

const (
	// defaultSegmentLatency is used until a stream has been measured, or if
	// its playlist has no EXT-X-PROGRAM-DATE-TIME. ABC's live edge typically
	// lists a segment ~30-40s after its audio starts.
	defaultSegmentLatency = 35 * time.Second
	latencySamples        = 30
	maxSegmentLatency     = 5 * time.Minute
)

// latencyTracker measures how far each stream's recorded audio trails the
// broadcast: a chunk's FetchedAt minus the program date-time of its first
// sample. Chunk FetchedAt minus this is when the audio went to air.
type latencyTracker struct {
	mu      sync.RWMutex
	samples map[string][]time.Duration
}

func newLatencyTracker() *latencyTracker {
	return &latencyTracker{samples: make(map[string][]time.Duration)}
}

// Observe records one measurement, ignoring implausible values (clock skew,
// or a stale segment fetched from a backlog).
func (l *latencyTracker) Observe(streamID string, d time.Duration) {
	if d < 0 || d > maxSegmentLatency {
		return
	}
	l.mu.Lock()
	s := append(l.samples[streamID], d)
	if len(s) > latencySamples {
		s = s[len(s)-latencySamples:]
	}
	l.samples[streamID] = s
	l.mu.Unlock()
	segmentLatency.WithLabelValues(streamID).Set(l.Latency(streamID).Seconds())
}

// Latency returns the median of recent measurements. The median rides out
// the jitter of our poll interval.
func (l *latencyTracker) Latency(streamID string) time.Duration {
	if l == nil {
		return defaultSegmentLatency
	}
	l.mu.RLock()
	s := slices.Clone(l.samples[streamID])
	l.mu.RUnlock()
	if len(s) == 0 {
		return defaultSegmentLatency
	}
	slices.Sort(s)
	return s[len(s)/2]
}

// segmentTimes returns the program date-time of each segment in pl, aligned
// with pl.Segments(); zero where the playlist gives no anchor. A date-time
// tag may sit on a segment or stand alone before one, and later segments
// follow on by duration.
func segmentTimes(pl *m3u8.Playlist) []time.Time {
	var out []time.Time
	var next time.Time
	for _, item := range pl.Items {
		switch it := item.(type) {
		case *m3u8.TimeItem:
			next = it.Time
		case *m3u8.SegmentItem:
			t := next
			if it.ProgramDateTime != nil {
				t = it.ProgramDateTime.Time
			}
			out = append(out, t)
			next = time.Time{}
			if !t.IsZero() {
				next = t.Add(time.Duration(it.Duration * float64(time.Second)))
			}
		}
	}
	return out
}
