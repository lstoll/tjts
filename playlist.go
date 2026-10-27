package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/etherlabsio/go-m3u8/m3u8"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const serveChunks = 3
const maxHLSSessions = 10000

type playlist struct {
	l       logrus.FieldLogger
	indexer *chunkIndex
	store   *s3ChunkStore
	streams []configStream
	hlsSess *hlsSessions
}

func newPlaylist(l logrus.FieldLogger, s []configStream, i *chunkIndex, st *s3ChunkStore, hs *hlsSessions) *playlist {
	return &playlist{l: l, streams: s, indexer: i, store: st, hlsSess: hs}
}

// Each player gets an independent session. Sessions are local to this process.
func (p *playlist) ServePlaylist(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	// Apply to redirects and errors too: caching an entry redirect shares a sid
	// across otherwise independent listeners.
	w.Header().Set("Cache-Control", "no-store")
	sid := r.URL.Query().Get("sid")
	if sid == "" {
		stream, tz := r.URL.Query().Get("stream"), r.URL.Query().Get("tz")
		if stream == "" || tz == "" {
			http.Error(w, "stream and tz are required", http.StatusBadRequest)
			return
		}
		base := ""
		for _, s := range p.streams {
			if s.ID == stream {
				base = s.BaseTimezone
				break
			}
		}
		if base == "" {
			http.Error(w, "stream not found", http.StatusNotFound)
			return
		}
		if _, err := offsetForTimezone(base, tz); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sid = uuid.NewString()
		p.hlsSess.mu.Lock()
		if len(p.hlsSess.m) >= maxHLSSessions {
			p.hlsSess.mu.Unlock()
			http.Error(w, "too many listener sessions", http.StatusServiceUnavailable)
			return
		}
		p.hlsSess.m[sid] = sessionEntry{data: sessionData{StreamID: stream, Timezone: tz}, updatedAt: time.Now()}
		p.hlsSess.mu.Unlock()
		u := *r.URL
		u.RawQuery = (url.Values{"sid": {sid}}).Encode()
		http.Redirect(w, r, u.String(), http.StatusSeeOther)
		return
	}
	body, status := p.sessionPlaylist(r.Context(), sid, time.Now())
	if status != http.StatusOK {
		http.Error(w, body, status)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	if r.Method != http.MethodHead {
		_, _ = fmt.Fprint(w, body)
	}
}

// Keep read/advance/write atomic. Only in-memory work happens while holding the
// lock; sending the response happens afterwards so a slow player cannot block it.
func (p *playlist) sessionPlaylist(ctx context.Context, sid string, now time.Time) (string, int) {
	p.hlsSess.mu.Lock()
	defer p.hlsSess.mu.Unlock()
	entry, ok := p.hlsSess.m[sid]
	if !ok {
		return "session not found or expired", http.StatusNotFound
	}
	sess := entry.data
	stream := sess.StreamID
	base := ""
	for _, s := range p.streams {
		if s.ID == stream {
			base = s.BaseTimezone
			break
		}
	}
	if base == "" {
		return "stream not found", http.StatusNotFound
	}
	offset, err := offsetForTimezoneAt(base, sess.Timezone, now)
	if err != nil {
		return err.Error(), http.StatusBadRequest
	}
	if sess.LatestSequence == 0 {
		// At the live edge leave enough recorded audio to form a complete window.
		seq, err := p.indexer.SequenceFor(ctx, stream, now.Add(-max(offset, streamBuffer)))
		if err != nil {
			serveEndpointErrorCount.WithLabelValues("hls", stream).Inc()
			return "audio history not available", http.StatusServiceUnavailable
		}
		sess.LatestSequence, sess.IntroducedAt = seq, now
	}
	chunks, err := p.indexer.Chunks(ctx, stream, sess.LatestSequence, serveChunks*2)
	if err != nil || len(chunks) < serveChunks {
		serveEndpointErrorCount.WithLabelValues("hls", stream).Inc()
		return "audio history not available", http.StatusServiceUnavailable
	}
	target := maxDuration(chunks)
	if sess.TargetDuration == 0 {
		sess.TargetDuration = target
	}
	if target > sess.TargetDuration {
		serveEndpointErrorCount.WithLabelValues("hls", stream).Inc()
		return "source segment duration changed; reopen the stream URL", http.StatusServiceUnavailable
	}
	at := 0
	if chunks[0].Sequence != sess.LatestSequence {
		sess.LatestSequence = chunks[0].Sequence
		sess.IntroducedAt = now
	} else {
		for at < len(chunks)-serveChunks {
			if hlsWindowSize(chunks[at+1:], sess.TargetDuration) == 0 {
				break
			}
			duration := time.Duration(chunks[at].Duration * float64(time.Second))
			if now.Before(sess.IntroducedAt.Add(duration)) {
				break
			}
			sess.LatestSequence = chunks[at+1].Sequence
			sess.IntroducedAt = sess.IntroducedAt.Add(duration)
			at++
		}
	}
	windowSize := hlsWindowSize(chunks[at:], sess.TargetDuration)
	if windowSize == 0 {
		return "audio history not available", http.StatusServiceUnavailable
	}
	pl := m3u8.Playlist{Sequence: sess.LatestSequence, Version: new(4), Target: sess.TargetDuration, Live: true}
	for _, c := range chunks[at : at+windowSize] {
		u := "/chunk?" + (url.Values{"stream": {stream}, "chunk": {c.ChunkID}}).Encode()
		pl.AppendItem(&m3u8.SegmentItem{Segment: u, Duration: c.Duration})
	}
	p.hlsSess.m[sid] = sessionEntry{data: sess, updatedAt: now}
	return pl.String(), http.StatusOK
}

func (p *playlist) ServeChunk(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	stream, chunk := r.URL.Query().Get("stream"), r.URL.Query().Get("chunk")
	if stream == "" || chunk == "" {
		http.Error(w, "stream and chunk are required", http.StatusBadRequest)
		return
	}
	rc, ok := p.indexer.GetChunk(stream, chunk)
	if !ok {
		http.Error(w, "chunk not found", http.StatusNotFound)
		return
	}
	target, err := p.store.PresignedGET(r.Context(), rc)
	if err != nil {
		serveEndpointErrorCount.WithLabelValues("hls_chunk", stream).Inc()
		p.l.WithError(err).Error("presigning audio chunk")
		http.Error(w, "Unable to serve chunk", http.StatusBadGateway)
		return
	}
	http.Redirect(w, r, target, http.StatusTemporaryRedirect)
}

func maxDuration(chunks []recordedChunk) int {
	duration := 0.0
	for _, c := range chunks {
		duration = max(duration, c.Duration)
	}
	return max(1, int(math.Round(duration)))
}

func offsetForTimezone(base, user string) (time.Duration, error) {
	return offsetForTimezoneAt(base, user, time.Now())
}

func offsetForTimezoneAt(base, user string, now time.Time) (time.Duration, error) {
	source, err := time.LoadLocation(base)
	if err != nil {
		return 0, fmt.Errorf("invalid source timezone %q", base)
	}
	listener, err := time.LoadLocation(user)
	if err != nil {
		return 0, fmt.Errorf("invalid listener timezone %q", user)
	}
	_, sourceOffset := now.In(source).Zone()
	_, listenerOffset := now.In(listener).Zone()
	offset := time.Duration(sourceOffset-listenerOffset) * time.Second
	// Always delay: a listener ahead of the source hears the previous day.
	if offset < 0 {
		offset += 24 * time.Hour
	}
	return offset, nil
}

// A sliding live window must retain at least three target durations, which
// can need four short segments even when TARGETDURATION rounds to ten seconds.
func hlsWindowSize(chunks []recordedChunk, target int) int {
	duration := 0.0
	for n, c := range chunks {
		duration += c.Duration
		if n+1 >= serveChunks && duration >= float64(3*target) {
			return n + 1
		}
	}
	return 0
}
