package main

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/etherlabsio/go-m3u8/m3u8"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

// serveChunks are the number of chunks we send to a user
const serveChunks = 3

// playlist generates time shifted hls playlists from content
type playlist struct {
	l logrus.FieldLogger

	indexer *chunkIndex
	store   *s3ChunkStore
	streams []configStream
	hlsSess *hlsSessions

	newEntrySF singleflight.Group
}

func newPlaylist(l logrus.FieldLogger, s []configStream, i *chunkIndex, st *s3ChunkStore, hs *hlsSessions) *playlist {
	return &playlist{
		l:       l,
		indexer: i,
		streams: s,
		store:   st,
		hlsSess: hs,
	}
}

// ServePlaylist: entry ?stream=&tz= → 303 ?sid= (new UUID). Per-sid state lives only in RAM (like ICY’s
// one connection advancing through the index, but split across playlist polls). Not written to object storage.
func (p *playlist) ServePlaylist(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	now := time.Now()
	ctx := r.Context()

	q := r.URL.Query()
	sid := q.Get("sid")
	streamQ := q.Get("stream")
	tzQ := q.Get("tz")

	if sid == "" {
		if streamQ == "" || tzQ == "" {
			http.Error(w, "sid || stream and tz must be present on query", http.StatusBadRequest)
			return
		}
		sfKey := streamQ + "\x00" + tzQ + "\x00" + clientIP(r)
		v, err, _ := p.newEntrySF.Do(sfKey, func() (interface{}, error) {
			nsid := uuid.New().String()
			p.hlsSess.Set(ctx, nsid, sessionData{StreamID: streamQ, Timezone: tzQ})
			return nsid, nil
		})
		if err != nil {
			p.l.WithError(err).Error("initial set session")
			http.Error(w, "Internal error", http.StatusBadRequest)
			return
		}
		sid = v.(string)
		u := *r.URL
		u.RawQuery = (url.Values{"sid": []string{sid}}).Encode()
		http.Redirect(w, r, u.String(), http.StatusSeeOther)
		return
	}

	sess := p.hlsSess.Get(ctx, sid)
	if sess.StreamID == "" {
		http.Error(w, "session not found or expired", http.StatusNotFound)
		return
	}

	defer func() {
		p.hlsSess.Set(ctx, sid, sess)
	}()

	streamID := sess.StreamID
	tzStr := sess.Timezone

	var baseTZ string
	for _, s := range p.streams {
		if s.ID == streamID {
			baseTZ = s.BaseTimezone
			break
		}
	}
	if baseTZ == "" {
		http.Error(w, fmt.Sprintf("Stream %s not found", streamID), http.StatusNotFound)
		return
	}

	offset, err := offsetForTimezone(baseTZ, tzStr)
	if err != nil {
		p.l.WithError(err).Debugf("finding offset")
		http.Error(w, fmt.Sprintf("Error calculating offset: %s", err.Error()), http.StatusBadRequest)
		return
	}

	if sess.LatestSequence == 0 {
		s, err := p.indexer.SequenceFor(ctx, streamID, now.Add(-offset))
		if err != nil {
			serveEndpointErrorCount.WithLabelValues("hls", sid).Inc()
			p.l.WithError(err).Errorf("getting sequence for %s", streamID)
			http.Error(w, "Internal Error", http.StatusInternalServerError)
			return
		}
		sess.LatestSequence = s
		sess.IntroducedAt = now
	}

	rcs, err := p.indexer.Chunks(ctx, streamID, sess.LatestSequence, serveChunks*2)
	if err != nil {
		serveEndpointErrorCount.WithLabelValues("hls", sid).Inc()
		p.l.WithError(err).Error("getting chunks")
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	if len(rcs) < serveChunks+1 {
		serveEndpointErrorCount.WithLabelValues("hls", sid).Inc()
		p.l.Errorf("insufficient chunks for %s", streamID)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	var serveIdx int

	if rcs[0].Sequence != sess.LatestSequence {
		sess.LatestSequence = rcs[0].Sequence
	} else {
		for serveIdx < len(rcs)-serveChunks {
			chunkDur := time.Duration(rcs[serveIdx].Duration * float64(time.Second))
			if sess.IntroducedAt.Before(now.Add(-chunkDur)) {
				sess.LatestSequence = rcs[serveIdx+1].Sequence
				sess.IntroducedAt = sess.IntroducedAt.Add(chunkDur)
				serveIdx++
			} else {
				break
			}
		}
	}

	pl := m3u8.Playlist{
		Cache:    new(true),
		Sequence: sess.LatestSequence,
		Version:  new(4), // TODO - when would it not be?
		Target:   maxDuration(rcs),
		Live:     true,
	}

	for i := serveIdx; i < serveChunks+serveIdx; i++ {
		s := rcs[i]
		segURL := fmt.Sprintf("/chunk?stream=%s&chunk=%s", url.QueryEscape(streamID), url.QueryEscape(s.ChunkID))
		pl.AppendItem(&m3u8.SegmentItem{
			Segment:  segURL,
			Duration: s.Duration,
		})
	}

	w.Header().Set("content-type", "application/x-mpegURL")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	fmt.Fprint(w, pl.String())
}

// ServeChunk redirects the user to the presigned S3 URL for a specific chunk.
func (p *playlist) ServeChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	streamID := r.URL.Query().Get("stream")
	chunkID := r.URL.Query().Get("chunk")
	if streamID == "" || chunkID == "" {
		http.Error(w, "stream and chunk must be present on query", http.StatusBadRequest)
		return
	}

	rc, ok := p.indexer.GetChunk(streamID, chunkID)
	if !ok {
		http.Error(w, "chunk not found", http.StatusNotFound)
		return
	}

	segURL, err := p.store.PresignedGET(r.Context(), rc)
	if err != nil {
		serveEndpointErrorCount.WithLabelValues("hls_chunk", streamID).Inc()
		p.l.WithError(err).Error("presigning segment URL")
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, segURL, http.StatusTemporaryRedirect)
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func maxDuration(sg []recordedChunk) int {
	var max float64
	for _, s := range sg {
		if max < s.Duration {
			max = s.Duration
		}
	}
	return int(max)
}

func offsetForTimezone(baseTZ, userTZ string) (time.Duration, error) {
	tz, err := time.LoadLocation(userTZ)
	if err != nil {
		return 0, fmt.Errorf("finding user timezone %s: %v", userTZ, err)
	}

	t := time.Date(1981, 12, 6, 01, 00, 00, 00, tz)

	btz, err := time.LoadLocation(baseTZ)
	if err != nil {
		return 0, fmt.Errorf("finding base timezone %s: %v", baseTZ, err)
	}

	bt := time.Date(1981, 12, 6, 01, 00, 00, 00, btz)

	return t.Sub(bt), nil
}
