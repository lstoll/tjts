package main

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/etherlabsio/go-m3u8/m3u8"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// serveChunks are the number of chunks we send to a user
const serveChunks = 3

// playlist generates time shifted hls playlists from content
type playlist struct {
	l logrus.FieldLogger

	indexer *recorder
	mapper  *diskChunkStore
	streams []configStream
	sess    *sessionStore
}

func newPlaylist(l logrus.FieldLogger, s []configStream, i *recorder, u *diskChunkStore, ss *sessionStore) *playlist {
	return &playlist{
		l:       l,
		indexer: i,
		streams: s,
		mapper:  u,
		sess:    ss,
	}
}

// ServePlaylist handles a request to build a m38u playlist for a stream
func (p *playlist) ServePlaylist(w http.ResponseWriter, r *http.Request) {
	p.l.Debugf("serving request for %s", r.URL.String())

	now := time.Now()
	ctx := r.Context()

	sid := r.URL.Query().Get("sid")
	if sid == "" {
		// start a new session, and switch to that URL
		sid = uuid.New().String()
		sess := sessionData{
			StreamID: r.URL.Query().Get("stream"),
			Timezone: r.URL.Query().Get("tz"),
		}
		if sess.StreamID == "" || sess.Timezone == "" {
			http.Error(w, "sid || stream and tz must be present on query", http.StatusBadRequest)
			return
		}
		if err := p.sess.Set(ctx, sid, sess); err != nil {
			p.l.WithError(err).Error("initial set session")
			http.Error(w, "Internal error", http.StatusBadRequest)
			return
		}
		u := *r.URL
		u.RawQuery = (url.Values{"sid": []string{sid}}).Encode()
		http.Redirect(w, r, u.String(), http.StatusSeeOther)
		return
	}

	sess, err := p.sess.Get(ctx, sid)
	if err != nil {
		p.l.WithError(err).Errorf("getting session %s", sid)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	defer func() {
		s := &sess
		p.l.Debugf("updating session to %#v", *s)
		if err := p.sess.Set(ctx, sid, *s); err != nil {
			p.l.WithError(err).Error("updating session")
		}
	}()

	// if the stream has no latest sequence, we want to find one. This will be
	// either the sequence that corresponds to our offset time, or if we have no
	// sequence older than that it'll be the oldest sequence.
	//
	// once we have a sequence, we want to save in the session the sequence ID
	// and when it was seen. On subsequent requests, we grab that sequence plus
	// a couple newer items than it. If the latest sequence introduced at + it's
	// duration is before now, drop it off, add some new sequences, and update
	// the session.
	if sess.LatestSequence == 0 {
		// We have no current offset, so calculate the difference between the
		// same contrived time in the different timezones to figure their
		// offset. Use that to set a sequence
		var baseTZ string
		for _, s := range p.streams {
			if s.ID == sess.StreamID {
				baseTZ = s.BaseTimezone
			}
		}
		if baseTZ == "" {
			http.Error(w, fmt.Sprintf("Stream %s not found", sess.StreamID), http.StatusNotFound)
			return
		}

		offset, err := offsetForTimezone(baseTZ, sess.Timezone)
		if err != nil {
			p.l.WithError(err).Debugf("finding offset")
			http.Error(w, fmt.Sprintf("Error calculating offset: %s", err.Error()), http.StatusBadRequest)
			return
		}

		s, err := p.indexer.SequenceFor(ctx, sess.StreamID, now.Add(-offset))
		if err != nil {
			p.l.WithError(err).Errorf("getting sequence for %s", sess.StreamID)
			http.Error(w, "Internal Error", http.StatusBadRequest)
			return
		}
		sess.LatestSequence = s
		sess.IntroducedAt = now
	}

	// grab some chunks from our latest sequence. Get extra, in case we have to shift forward
	rcs, err := p.indexer.Chunks(ctx, sess.StreamID, sess.LatestSequence, serveChunks*2)
	if err != nil {
		p.l.WithError(err).Error("getting chunks")
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	if len(rcs) < serveChunks+1 {
		p.l.Errorf("insufficient chunks for %s", sess.StreamID)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	var serveIdx int

	if rcs[0].Sequence != sess.LatestSequence {
		// if the first sequence returned is not our latest sequence, move latest
		// sequence to match.
		sess.LatestSequence = rcs[0].Sequence
	} else if sess.IntroducedAt.Before(now.Add(-time.Duration(rcs[0].Duration))) {
		// we've moved beyond the last sequences time.  shift things forward.
		sess.LatestSequence = rcs[1].Sequence
		sess.IntroducedAt = now
		serveIdx = 1
	}

	pl := m3u8.Playlist{
		Cache:    boolPtr(true),
		Sequence: sess.LatestSequence,
		Version:  intPtr(4), // TODO - when would it not be?
		Target:   maxDuration(rcs),
		Live:     true,
	}

	var (
		sqs  []int
		cids []string
	)

	for i := serveIdx; i < serveChunks+serveIdx; i++ {
		s := rcs[i]
		pl.AppendItem(&m3u8.SegmentItem{
			Segment:  "/segment/" + sess.StreamID + "/" + s.ChunkID,
			Duration: s.Duration,
		})
		sqs = append(sqs, s.Sequence)
		cids = append(cids, s.ChunkID)
	}

	p.l.Debugf("serving playlist from sequence %d with sequences %v chunks %v", sess.LatestSequence, sqs, cids)

	w.Header().Set("content-type", "application/x-mpegURL")

	fmt.Fprint(w, pl.String())
}

func boolPtr(b bool) *bool {
	return &b
}

func intPtr(i int) *int {
	return &i
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
