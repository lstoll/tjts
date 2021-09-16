package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/etherlabsio/go-m3u8/m3u8"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// playlist generates time shifted hls playlists from content
type playlist struct {
	l logrus.FieldLogger

	indexer *recorder
	mapper  *diskChunkStore
	streams []configStream
	sess    *sessionStore
}

func newPlaylist(l logrus.FieldLogger, s []configStream, i *recorder, u *diskChunkStore, ss *sessionStore) (*playlist, error) {
	return &playlist{
		l:       l,
		indexer: i,
		streams: s,
		mapper:  u,
		sess:    ss,
	}, nil
}

// ServePlaylist handles a request to build a m38u playlist for a station
func (p *playlist) ServePlaylist(w http.ResponseWriter, r *http.Request) {
	p.l.Debugf("serving request for %s", r.URL.String())

	stationID := r.URL.Query().Get("station")
	tzStr := r.URL.Query().Get("tz")
	now := time.Now()

	var baseTZ string
	for _, s := range p.streams {
		if s.ID == stationID {
			baseTZ = s.BaseTimezone
		}
	}
	if baseTZ == "" {
		http.Error(w, fmt.Sprintf("Station %s not found", stationID), http.StatusNotFound)
		return
	}

	sid := r.URL.Query().Get("sid")
	if sid == "" {
		q := r.URL.Query()
		q.Add("sid", uuid.New().String())
		u := *r.URL
		u.RawQuery = q.Encode()
		http.Redirect(w, r, u.String(), http.StatusSeeOther)
		return
	}

	sess, err := p.sess.Get(r.Context(), sid)
	if err != nil {
		p.l.WithError(err).Errorf("getting session %s", sid)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	// if the stream has no latest sequence, we want to find one. This will be
	// either the sequence that corresponds to our offset time, or if we have no
	// sequence older than that it'll be the oldest sequence.
	//
	// once we have a sequence, we want to save in the session the sequence ID
	// and when it was seen. On subsequent requests, we grab that sequence plus
	// a couple newer items than it. If the latest sequence introduced at + it's
	// duration is before now, drop it off, add some new sequences, and update
	// the session.

	if sess.Offset == 0 {
		// We have no current offset, so calculate the difference between the
		// same contrived time in the different timezones to figure their
		// offset.
		tz, err := time.LoadLocation(tzStr)
		if err != nil {
			p.l.WithError(err).Debugf("looking up user timezone %s", tzStr)
			http.Error(w, fmt.Sprintf("Couldn't find timezone %s", tzStr), http.StatusBadRequest)
			return
		}

		t := time.Date(1981, 12, 6, 01, 00, 00, 00, tz)

		btz, err := time.LoadLocation(baseTZ)
		if err != nil {
			p.l.WithError(err).Errorf("looking up base timezone %s", baseTZ)
			http.Error(w, fmt.Sprintf("Couldn't find timezone %s", baseTZ), http.StatusBadRequest)
			return
		}

		bt := time.Date(1981, 12, 6, 01, 00, 00, 00, btz)

		sess.Offset = t.Sub(bt)
	}

	// get the segments to serve up
	sg, err := p.indexer.ChunksBefore(r.Context(), stationID, now.Add(-sess.Offset), 3)
	if err != nil {
		p.l.WithError(err).Error("getting chunks")
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	if len(sg) < 1 {
		p.l.Errorf("no chunks for %s", stationID)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	// use the actual returned chunk time to update the offset. Do this always,
	// so even if we have holes in the stream we should hopefully re-calculat
	// around it.
	// TODO - validate this assumption, i've had some 🍷
	sess.Offset = now.Sub(sg[0].FetchedAt)
	if err := p.sess.Set(r.Context(), sid, sess); err != nil {
		p.l.WithError(err).Error("updating session")
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	pl := m3u8.Playlist{
		Cache:    boolPtr(true),
		Sequence: sg[0].Sequence,
		Version:  intPtr(4), // TODO - when would it not be?
		Target:   maxDuration(sg),
		Live:     true,
	}

	var (
		sqs  []int
		cids []string
	)

	for _, s := range sg {
		pl.AppendItem(&m3u8.SegmentItem{
			Segment:  "/segment/" + stationID + "/" + s.ChunkID,
			Duration: s.Duration,
		})
		sqs = append(sqs, s.Sequence)
		cids = append(cids, s.ChunkID)
	}

	p.l.Debugf("serving playlist from offset %s with sequences %v chunks %v", sess.Offset.String(), sqs, cids)

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
