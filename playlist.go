package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/etherlabsio/go-m3u8/m3u8"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type chunkIndex interface {
	// ChunksBefore should return the most recent num chunks before the given
	// time. These should be in playable order, i.e ascending in time. If no
	// chunks are before the given time, we should return the oldest existing
	ChunksBefore(ctx context.Context, streamID string, before time.Time, num int) ([]recordedChunk, error)
}

type sessStore interface {
	Get(ctx context.Context, sid string) (sessionData, error)
	Set(ctx context.Context, sid string, d sessionData) error
}

type urlMapper interface {
	URLFor(streamID, chunkID string) string
}

// playlist generates time shifted hls playlists from content
type playlist struct {
	l logrus.FieldLogger

	indexer chunkIndex
	mapper  urlMapper
	streams []configStream
	sess    sessStore
}

func newPlaylist(l logrus.FieldLogger, s []configStream, i chunkIndex, u urlMapper, ss sessStore) (*playlist, error) {
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
