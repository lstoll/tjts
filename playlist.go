package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/etherlabsio/go-m3u8/m3u8"
	"github.com/sirupsen/logrus"
)

type chunkIndex interface {
	// ChunksBefore should return the most recent num chunks before the given
	// time. These should be in playable order, i.e ascending in time. If no
	// chunks are before the given time, we should return the oldest existing
	ChunksBefore(ctx context.Context, streamID string, before time.Time, num int) ([]recordedChunk, error)
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
}

func newPlaylist(l logrus.FieldLogger, s []configStream, i chunkIndex, u urlMapper) (*playlist, error) {
	return &playlist{
		l:       l,
		indexer: i,
		streams: s,
		mapper:  u,
	}, nil
}

// ServePlaylist handles a request to build a m38u playlist for a station
func (p *playlist) ServePlaylist(w http.ResponseWriter, r *http.Request) {
	p.l.Debugf("serving request for %s", r.URL.String())

	stationID := r.URL.Query().Get("station")
	tzStr := r.URL.Query().Get("tz")

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

	// calculate the difference between the same contrived time in the
	// different timezones to figure their offset.
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

	offset := t.Sub(bt)

	// get the segments to serve up
	sg, err := p.indexer.ChunksBefore(r.Context(), stationID, time.Now().Add(-offset), 3)
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

	pl := m3u8.Playlist{
		Cache:    boolPtr(true),
		Sequence: sg[0].Sequence,
		Version:  intPtr(3), // TODO - when would it not be?
		Target:   avgDuration(sg),
		Live:     true,
	}

	// TODO - build URL
	// TODO - persist duration of segment

	// TODO don't make assumptions about the URL - should be possible to query something for it.

	for _, s := range sg {
		pl.AppendItem(&m3u8.SegmentItem{
			Segment:  "/segment/" + stationID + "/" + s.ChunkID,
			Duration: s.Duration,
		})
	}

	w.Header().Set("content-type", "application/x-mpegURL")

	fmt.Fprint(w, pl.String())
}

func boolPtr(b bool) *bool {
	return &b
}

func intPtr(i int) *int {
	return &i
}

func avgDuration(sg []recordedChunk) int {
	var tot float64
	for _, s := range sg {
		tot = tot + s.Duration
	}
	return int(tot) / len(sg)
}
