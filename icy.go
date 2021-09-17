package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/Comcast/gots/packet"
	"github.com/sirupsen/logrus"
)

const initialIcyServe = 30 * time.Second

// icyServer serves a given station over icecast
type icyServer struct {
	l logrus.FieldLogger

	streams []configStream

	indexer *recorder
	mapper  *diskChunkStore
}

func newIcyServer(l logrus.FieldLogger, s []configStream, i *recorder, u *diskChunkStore) *icyServer {
	return &icyServer{
		l:       l,
		indexer: i,
		streams: s,
		mapper:  u,
	}
}

func (i *icyServer) ServeIcecast(w http.ResponseWriter, r *http.Request) {
	i.l.Debugf("serving request for %s", r.URL.String())

	ctx := r.Context()
	now := time.Now()

	streamID := r.URL.Query().Get("stream")
	tzStr := r.URL.Query().Get("tz")

	if streamID == "" || tzStr == "" {
		http.Error(w, "stream and tz must be present on query", http.StatusBadRequest)
		return
	}

	var baseTZ string
	var stationName string
	for _, s := range i.streams {
		if s.ID == streamID {
			baseTZ = s.BaseTimezone
			stationName = s.Name
		}
	}
	if baseTZ == "" {
		http.Error(w, fmt.Sprintf("Stream %s not found", streamID), http.StatusNotFound)
		return
	}

	offset, err := offsetForTimezone(baseTZ, tzStr)
	if err != nil {
		i.l.WithError(err).Debugf("finding offset")
		http.Error(w, fmt.Sprintf("Error calculating offset: %s", err.Error()), http.StatusBadRequest)
		return
	}

	i.l.Debugf("offset %s", offset.String())

	s, err := i.indexer.SequenceFor(ctx, streamID, now.Add(-offset))
	if err != nil {
		i.l.WithError(err).Errorf("getting sequence for %s", streamID)
		http.Error(w, "Internal Error", http.StatusBadRequest)
		return
	}

	// now we want to get a sequence, stream it's contents, and sleep.

	w.Header().Set("Content-Type", "audio/aacp")
	w.Header().Set("icy-name", stationName)

	// track how much we've served to the user, so we can fast start the station
	// with the first few chunks
	var servedTime time.Duration

	nextRun := time.NewTimer(0)

	for {
		select {
		case <-ctx.Done():
			i.l.Debug("context done")
			return
		case <-nextRun.C:
			st := time.Now()

			rcs, err := i.indexer.Chunks(ctx, streamID, s, 1)
			if err != nil {
				i.l.WithError(err).Errorf("getting 1 chunk from %d", s)
				http.Error(w, "Internal Error", http.StatusBadRequest)
				return
			}
			if len(rcs) < 1 {
				i.l.Warnf("got no chunks for %s", streamID)
				http.Error(w, "Internal Error", http.StatusBadRequest)
				return
			}
			c := rcs[0]

			cr, err := i.mapper.ReaderFor(streamID, c.ChunkID)
			if err != nil {
				i.l.WithError(err).Errorf("getting %s chunk reader", streamID)
				http.Error(w, "Internal Error", http.StatusBadRequest)
				return
			}

			i.l.Debugf("s: %d gotSeq %d servedTime %s", s, c.Sequence, servedTime.String())

			var pkt packet.Packet
			for read, err := cr.Read(pkt[:]); read > 0 && err == nil; read, err = cr.Read(pkt[:]) {
				if err != nil {
					i.l.WithError(err).Error("reading packet")
					http.Error(w, "Internal Error", http.StatusBadRequest)
					return
				}
				// i.l.Debugf("got packet pid %d", packet.Pid(&pkt))
				p, err := packet.Payload(&pkt)
				if err != nil {
					i.l.WithError(err).Errorf("reading packet %d payload", packet.Pid(&pkt))
					http.Error(w, "Internal Error", http.StatusBadRequest)
					return
				}
				if _, err := w.Write(p); err != nil {
					i.l.WithError(err).Error("writing packet")
					http.Error(w, "Internal Error", http.StatusBadRequest)
					return
				}
			}

			cd := time.Duration(c.Duration * float64(time.Second))

			var sleepTime time.Duration

			if servedTime > initialIcyServe {
				// we've served our initial buffer, sleep for the chunk until it's time for the next one.
				// deduct 1ms from the serve time to kinda account for the processing time above. this is
				// pretty inaccurate, but good enough here
				//
				// processing time + how long the chunk will go for less 10 ms for "overhead"
				sleepTime = time.Since(st) + cd - 10*time.Millisecond
			} else {
				sleepTime = 0
			}

			nextRun.Reset(sleepTime)

			// increment sequence + serve time
			s = c.Sequence + 1
			servedTime = servedTime + cd
		}
	}
}
