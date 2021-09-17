package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/Comcast/gots/packet"
	"github.com/Comcast/gots/pes"
	"github.com/Comcast/gots/psi"
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
	l := i.l
	now := time.Now()

	streamID := r.URL.Query().Get("stream")
	tzStr := r.URL.Query().Get("tz")

	if streamID == "" || tzStr == "" {
		http.Error(w, "stream and tz must be present on query", http.StatusBadRequest)
		return
	}

	l = l.WithField("stream", streamID).WithField("tz", tzStr)

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
		l.WithError(err).Debugf("finding offset")
		http.Error(w, fmt.Sprintf("Error calculating offset: %s", err.Error()), http.StatusBadRequest)
		return
	}

	l.Debugf("offset %s", offset.String())

	s, err := i.indexer.SequenceFor(ctx, streamID, now.Add(-offset))
	if err != nil {
		l.WithError(err).Errorf("getting sequence for %s", streamID)
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
			l.Debug("context done")
			return
		case <-nextRun.C:
			st := time.Now()

			rcs, err := i.indexer.Chunks(ctx, streamID, s, 1)
			if err != nil {
				l.WithError(err).Errorf("getting 1 chunk from %d", s)
				http.Error(w, "Internal Error", http.StatusBadRequest)
				return
			}
			if len(rcs) < 1 {
				l.Warnf("got no chunks")
				http.Error(w, "Internal Error", http.StatusBadRequest)
				return
			}
			c := rcs[0]

			l = l.WithField("seq", c.Sequence).WithField("cid", c.ChunkID)

			cr, err := i.mapper.ReaderFor(streamID, c.ChunkID)
			if err != nil {
				l.WithError(err).Error("getting chunk reader")
				http.Error(w, "Internal Error", http.StatusBadRequest)
				return
			}

			l.Debugf("s: %d gotSeq %d servedTime %s", s, c.Sequence, servedTime.String())

			// refs:
			// * https://tsduck.io/download/docs/mpegts-introduction.pdf

			// TODO - this is 100% filled with hardcoded assumptions about the
			// stream. We need to determing the right thing to demux by using
			// the stream map, and also do some checking on the PES header data
			// to make sure we're extracting the right shit

			pat, err := psi.ReadPAT(cr)
			if err != nil {
				l.WithError(err).Error("getting pat")
				http.Error(w, "Internal Error", http.StatusBadRequest)
				return
			}
			_ = pat
			// l.Debugf("pat %#v", pat)

			const audioPid = 256

			var pkt packet.Packet
			for read, err := cr.Read(pkt[:]); read > 0 && err == nil; read, err = cr.Read(pkt[:]) {
				if err != nil {
					l.WithError(err).Error("reading packet")
					http.Error(w, "Internal Error", http.StatusBadRequest)
					return
				}
				if packet.Pid(&pkt) != audioPid {
					// skip non aac stream
					continue
				}
				if !pkt.HasPayload() {
					// skip, nothing to stream
					continue
				}

				// if we're here, we're running on the right packet ID stream

				if packet.PayloadUnitStartIndicator(&pkt) {
					// assume a PES header, extract it and return the data
					ph, err := packet.PESHeader(&pkt)
					if err != nil {
						l.WithError(err).Errorf("getting packet header")
						http.Error(w, "Internal Error", http.StatusBadRequest)
						return
					}
					pes, err := pes.NewPESHeader(ph)
					if err != nil {
						l.WithError(err).Errorf("creating pes header")
						http.Error(w, "Internal Error", http.StatusBadRequest)
						return
					}
					if _, err := w.Write(pes.Data()); err != nil {
						l.WithError(err).Error("writing packet")
						http.Error(w, "Internal Error", http.StatusBadRequest)
						return
					}
				} else {
					// otherwise assume a continuation from the last pes header,
					// so just stream it

					pl, err := pkt.Payload()
					if err != nil {
						l.WithError(err).Error("getting packet payload")
						http.Error(w, "Internal Error", http.StatusBadRequest)
						return
					}

					if _, err := w.Write(pl); err != nil {
						l.WithError(err).Error("writing packet")
						http.Error(w, "Internal Error", http.StatusBadRequest)
						return
					}
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
