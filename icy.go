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

// streamBuffer amount of content we serve ahead to the consumer, to give them a
// bit of a buffer for network issues or w/e. this is the minumum we always want
// the user to be ahead
const streamBuffer = 30 * time.Second

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
		serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
		l.WithError(err).Errorf("getting sequence for %s", streamID)
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}

	// now we want to get a sequence, stream it's contents, and sleep.

	w.Header().Set("Content-Type", "audio/aacp")
	w.Header().Set("icy-name", stationName)

	// note - from this point on http.Error is useless, we've already served headers and stuff

	// track when we start the streaming, and how much time we've streamed to
	// the user
	streamStart := time.Now()
	servedTime := time.Duration(0)

	nextRun := time.NewTimer(0)

	for {
		select {
		case <-ctx.Done():
			l.Debug("context done")
			return
		case <-nextRun.C:
			rcs, err := i.indexer.Chunks(ctx, streamID, s, 1)
			if err != nil {
				serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
				l.WithError(err).Errorf("getting 1 chunk from %d", s)
				return
			}
			if len(rcs) < 1 {
				l.Warnf("got no chunks")
				return
			}
			c := rcs[0]

			l = l.WithField("seq", c.Sequence).WithField("cid", c.ChunkID)

			cr, err := i.mapper.ReaderFor(streamID, c.ChunkID)
			if err != nil {
				serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
				l.WithError(err).Error("getting chunk reader")
				return
			}

			// refs:
			// * https://tsduck.io/download/docs/mpegts-introduction.pdf

			// TODO - this is 100% filled with hardcoded assumptions about the
			// stream. We need to determining the right thing to demux by using
			// the stream map, and also do some checking on the PES header data
			// to make sure we're extracting the right shit

			pat, err := psi.ReadPAT(cr)
			if err != nil {
				serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
				l.WithError(err).Error("getting pat")
				return
			}
			_ = pat
			// l.Debugf("pat %#v", pat)

			const audioPid = 256

			var pkt packet.Packet
			for read, err := cr.Read(pkt[:]); read > 0 && err == nil; read, err = cr.Read(pkt[:]) {
				if err != nil {
					serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
					l.WithError(err).Error("reading packet")
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
						serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
						l.WithError(err).Errorf("getting packet header")
						return
					}
					pes, err := pes.NewPESHeader(ph)
					if err != nil {
						serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
						l.WithError(err).Errorf("creating pes header")
						return
					}
					if _, err := w.Write(pes.Data()); err != nil {
						serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
						l.WithError(err).Error("writing packet")
						return
					}
				} else {
					// otherwise assume a continuation from the last pes header,
					// so just stream it
					pl, err := pkt.Payload()
					if err != nil {
						serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
						l.WithError(err).Error("getting packet payload")
						return
					}

					if _, err := w.Write(pl); err != nil {
						serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
						l.WithError(err).Error("writing packet")
						return
					}
				}
			}

			// We want to make sure the user has been served enough data for the
			// time elapsed since the start of the stream, plus streamBuffer.

			// increment the served time with the chunk we just sent, and
			// increment the sequence to represent a chunk seved.
			//
			// TODO - might want to consider calculating the actual data found
			// in the parsed ts segment, rather than relying on what we pulled
			// from the playlist. This would be more accurate.
			cd := time.Duration(c.Duration * float64(time.Second))
			servedTime = servedTime + cd
			s = c.Sequence + 1

			l.Debugf("s: %d gotSeq %d streamStart %s servedTime %s calcSleep %s", s, c.Sequence, streamStart.String(), servedTime.String(), calculateIcySleep(streamStart, servedTime).String())

			// set the timer to the calculated sleep interval
			nextRun.Reset(calculateIcySleep(streamStart, servedTime))
		}
	}
}

var nowFn = time.Now

// calculateIcySleep takes the time a stream started and how much has been
// served, and works out what the next timer should be
func calculateIcySleep(streamStart time.Time, servedTime time.Duration) time.Duration {
	// time.Since is basically now.Sub

	// calculate how much time we should have served to the user. This will
	// be the time elapsed since the stream started, plus the buffer
	expectedServed := nowFn().Sub(streamStart) + streamBuffer

	// the difference between how much we have served, vs. how much we should serve
	servedDelta := servedTime - expectedServed

	if servedDelta < 0 {
		// can't negative sleep
		return 0
	}
	return servedDelta
}
