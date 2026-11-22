package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

// streamBuffer amount of content we serve ahead to the consumer, to give them a
// bit of a buffer for network issues or w/e. this is the minimum we always want
// the user to be ahead
const streamBuffer = 30 * time.Second

// icyServer serves a given station over icecast
type icyServer struct {
	l logrus.FieldLogger

	streams []configStream

	indexer *chunkIndex
	store   *s3ChunkStore
	plays   *playIndex
}

func newIcyServer(l logrus.FieldLogger, s []configStream, i *chunkIndex, st *s3ChunkStore, plays *playIndex) *icyServer {
	return &icyServer{
		l:       l,
		indexer: i,
		streams: s,
		store:   st,
		plays:   plays,
	}
}

func (i *icyServer) ServeIcecast(w http.ResponseWriter, r *http.Request) {
	i.l.Debugf("serving request for %s", r.URL.String())

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

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
	var nowPlaying *configNowPlaying
	for _, s := range i.streams {
		if s.ID == streamID {
			baseTZ = s.BaseTimezone
			stationName = s.Name
			nowPlaying = s.NowPlaying
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

	s, err := i.indexer.SequenceFor(ctx, streamID, now.Add(-max(offset, streamBuffer)))
	if err != nil {
		serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
		l.WithError(err).Errorf("getting sequence for %s", streamID)
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}

	// now we want to get a sequence, stream it's contents, and sleep.

	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Content-Type", "audio/aacp")
	w.Header().Set("icy-name", stationName)
	w.Header().Set("Cache-Control", "no-store")
	// Clients that want in-band titles (Sonos, VLC, etc.) ask with this header.
	var meta *icyMetaWriter
	if r.Header.Get("Icy-MetaData") == "1" {
		w.Header().Set("icy-metaint", strconv.Itoa(icyMetaInt))
		meta = newIcyMetaWriter(w)
	}
	if r.Method == http.MethodHead {
		return
	}

	// Headers are committed only after the first chunk has been prepared.

	// track when we start the streaming, and how much time we've streamed to
	// the user
	streamStart := time.Now()
	servedTime := time.Duration(0)

	nextRun := time.NewTimer(0)
	defer nextRun.Stop()

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
				if servedTime == 0 {
					http.Error(w, "Unable to find audio chunk", http.StatusInternalServerError)
				}
				return
			}
			if len(rcs) < 1 {
				l.Warnf("got no chunks")
				if servedTime == 0 {
					http.Error(w, "No audio chunks available", http.StatusServiceUnavailable)
					return
				}
				nextRun.Reset(time.Second)
				continue
			}
			c := rcs[0]

			l = l.WithField("seq", c.Sequence).WithField("cid", c.ChunkID)

			// Prepare a complete segment before committing a successful audio response.
			var body bytes.Buffer
			rawAAC, err := i.streamChunkBody(ctx, &body, l, streamID, c)
			if err != nil {
				if servedTime == 0 {
					http.Error(w, "Unable to read audio chunk", http.StatusBadGateway)
				}
				return
			}
			if meta != nil {
				err = meta.WriteAudio(body.Bytes(), i.titleAt(streamID, stationName, nowPlaying, c))
			} else {
				_, err = body.WriteTo(w)
			}
			if err != nil {
				l.WithError(err).Debug("writing audio to consumer")
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
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

			l.Debugf("s: %d rawAAC: %t gotSeq %d streamStart %s servedTime %s calcSleep %s", s, rawAAC, c.Sequence, streamStart.String(), servedTime.String(), calculateIcySleep(streamStart, servedTime).String())

			// set the timer to the calculated sleep interval
			nextRun.Reset(calculateIcySleep(streamStart, servedTime))
		}
	}
}

func (i *icyServer) streamChunkBody(ctx context.Context, w io.Writer, l logrus.FieldLogger, streamID string, c recordedChunk) (rawAAC bool, err error) {
	defer func() {
		if err != nil {
			serveEndpointErrorCount.WithLabelValues("icy", streamID).Inc()
			l.WithError(err).Error("reading audio chunk")
		}
	}()
	cr, err := i.store.GetObjectReader(ctx, c)
	if err != nil {
		return false, err
	}
	defer cr.Close()

	body, err := readSegment(cr)
	if err != nil {
		return false, err
	}
	audio, info, err := extractAAC(body)
	if err != nil {
		return false, err
	}
	l.WithField("format", info.format).WithField("audio_pid", info.pid).Debug("detected audio format")
	n, err := w.Write(audio)
	if err == nil && n != len(audio) {
		err = io.ErrShortWrite
	}
	return info.format == "adts", err
}

// titleAt returns the ICY title for a position within chunk c: the track that
// was on air when that audio was broadcast, or the station name between tracks.
func (i *icyServer) titleAt(streamID, stationName string, np *configNowPlaying, c recordedChunk) func(float64) string {
	return func(frac float64) string {
		if np == nil || i.plays == nil {
			return stationName
		}
		at := c.FetchedAt.Add(-np.Lag).Add(time.Duration(frac * c.Duration * float64(time.Second)))
		if p, ok := i.plays.At(streamID, at); ok {
			return p.String()
		}
		return stationName
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

	if servedDelta <= 0 {
		// closest thing to immediate
		return 1 * time.Nanosecond
	}
	return servedDelta
}
