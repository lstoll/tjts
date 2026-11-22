package main

import (
	_ "embed"
	"encoding/json"
	"html/template"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	//go:embed index.html.tmpl
	indexData string
	indexTmpl = template.Must(template.New("index.html").Parse(indexData))
)

type index struct {
	l logrus.FieldLogger

	streams []tmplStream
}

func newIndex(l logrus.FieldLogger, streams []configStream) *index {
	var ts []tmplStream
	for _, s := range streams {
		t := tmplStream{
			ID:     s.ID,
			HLSURL: "/m3u8?stream=" + s.ID,
			ICYURL: "/icecast?stream=" + s.ID,
			Name:   s.Name,
		}
		if s.NowPlaying != nil {
			t.NowPlayingURL = "/nowplaying?stream=" + s.ID
		}
		ts = append(ts, t)
	}

	return &index{l: l, streams: ts}
}

type tmplStream struct {
	ID            string
	HLSURL        string
	ICYURL        string
	NowPlayingURL string
	Name          string
}

func (i *index) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	data := map[string]any{
		"Streams": i.streams,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := indexTmpl.Execute(w, data); err != nil {
		i.l.WithError(err).Error("executing template")
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}
}

const nowPlayingRecentMax = 50

// nowPlayingServer serves what a listener in a given timezone is hearing, and
// what they heard before, on their shifted timeline.
type nowPlayingServer struct {
	streams   []configStream
	plays     *playIndex
	latency   *latencyTracker
	retention time.Duration
}

// heardPlay is a play with the time the listener heard it on their timeline.
type heardPlay struct {
	play
	HeardAt time.Time `json:"heardAt"`
}

type nowPlayingResponse struct {
	Station string      `json:"station"`
	Now     *heardPlay  `json:"now"`
	Recent  []heardPlay `json:"recent"`
}

func (n *nowPlayingServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	streamID := r.URL.Query().Get("stream")
	tz := r.URL.Query().Get("tz")
	if streamID == "" || tz == "" {
		http.Error(w, "stream and tz must be present on query", http.StatusBadRequest)
		return
	}
	var stream *configStream
	for i := range n.streams {
		if n.streams[i].ID == streamID {
			stream = &n.streams[i]
		}
	}
	if stream == nil || stream.NowPlaying == nil {
		http.Error(w, "Stream not found", http.StatusNotFound)
		return
	}
	now := nowFn()
	offset, err := offsetForTimezoneAt(stream.BaseTimezone, tz, now)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Both endpoints serve audio fetched at least streamBuffer ago.
	shift := max(offset, streamBuffer) + n.latency.Latency(streamID) + stream.NowPlaying.Lag
	// The broadcast moment the listener is hearing now.
	listening := now.Add(-shift)
	heard := func(p play) heardPlay { return heardPlay{play: p, HeardAt: p.PlayedAt.Add(shift)} }

	resp := nowPlayingResponse{Station: stream.Name, Recent: []heardPlay{}}
	if p, ok := n.plays.At(streamID, listening); ok {
		hp := heard(p)
		resp.Now = &hp
	}
	for _, p := range n.plays.Recent(streamID, now.Add(-n.retention), listening, nowPlayingRecentMax) {
		if resp.Now != nil && p.ID == resp.Now.ID {
			continue
		}
		resp.Recent = append(resp.Recent, heard(p))
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(w).Encode(resp)
}
