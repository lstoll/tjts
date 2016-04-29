package iceshift

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	log "github.com/Sirupsen/logrus"
)

type IceServer struct {
	shifters []Shifter
}

func NewIceServer() *IceServer {
	return &IceServer{}
}

func (i *IceServer) AddEndpoint(upstream *url.URL, mountAt string, maxOffset time.Duration) {
	shifter := NewDiskShifter(upstream, maxOffset)
	i.shifters = append(i.shifters, shifter)
	http.HandleFunc("/"+mountAt, i.httpHandler(shifter))
}

func (i *IceServer) ListenAndServe(listen string) {
	log.WithFields(log.Fields{"fn": "ListenAndServe", "at": "listening", "address": listen}).Info("Server Started")
	http.ListenAndServe(listen, nil)
}

func (i *IceServer) httpHandler(shifter Shifter) func(http.ResponseWriter, *http.Request) {
	// Guts of the icecast stuff. Listen on mount, send data from shifter down.
	return func(w http.ResponseWriter, r *http.Request) {
		offsetStr := r.URL.Query().Get("offset")
		var offset time.Duration
		if offsetStr == "" {
			offset = 0 * time.Second
		} else {
			parsed, err := time.ParseDuration(offsetStr)
			if err != nil {
				// handle error
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "Error parsing offset: %s", err)
				return
			}
			offset = parsed
		}
		data, errs := shifter.StreamFrom(offset)
		w.Header().Set("Content-Type", "audio/aacp")
		w.Header().Set("icy-name", "Triple J")
		for {
			select {
			case d := <-data:
				_, err := w.Write(d)
				if err != nil {
					return
				}
			case <-errs:
				return
			}
		}
	}
}
