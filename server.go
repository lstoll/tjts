package iceshift

import (
	"log"
	"net/http"
)

type Server struct {
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) AddEndpoint(mount string, data chan ([]byte)) {
	http.HandleFunc("/"+mount, s.newHandler(data))
}

func (s *Server) ListenAndServe(listen string) {
	log.Printf("Starting server at %s", listen)
	http.ListenAndServe(listen, nil)
}

func (i *Server) newHandler(data chan []byte) func(http.ResponseWriter, *http.Request) {
	// Guts of the icecast stuff. Listen on mount, send data from shifter down.
	return func(w http.ResponseWriter, r *http.Request) {
		/*offsetStr := r.URL.Query().Get("offset")
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
		}*/
		w.Header().Set("Content-Type", "audio/aacp")
		w.Header().Set("icy-name", "Triple J")
		for {
			select {
			case d := <-data:
				_, err := w.Write(d)
				if err != nil {
					return
				}
			}
		}
	}
}
