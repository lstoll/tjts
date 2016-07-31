package tjts

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

type Server struct {
}

func NewServer() *Server {
	http.HandleFunc("/", newStaticHandler(IndexHTML))
	http.HandleFunc("/moment.js", newStaticHandler(MomentJS))
	http.HandleFunc("/moment-timezone-with-data-2010-2020.js", newStaticHandler(MomentJSWTZWithData))
	return &Server{}
}

func (s *Server) AddEndpoint(mount string, sh Shifter) {
	http.HandleFunc("/"+mount, s.newHandler(sh))
}

func (s *Server) ListenAndServe(listen string) {
	log.Printf("Starting server at %s", listen)
	http.ListenAndServe(listen, nil)
}

func (i *Server) newHandler(sh Shifter) func(http.ResponseWriter, *http.Request) {
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
		data, closer := sh.StreamFrom(offset)
		w.Header().Set("Content-Type", "audio/aacp")
		w.Header().Set("icy-name", "Triple J")
		for {
			select {
			case d, ok := <-data:
				if !ok {
					log.Printf("Data channel closed for client, ending")
					return
				}
				_, err := w.Write(d)
				if err != nil {
					closer <- struct{}{}
					return
				}
			}
		}
	}
}

func newStaticHandler(content string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, content)
	}
}
