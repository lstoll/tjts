package tjts

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

const baseTZ = "Australia/Sydney"

type Server struct {
}

func NewServer() *Server {
	http.HandleFunc("/", newStaticHandler(IndexHTML))
	return &Server{}
}

func (s *Server) AddEndpoint(mount string, sh Shifter) {
	http.HandleFunc("/"+mount, newHandler(sh))
}

func (s *Server) ListenAndServe(listen string) {
	log.Printf("Starting server at %s", listen)
	if err := http.ListenAndServe(listen, nil); err != nil {
		log.Fatal(err)
	}
}

func newHandler(sh Shifter) func(http.ResponseWriter, *http.Request) {
	// Guts of the icecast stuff. Listen on mount, send data from shifter down.
	return func(w http.ResponseWriter, r *http.Request) {
		tzStr := r.URL.Query().Get("tz")

		// calculate the difference between the same contrived time in the
		// different timezones to figure their offset.
		tz, err := time.LoadLocation(tzStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Couldn't find timezone %s", tzStr), http.StatusBadRequest)
			return
		}

		t := time.Date(1981, 12, 6, 01, 00, 00, 00, tz)

		btz, err := time.LoadLocation(baseTZ)
		if err != nil {
			http.Error(w, fmt.Sprintf("Couldn't find timezone %s", baseTZ), http.StatusBadRequest)
			return
		}

		bt := time.Date(1981, 12, 6, 01, 00, 00, 00, btz)

		offset := t.Sub(bt)

		data, closer := sh.StreamFrom(offset)
		w.Header().Set("Content-Type", "audio/aacp")
		w.Header().Set("icy-name", "Triple J")
		for d := range data {
			_, err := w.Write(d)
			if err != nil {
				closer <- struct{}{}
				return

			}
		}
		log.Printf("Data channel closed for client, ending")
	}
}

func newStaticHandler(content string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, content)
	}
}
