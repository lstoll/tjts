package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	// avoid having to bundle this in the docker image
	_ "time/tzdata"

	"github.com/oklog/run"
	"github.com/sirupsen/logrus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := logrus.New()

	var (
		listen     = flag.String("listen", "localhost:8080", "Address to listen on")
		configPath = flag.String("config", "", "path to config file")
		debug      = flag.Bool("debug", false, "enable debug logging")
	)
	flag.Parse()

	if *listen == "" {
		l.Fatal("-listen must be provided")
	}
	if *configPath == "" {
		l.Fatal("-config must be provided")
	}

	if *debug {
		l.Level = logrus.DebugLevel
	} else {
		l.Level = logrus.InfoLevel
	}

	cfg, err := loadAndValdiateConfig(*configPath)
	if err != nil {
		l.Fatal(err)
	}

	// ensure db exists/make it, to avoid those out of memory errors
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0755); err != nil {
		l.Fatalf("ensuring %s exists: %v", filepath.Dir(cfg.DBPath), err)
	}

	rec, err := newRecorder(cfg.DBPath)
	if err != nil {
		l.Fatal(err)
	}

	ds, err := newDiskChunkStore(rec, cfg.ChunkDir, "/segment")
	if err != nil {
		l.WithError(err).Fatal("creating chunk store")
	}

	ss, err := newSessionStore()
	if err != nil {
		l.WithError(err).Fatal("creating session store")
	}

	pl, err := newPlaylist(l.WithField("component", "playlist"), cfg.Streams, rec, ds, ss)
	if err != nil {
		l.WithError(err).Fatal("creating playlist")
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/m3u8", pl.ServePlaylist)
	mux.Handle("/segment/", http.StripPrefix("/segment/", ds))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "greetings") })

	srv := &http.Server{
		Addr:    *listen,
		Handler: mux,
	}

	var g run.Group

	g.Add(run.SignalHandler(ctx, os.Interrupt))

	for _, s := range cfg.Streams {

		fcs, err := ds.FetcherStore(s.ID)
		if err != nil {
			l.WithError(err).Fatalf("creating fetcher store for %s", s.ID)
		}

		f, err := newFetcher(l.WithField("component", "fetcher"), fcs, s.URL)
		if err != nil {
			l.WithError(err).Fatal("creating fetcher")
		}

		g.Add(f.Run, f.Interrupt)
	}

	g.Add(func() error {
		l.Infof("listening on %s", *listen)
		return srv.ListenAndServe()
	}, func(error) {
		// use a new context, as upstream will be canceled by now
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})

	if err := g.Run(); err != nil {
		l.Fatal(err)
	}
}
