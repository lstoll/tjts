package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	// avoid having to bundle this in the docker image
	_ "time/tzdata"

	"github.com/oklog/run"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		listen     = flag.String("listen", "localhost:5000", "Address to listen on")
		configPath = flag.String("config", "", "path to config file")
	)
	flag.Parse()

	if *listen == "" {
		log.Fatal("-listen must be provided")
	}
	if *configPath == "" {
		log.Fatal("-config must be provided")
	}

	cfg, err := loadAndValdiateConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	// TODO - ensure db exists/make it? avoid those out of memory errors

	rec, err := newRecorder(cfg.DBPath)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	srv := &http.Server{
		Addr:    *listen,
		Handler: mux,
	}

	var g run.Group

	g.Add(run.SignalHandler(ctx, os.Interrupt))

	for _, s := range cfg.Streams {
		hc := &http.Client{
			Timeout: time.Second * 5,
		}

		u, err := url.Parse(s.URL)
		if err != nil {
			log.Fatalf("parsing %s for %s: %v", s.URL, s.ID, err)
		}

		dcs, err := newDiskChunkStore(rec, cfg.ChunkDir, s.ID)
		if err != nil {
			log.Fatal(err)
		}

		f := &fetcher{
			hc:  hc,
			url: u,
			cs:  dcs,
		}

		g.Add(f.Run, f.Interrupt)
	}

	g.Add(func() error {
		return srv.ListenAndServe()
	}, func(error) {
		// use a new context, as upstream will be canceled by now
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})

	if err := g.Run(); err != nil {
		log.Fatal(err)
	}
}
