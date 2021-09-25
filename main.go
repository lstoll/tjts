package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	// avoid having to bundle this in the docker image
	_ "time/tzdata"

	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := logrus.New()

	var (
		listen        = flag.String("listen", "localhost:8080", "Address to listen on")
		metricsListen = flag.String("metrics-listen", "localhost:8090", "Address to serve metrics on (prom at /metrics), if set")
		configPath    = flag.String("config", "", "path to config file")
		debug         = flag.Bool("debug", false, "enable debug logging")
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

	db, err := newDB(cfg.DBPath)
	if err != nil {
		l.WithError(err).Fatalf("opening database at %s", cfg.DBPath)
	}
	defer db.Close()

	rec := newRecorder(db)

	ds := newDiskChunkStore(rec, cfg.ChunkDir, "/segment")

	ss := newSessionStore(db)

	pl := newPlaylist(l.WithField("component", "playlist"), cfg.Streams, rec, ds, ss)
	is := newIcyServer(l.WithField("component", "icyServer"), cfg.Streams, rec, ds)

	idx := newIndex(l.WithField("component", "index"), cfg.Streams)

	gc := newGarbageCollector(l.WithField("component", "gc"), db, ds)

	mux := http.NewServeMux()

	mux.HandleFunc("/m3u8", pl.ServePlaylist)
	mux.HandleFunc("/icecast", is.ServeIcecast)
	mux.Handle("/segment/", http.StripPrefix("/segment/", ds))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		idx.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Addr:    *listen,
		Handler: mux,
	}

	var g run.Group

	g.Add(run.SignalHandler(ctx, os.Interrupt))

	g.Add(gc.Run, gc.Interrupt)

	for _, s := range cfg.Streams {

		fcs, err := ds.FetcherStore(s.ID)
		if err != nil {
			l.WithError(err).Fatalf("creating fetcher store for %s", s.ID)
		}

		f, err := newFetcher(l.WithField("component", "fetcher").WithField("stationid", s.ID), fcs, s.URL)
		if err != nil {
			l.WithError(err).Fatal("creating fetcher")
		}

		g.Add(f.Run, f.Interrupt)
	}

	if *metricsListen != "" {
		ph := promhttp.InstrumentMetricHandler(
			prometheus.DefaultRegisterer, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
				ErrorLog: l,
			}),
		)

		pm := http.NewServeMux()
		pm.Handle("/metrics", ph)

		metricsSrv := &http.Server{
			Addr:    *metricsListen,
			Handler: pm,
		}

		prometheus.MustRegister(newMetricsCollector(db))

		g.Add(func() error {
			l.Printf("Listing for metrics on %s", *metricsListen)
			if err := metricsSrv.ListenAndServe(); err != nil {
				return fmt.Errorf("serving metrics: %v", err)
			}
			return nil
		}, func(error) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := metricsSrv.Shutdown(ctx); err != nil {
				log.Printf("shutting down metrics server: %v", err)
			}
			log.Print("returning metrics shutdown")
		})
	}

	g.Add(func() error {
		l.Infof("listening on %s", *listen)
		return srv.ListenAndServe()
	}, func(error) {
		// use a new context, as upstream will be canceled by now
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	if err := g.Run(); err != nil {
		l.Fatal(err)
	}
}
