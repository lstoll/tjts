package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

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

	s3Client, err := newS3Client(ctx, cfg.S3)
	if err != nil {
		l.WithError(err).Fatal("s3 client")
	}
	if err := ensureS3Bucket(ctx, s3Client, cfg.S3.Bucket); err != nil {
		l.WithError(err).Fatal("s3 bucket")
	}

	idx := newChunkIndex()
	store := newS3ChunkStore(s3Client, cfg.S3.Bucket, cfg.S3.PresignTTL, idx)

	for _, s := range cfg.Streams {
		if err := store.LoadStream(ctx, s.ID); err != nil {
			l.WithError(err).Warnf("loading stream index for %s", s.ID)
		}
	}

	hlsSess := newHLSSessions()
	pl := newPlaylist(l.WithField("component", "playlist"), cfg.Streams, idx, store, hlsSess)
	is := newIcyServer(l.WithField("component", "icyServer"), cfg.Streams, idx, store)

	idxPage := newIndex(l.WithField("component", "index"), cfg.Streams)

	gc := newGarbageCollector(l.WithField("component", "gc"), idx, store, hlsSess)

	mux := http.NewServeMux()

	mux.HandleFunc("/m3u8", pl.ServePlaylist)
	mux.HandleFunc("/chunk", pl.ServeChunk)
	mux.HandleFunc("/icecast", is.ServeIcecast)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		idxPage.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Addr:    *listen,
		Handler: mux,
	}

	var g run.Group

	g.Add(run.SignalHandler(ctx, os.Interrupt))

	g.Add(gc.Run, gc.Interrupt)

	for _, s := range cfg.Streams {
		fcs := store.FetcherStore(s.ID)

		f, err := newFetcher(l.WithField("component", "fetcher").WithField("stationid", s.ID), fcs, s.ID, s.URL)
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

		prometheus.MustRegister(newMetricsCollector(idx))

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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	if err := g.Run(); err != nil {
		l.Fatal(err)
	}
}
