package main

import (
	"context"
	"flag"
	"log"
	"net/http"
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

	mux := http.NewServeMux()

	srv := &http.Server{
		Addr:    *listen,
		Handler: mux,
	}

	var g run.Group

	g.Add(run.SignalHandler(ctx, os.Interrupt))

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
