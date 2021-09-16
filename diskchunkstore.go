package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// diskChunkStore manages persisting and serving chunks from disk
type diskChunkStore struct {
	basePath  string
	urlPrefix string
	cr        *recorder
}

func newDiskChunkStore(cr *recorder, basePath, urlPrefix string) (*diskChunkStore, error) {
	return &diskChunkStore{
		basePath:  basePath,
		urlPrefix: urlPrefix,
		cr:        cr,
	}, nil
}

// FetcherStore returns an individual storage for a given fetcher
func (d *diskChunkStore) FetcherStore(streamID string) (*stationChunkStore, error) {
	od := filepath.Join(d.basePath, streamID)
	if err := os.MkdirAll(od, 0755); err != nil {
		return nil, fmt.Errorf("creating %s: %v", od, err)
	}
	return &stationChunkStore{
		path: od,
		cr:   d.cr,
	}, nil
}

func (d *diskChunkStore) URLFor(streamID, chunkID string) string {
	return d.urlPrefix + "/" + streamID + "/" + chunkID
}

func (d *diskChunkStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sp := strings.Split(r.URL.Path, "/")
	if len(sp) != 2 {
		http.Error(w, "Not Found", http.StatusNotFound)
	}
	// TODO - think about exploit opportunities here.
	http.ServeFile(w, r, filepath.Join(d.basePath, sp[0], sp[1]))
}

type stationChunkStore struct {
	path     string
	streamID string

	cr *recorder
}

func (s *stationChunkStore) WriteChunk(ctx context.Context, chunkName string, chunkDuration float64, r io.Reader) error {
	cfn := s.filename(chunkName)
	f, err := os.Create(cfn)
	if err != nil {
		return fmt.Errorf("creating %s: %v", cfn, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("copying data to %s: %v", cfn, err)
	}
	if err := s.cr.RecordChunk(ctx, s.streamID, chunkName, chunkDuration, time.Now()); err != nil {
		return fmt.Errorf("recording chunk: %v", err)
	}
	return nil
}

func (d *stationChunkStore) ChunkExists(ctx context.Context, chunkName string) (bool, error) {
	cfn := d.filename(chunkName)
	if _, err := os.Stat(cfn); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat on %s: %v", cfn, err)
	}
	return true, nil
}

func (s *stationChunkStore) filename(chunkName string) string {
	return filepath.Join(s.path, chunkName)
}
