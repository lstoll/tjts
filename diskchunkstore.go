package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

var _ chunkStore = (*diskChunkStore)(nil)

type chunkRecorder interface {
	// RecordChunk is called to note that the given chunk was persisted from the given time
	RecordChunk(ctx context.Context, streamID, chunkID string, timestamp time.Time) error
}

// diskChunkStore manages persisting and serving chunks from disk
type diskChunkStore struct {
	basePath string
	streamID string

	cr chunkRecorder

	hh http.Handler
}

func newDiskChunkStore(cr chunkRecorder, basePath, streamID string) (*diskChunkStore, error) {
	od := filepath.Join(basePath, streamID)
	if err := os.MkdirAll(od, 0755); err != nil {
		return nil, fmt.Errorf("creating %s: %v", od, err)
	}

	return &diskChunkStore{
		basePath: basePath,
		streamID: streamID,
		cr:       cr,
		hh:       http.FileServer(http.Dir(od)),
	}, nil
}

func (d *diskChunkStore) WriteChunk(ctx context.Context, chunkName string, r io.Reader) error {
	cfn := d.filename(chunkName)
	f, err := os.Create(cfn)
	if err != nil {
		return fmt.Errorf("creating %s: %v", cfn, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("copying data to %s: %v", cfn, err)
	}
	if err := d.cr.RecordChunk(ctx, d.streamID, cfn, time.Now()); err != nil {
		return fmt.Errorf("recording chunk: %v", err)
	}
	return nil
}

func (d *diskChunkStore) ChunkExists(ctx context.Context, chunkName string) (bool, error) {
	cfn := d.filename(chunkName)
	if _, err := os.Stat(cfn); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat on %s: %v", cfn, err)
	}
	return true, nil
}

func (d *diskChunkStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO - cache headers etc?
	d.hh.ServeHTTP(w, r)
}

func (d *diskChunkStore) filename(chunkName string) string {
	return filepath.Join(d.basePath, d.streamID, chunkName)
}
