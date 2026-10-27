package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	gcInterval  = 1 * time.Hour
	sessMaxAge  = 12 * time.Hour
	chunkMaxAge = 24 * time.Hour

	expiredChunksMax = 1000
)

type objectDeleter interface {
	DeleteObject(ctx context.Context, objectKey string) error
}

type garbageCollector struct {
	l logrus.FieldLogger

	idx     *chunkIndex
	obj     objectDeleter
	hlsSess *hlsSessions

	ctx    context.Context
	cancel context.CancelFunc
	maxAge time.Duration
}

func newGarbageCollector(l logrus.FieldLogger, idx *chunkIndex, obj objectDeleter, hlsSess *hlsSessions) *garbageCollector {
	ctx, cancel := context.WithCancel(context.Background())
	return &garbageCollector{
		l:       l,
		idx:     idx,
		obj:     obj,
		hlsSess: hlsSess,
		ctx:     ctx, cancel: cancel, maxAge: chunkMaxAge,
	}
}

func (g *garbageCollector) Run() error {
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()
	for {
		if err := g.collect(); err != nil && g.ctx.Err() == nil {
			g.l.WithError(err).Error("garbage collection failed; will retry")
		}
		select {
		case <-ticker.C:
		case <-g.ctx.Done():
			return nil
		}
	}
}

func (g *garbageCollector) Interrupt(_ error) { g.cancel() }

func (g *garbageCollector) collect() error {
	ctx, cancel := context.WithTimeout(g.ctx, 10*time.Minute)
	defer cancel()
	now := time.Now()
	g.hlsSess.pruneSessions(now.Add(-sessMaxAge).UTC())
	// Drain the backlog in bounded batches, including after a long outage.
	for {
		ecs := g.idx.ExpiredChunks(now.Add(-g.maxAge).UTC(), expiredChunksMax)
		if len(ecs) == 0 {
			return nil
		}
		var errs []error
		for _, rc := range ecs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := g.obj.DeleteObject(ctx, rc.ObjectKey); err != nil {
				errs = append(errs, fmt.Errorf("delete %s: %w", rc.ObjectKey, err))
				continue
			}
			g.idx.Remove(rc)
		}
		if len(errs) > 0 {
			return errors.Join(errs...)
		}
	}
}
