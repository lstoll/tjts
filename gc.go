package main

import (
	"context"
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

	ticker *time.Ticker
	stopC  chan struct{}
}

func newGarbageCollector(l logrus.FieldLogger, idx *chunkIndex, obj objectDeleter, hlsSess *hlsSessions) *garbageCollector {
	return &garbageCollector{
		l:       l,
		idx:     idx,
		obj:     obj,
		hlsSess: hlsSess,
		stopC:   make(chan struct{}),
	}
}

func (g *garbageCollector) Run() error {
	if err := g.collect(); err != nil {
		return err
	}

	g.ticker = time.NewTicker(gcInterval)

	for {
		select {
		case <-g.ticker.C:
			if err := g.collect(); err != nil {
				return fmt.Errorf("running gc: %v", err)
			}
		case <-g.stopC:
			return nil
		}
	}
}

func (g *garbageCollector) Interrupt(_ error) {
	g.stopC <- struct{}{}
}

func (g *garbageCollector) collect() error {
	ctx := context.Background()

	n := g.hlsSess.pruneSessions(time.Now().Add(-sessMaxAge).UTC())
	if n > 0 {
		g.l.Debugf("gc'd %d hls sessions", n)
	}

	ecs := g.idx.ExpiredChunks(time.Now().Add(-chunkMaxAge).UTC(), expiredChunksMax)
	if len(ecs) < 1 {
		return nil
	}

	g.l.Debugf("found %d expired chunks (max %d)", len(ecs), expiredChunksMax)

	for _, rc := range ecs {
		if err := g.obj.DeleteObject(ctx, rc.ObjectKey); err != nil {
			return fmt.Errorf("deleting object %s: %v", rc.ObjectKey, err)
		}
		g.idx.Remove(rc)
		g.l.Debugf("deleted chunk %s seq %d", rc.ObjectKey, rc.Sequence)
	}

	return nil
}
