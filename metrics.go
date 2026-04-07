package main

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	serveEndpointErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tjts_serving_endpoint_errors",
		Help: "Count of errors serving content to users",
	}, []string{"protocol", "streamid"})
	fetchErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tjts_stream_fetch_errors",
		Help: "Count of errors while fetching stream playlist or chunks",
	}, []string{"streamid"})
)

var _ prometheus.Collector = (*metricsCollector)(nil)

type metricsCollector struct {
	idx *chunkIndex

	latestChunkFetchAgo *prometheus.Desc
}

func newMetricsCollector(idx *chunkIndex) *metricsCollector {
	return &metricsCollector{
		idx: idx,
		latestChunkFetchAgo: prometheus.NewDesc(
			"tjts_last_chunk_fetched_at",
			"Time when last chunk was fetched, by stream",
			[]string{"streamid"}, nil),
	}
}

func (m *metricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.latestChunkFetchAgo
}

func (m *metricsCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lcts := m.lastChunkTimes(ctx)
	for k, v := range lcts {
		ch <- prometheus.MustNewConstMetric(m.latestChunkFetchAgo, prometheus.GaugeValue, float64(v.Unix()), k)
	}
}

func (m *metricsCollector) lastChunkTimes(_ context.Context) map[string]time.Time {
	return m.idx.LastFetchedByStream()
}
