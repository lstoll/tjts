package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	serveEndpointErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tjts_serving_endpoint_errors",
		Help: "Count of errors serving content to users",
	}, []string{"protocol", "streamid"})
)

var _ prometheus.Collector = (*metricsCollector)(nil)

type metricsCollector struct {
	db *sql.DB

	latestChunkFetchAgo *prometheus.Desc
}

func newMetricsCollector(db *sql.DB) *metricsCollector {
	return &metricsCollector{
		db: db,

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
	// we don't get a context from the scrape here, so just create one with a
	// reasonably timeout to avoid blocking things here
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lcts, err := m.lastChunkTimes(ctx)
	if err == nil {
		for k, v := range lcts {
			ch <- prometheus.MustNewConstMetric(m.latestChunkFetchAgo, prometheus.GaugeValue, float64(v.Unix()), k)
		}
	} else {
		ch <- prometheus.NewInvalidMetric(m.latestChunkFetchAgo, fmt.Errorf("get latest device location timestamp: %v", err))
	}
}

func (m *metricsCollector) lastChunkTimes(ctx context.Context) (map[string]time.Time, error) {
	// TODO - kinda sucks to have to convert to using time to get out of this
	// query?
	r, err := m.db.QueryContext(ctx, `select stream_id, strftime("%s", max(fetched_at)) from chunks group by stream_id`)
	if err != nil {
		return nil, fmt.Errorf("querying last chunk times: %v", err)
	}
	ret := map[string]time.Time{}
	for r.Next() {
		var (
			sid string
			fai int64
		)
		if err := r.Scan(&sid, &fai); err != nil {
			return nil, fmt.Errorf("scanning row: %v", err)
		}
		ret[sid] = time.Unix(fai, 0)
	}
	return ret, nil
}
