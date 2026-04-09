// Package prometheus provides a Prometheus collector for lymbo/kharon metrics.
//
// This is a separate Go module so that importing github.com/ochaton/lymbo
// does not pull in the prometheus client dependency.
//
// Usage:
//
//	import lymbo_prom "github.com/ochaton/lymbo/prometheus"
//
//	// Single instance, default registry:
//	prometheus.MustRegister(lymbo_prom.NewCollector(kh, nil))
//
//	// Multiple instances, custom registry:
//	reg := prometheus.NewRegistry()
//	reg.MustRegister(lymbo_prom.NewCollector(khPinger, prometheus.Labels{"kharon": "pinger"}))
//	reg.MustRegister(lymbo_prom.NewCollector(khPonger, prometheus.Labels{"kharon": "ponger"}))
package prometheus

import (
	"math"

	"github.com/ochaton/lymbo/stats"
	prom "github.com/prometheus/client_golang/prometheus"
)

// StatsProvider is satisfied by *lymbo.Kharon.
type StatsProvider interface {
	Stats() stats.Stats
}

// NewCollector creates a prometheus.Collector that exports metrics from the given provider.
// constLabels are added to every metric — use them to distinguish multiple
// kharon instances within the same process. Pass nil for single-instance setups.
func NewCollector(sp StatsProvider, constLabels prom.Labels) prom.Collector {
	return &collector{
		sp: sp,
		ticketsDesc: prom.NewDesc(
			"kharon_tickets_total",
			"Cumulative count of ticket operations.",
			[]string{"operation", "type", "tube"}, constLabels,
		),
		workersDesc: prom.NewDesc(
			"kharon_workers_running",
			"Current number of active worker goroutines.",
			nil, constLabels,
		),
		processDurationDesc: prom.NewDesc(
			"kharon_task_process_duration_seconds",
			"Histogram of ticket processing durations.",
			[]string{"type", "tube"}, constLabels,
		),
		queueWaitDesc: prom.NewDesc(
			"kharon_queue_wait_duration_seconds",
			"Histogram of time tickets waited in queue before processing.",
			[]string{"type", "tube"}, constLabels,
		),
	}
}

type collector struct {
	sp StatsProvider

	ticketsDesc         *prom.Desc
	workersDesc         *prom.Desc
	processDurationDesc *prom.Desc
	queueWaitDesc       *prom.Desc
}

func (c *collector) Describe(ch chan<- *prom.Desc) {
	ch <- c.ticketsDesc
	ch <- c.workersDesc
	ch <- c.processDurationDesc
	ch <- c.queueWaitDesc
}

var operationCounters = []struct {
	label string
	get   func(m stats.Measurements) int64
}{
	{"added", func(m stats.Measurements) int64 { return m.Added }},
	{"polled", func(m stats.Measurements) int64 { return m.Polled }},
	{"acked", func(m stats.Measurements) int64 { return m.Acked }},
	{"failed", func(m stats.Measurements) int64 { return m.Failed }},
	{"done", func(m stats.Measurements) int64 { return m.Done }},
	{"retried", func(m stats.Measurements) int64 { return m.Retried }},
	{"canceled", func(m stats.Measurements) int64 { return m.Canceled }},
	{"deleted", func(m stats.Measurements) int64 { return m.Deleted }},
	{"expired", func(m stats.Measurements) int64 { return m.Expired }},
	{"processed", func(m stats.Measurements) int64 { return m.Processed }},
}

func (c *collector) Collect(ch chan<- prom.Metric) {
	snap := c.sp.Stats()

	// Per-(type, tube) counters and histograms
	for key, m := range snap.ByKey {
		for _, op := range operationCounters {
			v := op.get(m)
			if v == 0 {
				continue
			}
			ch <- prom.MustNewConstMetric(
				c.ticketsDesc, prom.CounterValue, float64(v),
				op.label, key.Type, key.Tube,
			)
		}
		emitHistogram(c.processDurationDesc, m.TaskProcessDuration, key.Type, key.Tube, ch)
		emitHistogram(c.queueWaitDesc, m.QueueWaitDuration, key.Type, key.Tube, ch)
	}

	// Workers gauge (global, no type/tube)
	ch <- prom.MustNewConstMetric(
		c.workersDesc, prom.GaugeValue, float64(snap.RunningWorkers),
	)
}

// emitHistogram converts an exponential HistogramSnapshot into classic
// prometheus buckets. Upper bound for bucket index i: 2^((offset+i+1) / 2^scale).
func emitHistogram(desc *prom.Desc, hs stats.HistogramSnapshot, ticketType, tube string, ch chan<- prom.Metric) {
	if hs.Count == 0 {
		return
	}

	buckets := make(map[float64]uint64)
	var cumulative uint64

	for i, c := range hs.Counts {
		idx := hs.Offset + int32(i)
		upperBound := math.Exp2(float64(idx+1) / math.Exp2(float64(hs.Scale)))
		cumulative += c
		buckets[upperBound] = cumulative
	}

	ch <- prom.MustNewConstHistogram(
		desc, hs.Count, hs.SumSec, buckets, ticketType, tube,
	)
}
