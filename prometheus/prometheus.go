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
			[]string{"operation"}, constLabels,
		),
		ticketsTypedDesc: prom.NewDesc(
			"kharon_tickets_by_type_total",
			"Cumulative count of ticket operations per ticket type.",
			[]string{"operation", "type"}, constLabels,
		),
		ticketsTubeDesc: prom.NewDesc(
			"kharon_tickets_by_tube_total",
			"Cumulative count of ticket operations per tube.",
			[]string{"operation", "tube"}, constLabels,
		),
		workersDesc: prom.NewDesc(
			"kharon_workers_running",
			"Current number of active worker goroutines.",
			nil, constLabels,
		),
		durationByTypeDesc: prom.NewDesc(
			"kharon_task_process_duration_seconds",
			"Histogram of ticket processing durations.",
			[]string{"type"}, constLabels,
		),
		durationByTubeDesc: prom.NewDesc(
			"kharon_task_process_duration_by_tube_seconds",
			"Histogram of ticket processing durations by tube.",
			[]string{"tube"}, constLabels,
		),
		queueWaitByTypeDesc: prom.NewDesc(
			"kharon_queue_wait_duration_seconds",
			"Histogram of time tickets waited in queue before processing.",
			[]string{"type"}, constLabels,
		),
		queueWaitByTubeDesc: prom.NewDesc(
			"kharon_queue_wait_duration_by_tube_seconds",
			"Histogram of time tickets waited in queue before processing, by tube.",
			[]string{"tube"}, constLabels,
		),
	}
}

type collector struct {
	sp StatsProvider

	ticketsDesc         *prom.Desc
	ticketsTypedDesc    *prom.Desc
	ticketsTubeDesc     *prom.Desc
	workersDesc         *prom.Desc
	durationByTypeDesc  *prom.Desc
	durationByTubeDesc  *prom.Desc
	queueWaitByTypeDesc *prom.Desc
	queueWaitByTubeDesc *prom.Desc
}

func (c *collector) Describe(ch chan<- *prom.Desc) {
	ch <- c.ticketsDesc
	ch <- c.ticketsTypedDesc
	ch <- c.ticketsTubeDesc
	ch <- c.workersDesc
	ch <- c.durationByTypeDesc
	ch <- c.durationByTubeDesc
	ch <- c.queueWaitByTypeDesc
	ch <- c.queueWaitByTubeDesc
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

	// Global counters
	for _, op := range operationCounters {
		v := op.get(snap.Measurements)
		if v == 0 {
			continue
		}
		ch <- prom.MustNewConstMetric(
			c.ticketsDesc, prom.CounterValue, float64(v), op.label,
		)
	}

	// Per-type counters
	for ticketType, m := range snap.ByType {
		for _, op := range operationCounters {
			v := op.get(m)
			if v == 0 {
				continue
			}
			ch <- prom.MustNewConstMetric(
				c.ticketsTypedDesc, prom.CounterValue, float64(v), op.label, ticketType,
			)
		}
	}

	// Per-tube counters
	for tube, m := range snap.ByTube {
		for _, op := range operationCounters {
			v := op.get(m)
			if v == 0 {
				continue
			}
			ch <- prom.MustNewConstMetric(
				c.ticketsTubeDesc, prom.CounterValue, float64(v), op.label, tube,
			)
		}
	}

	// Workers gauge
	ch <- prom.MustNewConstMetric(
		c.workersDesc, prom.GaugeValue, float64(snap.RunningWorkers),
	)

	// Per-type histograms
	for ticketType, m := range snap.ByType {
		emitHistogram(c.durationByTypeDesc, m.TaskProcessDuration, ticketType, ch)
		emitHistogram(c.queueWaitByTypeDesc, m.QueueWaitDuration, ticketType, ch)
	}

	// Per-tube histograms
	for tube, m := range snap.ByTube {
		emitHistogram(c.durationByTubeDesc, m.TaskProcessDuration, tube, ch)
		emitHistogram(c.queueWaitByTubeDesc, m.QueueWaitDuration, tube, ch)
	}
}

// emitHistogram converts an exponential HistogramSnapshot into classic
// prometheus buckets. Upper bound for bucket index i: 2^((offset+i+1) / 2^scale).
func emitHistogram(desc *prom.Desc, hs stats.HistogramSnapshot, ticketType string, ch chan<- prom.Metric) {
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
		desc, hs.Count, hs.SumSec, buckets, ticketType,
	)
}
