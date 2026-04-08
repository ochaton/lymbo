package stats

import (
	"sync"
	"time"
)

type Metric uint8

const (
	Add Metric = iota
	Poll
	Ack
	Fail
	Done
	Retry
	Cancel
	Delete
	Expire
	Process
	Workers
	TaskProcessDuration
)

// Measurers holds a set of counters and a histogram.
type Measurers struct {
	added          *counter
	polled         *counter
	acked          *counter
	failed         *counter
	done           *counter
	retried        *counter
	canceled       *counter
	deleted        *counter
	expired        *counter
	processed      *counter
	runningWorkers *counter

	taskProcessDuration *hist
	queueWaitDuration   *hist
}

func newMeasurers() Measurers {
	return Measurers{
		added:               newCounter(),
		polled:              newCounter(),
		acked:               newCounter(),
		failed:              newCounter(),
		done:                newCounter(),
		retried:             newCounter(),
		canceled:            newCounter(),
		deleted:             newCounter(),
		expired:             newCounter(),
		processed:           newCounter(),
		runningWorkers:      newCounter(),
		taskProcessDuration: newHist(4),
		queueWaitDuration:   newHist(4),
	}
}

func (m *Measurers) lookup(met Metric) *counter {
	switch met {
	case Add:
		return m.added
	case Poll:
		return m.polled
	case Ack:
		return m.acked
	case Fail:
		return m.failed
	case Done:
		return m.done
	case Retry:
		return m.retried
	case Cancel:
		return m.canceled
	case Delete:
		return m.deleted
	case Expire:
		return m.expired
	case Process:
		return m.processed
	case Workers:
		return m.runningWorkers
	default:
		return nil
	}
}

func (m *Measurers) Inc(met Metric) {
	if c := m.lookup(met); c != nil {
		c.Add(1)
	}
}

func (m *Measurers) snapshot() Measurements {
	return Measurements{
		Added:               m.added.Get(),
		Polled:              m.polled.Get(),
		Acked:               m.acked.Get(),
		Failed:              m.failed.Get(),
		Done:                m.done.Get(),
		Retried:             m.retried.Get(),
		Canceled:            m.canceled.Get(),
		Deleted:             m.deleted.Get(),
		Expired:             m.expired.Get(),
		Processed:           m.processed.Get(),
		RunningWorkers:      m.runningWorkers.Get(),
		TaskProcessDuration: m.taskProcessDuration.Snapshot(),
		QueueWaitDuration:   m.queueWaitDuration.Snapshot(),
	}
}

func (m *Measurers) reset() {
	m.added.Reset()
	m.polled.Reset()
	m.acked.Reset()
	m.failed.Reset()
	m.done.Reset()
	m.retried.Reset()
	m.canceled.Reset()
	m.deleted.Reset()
	m.expired.Reset()
	m.processed.Reset()
	m.runningWorkers.Reset()
	m.taskProcessDuration = newHist(4)
	m.queueWaitDuration = newHist(4)
}

// T is the top-level stats container.
type T struct {
	rw sync.RWMutex
	Measurers

	byType map[string]*Measurers
	byTube map[string]*Measurers
}

func New() *T {
	m := newMeasurers()
	return &T{
		Measurers: m,
		byType:    make(map[string]*Measurers),
		byTube:    make(map[string]*Measurers),
	}
}

func (s *T) Inc(met Metric) {
	s.Add(met, 1)
}

func (s *T) Dec(met Metric) {
	s.Add(met, -1)
}

func (s *T) Add(met Metric, n int64) {
	if c := s.Measurers.lookup(met); c != nil {
		s.rw.Lock()
		defer s.rw.Unlock()
		c.Add(n)
	}
}

func (s *T) Get(met Metric) int64 {
	if c := s.Measurers.lookup(met); c != nil {
		s.rw.RLock()
		defer s.rw.RUnlock()
		return c.Get()
	}
	return 0
}

// ByType returns the Measurers for the given ticket type, creating lazily.
func (s *T) ByType(ticketType string) *Measurers {
	s.rw.Lock()
	defer s.rw.Unlock()

	m, ok := s.byType[ticketType]
	if !ok {
		n := newMeasurers()
		m = &n
		s.byType[ticketType] = m
	}
	return m
}

// ByTube returns the Measurers for the given tube, creating lazily.
func (s *T) ByTube(tube string) *Measurers {
	s.rw.Lock()
	defer s.rw.Unlock()

	m, ok := s.byTube[tube]
	if !ok {
		n := newMeasurers()
		m = &n
		s.byTube[tube] = m
	}
	return m
}

func (s *T) ObserveQueueWaitDuration(ticketType string, tube string, duration time.Duration) {
	s.rw.Lock()
	s.Measurers.queueWaitDuration.RecordDuration(duration)
	s.rw.Unlock()

	s.ByType(ticketType).queueWaitDuration.RecordDuration(duration)
	s.ByTube(tube).queueWaitDuration.RecordDuration(duration)
}

func (s *T) ObserveTaskProcessDuration(ticketType string, tube string, duration time.Duration) {
	s.rw.Lock()
	s.Measurers.taskProcessDuration.RecordDuration(duration)
	s.rw.Unlock()

	s.ByType(ticketType).taskProcessDuration.RecordDuration(duration)
	s.ByTube(tube).taskProcessDuration.RecordDuration(duration)
}

func (s *T) Snapshot() Stats {
	s.rw.RLock()
	defer s.rw.RUnlock()

	snap := Stats{
		Measurements: s.Measurers.snapshot(),
	}

	if len(s.byType) > 0 {
		snap.ByType = make(map[string]Measurements, len(s.byType))
		for k, m := range s.byType {
			snap.ByType[k] = m.snapshot()
		}
	}
	if len(s.byTube) > 0 {
		snap.ByTube = make(map[string]Measurements, len(s.byTube))
		for k, m := range s.byTube {
			snap.ByTube[k] = m.snapshot()
		}
	}

	return snap
}

func (s *T) Reset() {
	s.rw.Lock()
	defer s.rw.Unlock()

	s.Measurers.reset()
	s.byType = make(map[string]*Measurers)
	s.byTube = make(map[string]*Measurers)
}

// Measurements holds counter and histogram values for a single scope (global, per-type, or per-tube).
type Measurements struct {
	Added          int64             `json:"added"`
	Polled         int64             `json:"polled"`
	Acked          int64             `json:"acked"`
	Failed         int64             `json:"failed"`
	Done           int64             `json:"done"`
	Retried        int64             `json:"retried"`
	Canceled       int64             `json:"canceled"`
	Deleted        int64             `json:"deleted"`
	Expired        int64             `json:"expired"`
	Processed      int64             `json:"processed"`
	RunningWorkers int64             `json:"runningWorkers"`
	TaskProcessDuration HistogramSnapshot `json:"taskProcessDuration"`
	QueueWaitDuration   HistogramSnapshot `json:"queueWaitDuration"`
}

// Stats is the top-level snapshot returned by T.Snapshot().
type Stats struct {
	Measurements

	ByType map[string]Measurements `json:"byType,omitempty"`
	ByTube map[string]Measurements `json:"byTube,omitempty"`
}
