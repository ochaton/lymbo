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

// Key identifies a (ticket type, tube) pair for per-key stats.
type Key struct {
	Type string
	Tube string
}

// Measurers holds a set of counters and a histogram.
type Measurers struct {
	added     *counter
	polled    *counter
	acked     *counter
	failed    *counter
	done      *counter
	retried   *counter
	canceled  *counter
	deleted   *counter
	expired   *counter
	processed *counter

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
		taskProcessDuration: newHist(2),
		queueWaitDuration:   newHist(2),
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
		TaskProcessDuration: m.taskProcessDuration.Snapshot(),
		QueueWaitDuration:   m.queueWaitDuration.Snapshot(),
	}
}

// T is the top-level stats container.
type T struct {
	rw sync.RWMutex

	runningWorkers *counter
	byKey          map[Key]*Measurers
}

func New() *T {
	return &T{
		runningWorkers: newCounter(),
		byKey:          make(map[Key]*Measurers),
	}
}

func (s *T) IncWorkers() {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.runningWorkers.Add(1)
}

func (s *T) DecWorkers() {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.runningWorkers.Add(-1)
}

// ByKey returns the Measurers for the given (type, tube) pair, creating lazily.
func (s *T) ByKey(ticketType, tube string) *Measurers {
	s.rw.Lock()
	defer s.rw.Unlock()

	k := Key{Type: ticketType, Tube: tube}
	m, ok := s.byKey[k]
	if !ok {
		n := newMeasurers()
		m = &n
		s.byKey[k] = m
	}
	return m
}

func (s *T) ObserveQueueWaitDuration(ticketType, tube string, duration time.Duration) {
	s.ByKey(ticketType, tube).queueWaitDuration.RecordDuration(duration)
}

func (s *T) ObserveTaskProcessDuration(ticketType, tube string, duration time.Duration) {
	s.ByKey(ticketType, tube).taskProcessDuration.RecordDuration(duration)
}

func (s *T) Snapshot() Stats {
	s.rw.RLock()
	defer s.rw.RUnlock()

	snap := Stats{
		RunningWorkers: s.runningWorkers.Get(),
	}

	if len(s.byKey) > 0 {
		snap.ByKey = make(map[Key]Measurements, len(s.byKey))
		for k, m := range s.byKey {
			snap.ByKey[k] = m.snapshot()
		}
	}

	return snap
}

func (s *T) Reset() {
	s.rw.Lock()
	defer s.rw.Unlock()

	s.byKey = make(map[Key]*Measurers)
}

// Measurements holds counter and histogram values for a single (type, tube) scope.
type Measurements struct {
	Added               int64             `json:"added"`
	Polled              int64             `json:"polled"`
	Acked               int64             `json:"acked"`
	Failed              int64             `json:"failed"`
	Done                int64             `json:"done"`
	Retried             int64             `json:"retried"`
	Canceled            int64             `json:"canceled"`
	Deleted             int64             `json:"deleted"`
	Expired             int64             `json:"expired"`
	Processed           int64             `json:"processed"`
	TaskProcessDuration HistogramSnapshot `json:"taskProcessDuration"`
	QueueWaitDuration   HistogramSnapshot `json:"queueWaitDuration"`
}

// Stats is the top-level snapshot returned by T.Snapshot().
type Stats struct {
	RunningWorkers int64                `json:"runningWorkers"`
	ByKey          map[Key]Measurements `json:"byKey,omitempty"`
}

// Total aggregates all per-key measurements into a single Measurements.
func (s Stats) Total() Measurements {
	var t Measurements
	for _, m := range s.ByKey {
		t.Added += m.Added
		t.Polled += m.Polled
		t.Acked += m.Acked
		t.Failed += m.Failed
		t.Done += m.Done
		t.Retried += m.Retried
		t.Canceled += m.Canceled
		t.Deleted += m.Deleted
		t.Expired += m.Expired
		t.Processed += m.Processed
	}
	return t
}
