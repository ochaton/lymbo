package lymbo

import "sync/atomic"

type counter struct {
	value atomic.Int64
}

func (c *counter) Inc() {
	c.value.Add(1)
}

func (c *counter) Add(n int64) {
	c.value.Add(n)
}

func (c *counter) Get() int64 {
	return c.value.Load()
}

type stats struct {
	added          *counter
	polled         *counter
	scheduled      *counter
	acked          *counter
	failed         *counter
	done           *counter
	retried        *counter
	canceled       *counter
	deleted        *counter
	expired        *counter
	processed      *counter
	runningWorkers *counter
}

func (s *stats) Snapshot() Stats {
	return Stats{
		Added:          s.added.Get(),
		Polled:         s.polled.Get(),
		Scheduled:      s.scheduled.Get(),
		Acked:          s.acked.Get(),
		Failed:         s.failed.Get(),
		Done:           s.done.Get(),
		Retried:        s.retried.Get(),
		Canceled:       s.canceled.Get(),
		Deleted:        s.deleted.Get(),
		Expired:        s.expired.Get(),
		Processed:      s.processed.Get(),
		RunningWorkers: s.runningWorkers.Get(),
	}
}

// Stats contains counters for tracking ticket processing activity.
// All fields except RunningWorkers are cumulative counters that can be reset via ResetStats().
type Stats struct {
	// Added is the number of tickets added to the store via Put().
	Added int64 `json:"added"`
	// Polled is the number of tickets fetched from the store by the poller.
	Polled int64 `json:"polled"`
	// Scheduled is the number of tickets sent to workers for processing.
	Scheduled int64 `json:"scheduled"`
	// Acked is the number of tickets acknowledged (successfully completed and removed).
	Acked int64 `json:"acked"`
	// Failed is the number of tickets marked as failed.
	Failed int64 `json:"failed"`
	// Done is the number of tickets marked as done (completed but kept in store).
	Done int64 `json:"done"`
	// Retried is the number of tickets rescheduled for retry.
	Retried int64 `json:"retried"`
	// Canceled is the number of tickets canceled.
	Canceled int64 `json:"canceled"`
	// Deleted is the number of tickets explicitly deleted via Delete().
	Deleted int64 `json:"deleted"`
	// Expired is the number of tickets removed due to expiration.
	Expired int64 `json:"expired"`
	// Processed is the number of tickets that have been processed by workers.
	Processed int64 `json:"processed"`
	// RunningWorkers is the current number of active worker goroutines.
	// This is a gauge (current state), not a cumulative counter, and is not affected by ResetStats().
	RunningWorkers int64 `json:"runningWorkers"`
}

func newStats() *stats {
	return &stats{
		added:          &counter{},
		polled:         &counter{},
		scheduled:      &counter{},
		acked:          &counter{},
		failed:         &counter{},
		done:           &counter{},
		retried:        &counter{},
		canceled:       &counter{},
		deleted:        &counter{},
		expired:        &counter{},
		processed:      &counter{},
		runningWorkers: &counter{},
	}
}

func (s *stats) reset() {
	s.added.value.Store(0)
	s.polled.value.Store(0)
	s.scheduled.value.Store(0)
	s.acked.value.Store(0)
	s.failed.value.Store(0)
	s.done.value.Store(0)
	s.retried.value.Store(0)
	s.canceled.value.Store(0)
	s.deleted.value.Store(0)
	s.expired.value.Store(0)
	s.processed.value.Store(0)
}
