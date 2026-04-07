package lymbo

import (
	"slices"
	"time"
)

// Settings contains configuration options for Kharon.
type Settings struct {
	// processTime is the time-to-run for tickets (prevents re-polling during processing).
	// during processTime the ticket is effectively blocked in "pending" state (rescheduled)
	// during this time ticket is protected from double processing
	processTime time.Duration

	// maxBackoffDelay is the maximum delay between retry attempts.
	maxBackoffDelay time.Duration

	// backoffBase is the base for exponential backoff calculation.
	// The delay is calculated as: backoffBase^attempts seconds.
	// Defaults to DefaultBackoffBase.
	backoffBase float64

	// maxReactionDelay is the maximum time to wait between store polls.
	// Defaults to MaxPollIntervalDefault.
	maxReactionDelay time.Duration

	// minReactionDelay is the minimum time to wait between store polls.
	// Defaults to MinPollIntervalDefault.
	minReactionDelay time.Duration

	// batchSize is the maximum number of tickets to poll at once.
	// Defaults to 1, capped at Workers.
	batchSize int

	// workers is the number of concurrent ticket processors.
	// Defaults to 1.
	workers int

	// enableExpiration enables automatic cleanup of expired tickets.
	enableExpiration bool

	// expirationInterval
	expirationInterval time.Duration

	// shutdownFlushTimeout is the timeout for flushing remaining batch on shutdown.
	shutdownFlushTimeout time.Duration

	// flushInterval is the interval for flushing batches to the store.
	flushInterval time.Duration

	// tubes
	tubes []Tube
}

// DefaultSettings returns a Settings instance with sensible defaults.
func DefaultSettings() *Settings {
	return &Settings{
		processTime:          30 * time.Second,
		maxReactionDelay:     MaxPollIntervalDefault,
		minReactionDelay:     MinPollIntervalDefault,
		maxBackoffDelay:      MaxBackoffDelay,
		backoffBase:          DefaultBackoffBase,
		batchSize:            4,
		workers:              4,
		enableExpiration:     true,
		expirationInterval:   ExpirationInterval,
		shutdownFlushTimeout: 5 * time.Second,
		flushInterval:        100 * time.Millisecond,
		tubes:                nil, // nil means only Default tube
	}
}

func (s *Settings) WithExpiration() *Settings {
	s.enableExpiration = true
	return s
}

func (s *Settings) WithExpirationInterval(d time.Duration) *Settings {
	s.expirationInterval = d
	return s
}

func (s *Settings) WithWorkers(workers int) *Settings {
	s.workers = workers
	return s
}

func (s *Settings) WithBatchSize(batchSize int) *Settings {
	s.batchSize = batchSize
	return s
}

func (s *Settings) WithoutExpiration() *Settings {
	s.enableExpiration = false
	return s
}

func (s *Settings) WithProcessTime(d time.Duration) *Settings {
	s.processTime = d
	return s
}

// WithBackoffBase sets the base for exponential backoff calculation.
// The delay is calculated as: backoffBase^attempts seconds.
func (s *Settings) WithBackoffBase(base float64) *Settings {
	s.backoffBase = base
	return s
}

func (s *Settings) WithMaxReactionDelay(d time.Duration) *Settings {
	s.maxReactionDelay = d
	return s
}

func (s *Settings) WithMinReactionDelay(d time.Duration) *Settings {
	s.minReactionDelay = d
	return s
}

// WithShutdownFlushTimeout sets the timeout for flushing remaining batch on shutdown.
func (s *Settings) WithShutdownFlushTimeout(d time.Duration) *Settings {
	s.shutdownFlushTimeout = d
	return s
}

// WithFlushInterval sets the interval for flushing batches to the store.
func (s *Settings) WithFlushInterval(d time.Duration) *Settings {
	s.flushInterval = d
	return s
}

// WithTubes sets the tubes for Kharon to work with.
// If no tubes are provided, it defaults to the "default" tube.
func (s *Settings) WithOnlyTubes(tubes ...Tube) *Settings {
	s.tubes = slices.Compact(tubes)
	return s
}

// normalize applies defaults and constraints to the settings.
func (s *Settings) normalize() {
	if s.workers <= 0 {
		s.workers = 1
	}
	if s.maxReactionDelay <= 0 {
		s.maxReactionDelay = MaxPollIntervalDefault
	}
	if s.minReactionDelay <= 0 {
		s.minReactionDelay = MinPollIntervalDefault
	}
	if s.maxReactionDelay < s.minReactionDelay {
		s.maxReactionDelay = s.minReactionDelay
	}
	if s.batchSize <= 0 {
		s.batchSize = 1
	}
	if s.batchSize > s.workers {
		s.batchSize = s.workers
	}
	if s.backoffBase <= 0 {
		s.backoffBase = DefaultBackoffBase
	}
	if s.flushInterval <= 0 {
		s.flushInterval = 100 * time.Millisecond
	}
}
