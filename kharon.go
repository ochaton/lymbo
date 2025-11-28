package lymbo

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/ochaton/lymbo/status"
	"golang.org/x/sync/errgroup"
)

// Default configuration values.
const (
	// MinPollIntervalDefault is the default minimum time between store polls.
	MinPollIntervalDefault = 10 * time.Millisecond

	// MaxPollIntervalDefault is the default maximum time to wait between store polls.
	MaxPollIntervalDefault = 15 * time.Second

	// ExpirationBatchSize is the number of tickets to expire per batch.
	ExpirationBatchSize = 100

	// ExpirationInterval is how often to run the expiration worker.
	ExpirationInterval = 3 * time.Second
)

const (
	// MaxBackoffDelay is the maximum delay between retry attempts.
	MaxBackoffDelay = 15 * time.Second

	// BackoffBaseDefault is the default base for exponential backoff calculations.
	BackoffBaseDefault = 1.5

	secondsPerMinute = 60
	secondsPerHour         = 60 * secondsPerMinute
	secondsPerDay          = 24 * secondsPerHour
	unixToInternal   int64 = (1969*365 + 1969/4 - 1969/100 + 1969/400) * secondsPerDay
	maxInt64         int64 = int64(^uint64(0) >> 1)
	maxUnix          int64 = maxInt64 - unixToInternal
)

var maxDate = time.Unix(maxUnix, 0)

// Kharon is the main orchestrator for the job processing system.
// It coordinates polling, dispatching, and processing of tickets.
type Kharon struct {
	store    Store
	settings Settings
	logger   *slog.Logger
	bus      chan *Ticket
}

// Settings contains configuration options for Kharon.
type Settings struct {
	// processTime is the time-to-run for tickets (prevents re-polling during processing).
	processTime time.Duration

	// maxBackoffDelay is the maximum delay between retry attempts.
	maxBackoffDelay time.Duration

	// backoffBase is the base for exponential backoff calculations.
	// The delay is calculated as: base^attempts.
	// Defaults to BackoffBaseDefault (1.5).
	backoffBase float64

	// maxPollInterval is the maximum time to wait between store polls.
	// Defaults to MaxPollIntervalDefault.
	maxPollInterval time.Duration

	// minPollInterval is the minimum time to wait between store polls.
	// Defaults to MinPollIntervalDefault.
	minPollInterval time.Duration

	// batchSize is the maximum number of tickets to poll at once.
	// Defaults to 1, capped at Workers.
	batchSize int

	// workers is the number of concurrent ticket processors.
	// Defaults to 1.
	workers int

	// enableExpiration enables automatic cleanup of expired tickets.
	enableExpiration bool
}

// DefaultSettings returns a Settings instance with sensible defaults.
func DefaultSettings() *Settings {
	return &Settings{
		processTime:      30 * time.Second,
		maxPollInterval:  MaxPollIntervalDefault,
		minPollInterval:  MinPollIntervalDefault,
		maxBackoffDelay:  MaxBackoffDelay,
		backoffBase:      BackoffBaseDefault,
		batchSize:        10,
		workers:          4,
		enableExpiration: true,
	}
}

func (s *Settings) WithExpiration() *Settings {
	s.enableExpiration = true
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

// WithBackoffBase sets the base for exponential backoff calculations.
// The delay is calculated as: base^attempts.
func (s *Settings) WithBackoffBase(base float64) *Settings {
	s.backoffBase = base
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

// normalize applies defaults and constraints to the settings.
func (s *Settings) normalize() {
	if s.workers <= 0 {
		s.workers = 1
	}
	if s.maxPollInterval <= 0 {
		s.maxPollInterval = MaxPollIntervalDefault
	}
	if s.minPollInterval <= 0 {
		s.minPollInterval = MinPollIntervalDefault
	}
	if s.maxPollInterval < s.minPollInterval {
		s.maxPollInterval = s.minPollInterval
	}
	if s.batchSize <= 0 {
		s.batchSize = 1
	}
	if s.batchSize > s.workers {
		s.batchSize = s.workers
	}
	if s.backoffBase <= 1 {
		s.backoffBase = BackoffBaseDefault
	}
}

// NewKharon creates a new Kharon instance with the provided store, settings, and logger.
//
// The store parameter is required and must not be nil (panics otherwise).
// The logger parameter is optional; if nil, slog.Default() is used.
// Settings are normalized to apply defaults and constraints.
func NewKharon(store Store, s *Settings, logger *slog.Logger) *Kharon {
	if store == nil {
		panic("kharon: store cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if s == nil {
		s = DefaultSettings()
	}

	s.normalize()

	return &Kharon{
		store:    store,
		settings: *s,
		logger:   logger,
		bus:      make(chan *Ticket),
	}
}

func (k *Kharon) save(ctx context.Context, tid TicketId, s status.Status, o *Opts) error {
	return k.store.Update(ctx, tid, func(ctx context.Context, t *Ticket) error {
		t.Status = s
		now := time.Now()
		t.Mtime = &now
		if o.ExpireIn > 0 {
			t.Runat = now.Add(o.ExpireIn)
		} else {
			t.Runat = maxDate
		}
		return nil
	})
}

func (k *Kharon) Ack(ctx context.Context, tid TicketId, opts ...Option) error {
	o := &Opts{}
	for _, opt := range opts {
		opt(o)
	}

	if o.Keep {
		return k.save(ctx, tid, status.Done, o)
	}

	return k.store.Delete(ctx, tid)
}

// Done marks a ticket as successfully completed.
// It automatically adds the WithKeep option to retain the ticket in the store.
func (k *Kharon) Done(ctx context.Context, tid TicketId, opts ...Option) error {
	o := &Opts{}
	for _, opt := range opts {
		opt(o)
	}

	o.Keep = true
	return k.save(ctx, tid, status.Done, o)
}

// Cancel marks a ticket as cancelled.
func (k *Kharon) Cancel(ctx context.Context, tid TicketId, opts ...Option) error {
	o := &Opts{}
	for _, opt := range opts {
		opt(o)
	}

	if !o.Keep {
		return k.store.Delete(ctx, tid)
	}

	return k.save(ctx, tid, status.Cancelled, o)
}

// Fail marks a ticket as failed.
func (k *Kharon) Fail(ctx context.Context, tid TicketId, opts ...Option) error {
	o := &Opts{}
	for _, opt := range opts {
		opt(o)
	}

	o.Keep = true
	return k.save(ctx, tid, status.Failed, o)
}

// Add adds a new ticket to the store.
func (k *Kharon) Add(ctx context.Context, t Ticket) error {
	return k.store.Add(ctx, t)
}

// Delete removes a ticket from the store.
func (k *Kharon) Delete(ctx context.Context, tid TicketId) error {
	return k.store.Delete(ctx, tid)
}

// Get retrieves a ticket from the store.
func (k *Kharon) Get(ctx context.Context, tid TicketId) (Ticket, error) {
	return k.store.Get(ctx, tid)
}

// Run starts the Kharon job processing system with the given context and router.
// It spawns worker goroutines and begins polling for tickets to process.
// Returns when ctx is cancelled or an error occurs.
func (k *Kharon) Run(ctx context.Context, r *Router) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer close(k.bus)

	for range k.settings.workers {
		go k.work(wctx, r)
	}

	if k.settings.enableExpiration {
		go k.runExpirationWorker(wctx)
	}

	sleepDuration := k.settings.minPollInterval
	k.logger.InfoContext(ctx, "kharon started",
		"workers", k.settings.workers,
		"min_poll_timeout", k.settings.minPollInterval.String(),
		"max_poll_timeout", k.settings.maxPollInterval.String(),
		"batch_size", k.settings.batchSize,
		"process_time", k.settings.processTime.String(),
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
			result, err := k.store.PollPending(k.settings.batchSize, time.Now(), k.settings.processTime, k.settings.maxBackoffDelay, k.settings.backoffBase)
			if err != nil {
				k.logger.ErrorContext(ctx, "error polling store", "error", err)
				continue
			}

			if result.SleepUntil != nil {
				sleepDuration = time.Until(*result.SleepUntil)
				sleepDuration = min(sleepDuration, k.settings.maxPollInterval)
				sleepDuration = max(sleepDuration, k.settings.minPollInterval)
				continue
			}

			for _, t := range result.Tickets {
				k.bus <- &t
			}
		}
	}
}

// runExpirationWorker runs a background worker that periodically expires old tickets.
func (k *Kharon) runExpirationWorker(ctx context.Context) {
	k.logger.InfoContext(ctx, "ticket expiration worker started")
	ticker := time.NewTicker(ExpirationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			k.logger.DebugContext(ctx, "ticket expiration worker exiting")
			return
		case <-ticker.C:
			if err := k.store.ExpireTickets(ExpirationBatchSize, time.Now()); err != nil {
				k.logger.ErrorContext(ctx, "error expiring tickets", "error", err)
			} else {
				k.logger.DebugContext(ctx, "ticket expiration run completed")
			}
		}
	}
}

// work is the main worker loop that processes tickets from the bus.
func (k *Kharon) work(ctx context.Context, r *Router) {
	defer k.logger.DebugContext(ctx, "worker exiting")

	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-k.bus:
			if !ok {
				return
			}
			if t == nil {
				k.logger.WarnContext(ctx, "received nil ticket")
				continue
			}

			k.processTicket(ctx, r, t)
		}
	}
}

// processTicket processes a single ticket with the appropriate handler.
func (k *Kharon) processTicket(ctx context.Context, r *Router, t *Ticket) {
	handler := r.Handler(t)
	g := &errgroup.Group{}
	g.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				k.logger.ErrorContext(ctx, "panic occurred while processing ticket",
					"ticket_id", t.ID,
					"type", t.Type,
					"panic", r,
				)
				debug.PrintStack()
			}
		}()
		return handler.ProcessTicket(ctx, *t)
	})

	if err := g.Wait(); err != nil {
		k.logger.ErrorContext(ctx, "error processing ticket",
			"ticket_id", t.ID,
			"type", t.Type,
			"error", err,
		)
	}
}
