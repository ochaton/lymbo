package lymbo

import (
	"context"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/ochaton/lymbo/status"
)

// Default configuration values.
const (
	// MinPollIntervalDefault is the default minimum time between store polls.
	MinPollIntervalDefault = 10 * time.Millisecond

	// MaxPollIntervalDefault is the default maximum time to wait between store polls.
	MaxPollIntervalDefault = 15 * time.Second

	// ExpirationBatchSize is the number of tickets to expire per batch.
	ExpirationBatchSize = 1000

	// ExpirationInterval is how often to run the expiration worker.
	ExpirationInterval = 100 * time.Millisecond
)

const (
	// MaxBackoffDelay is the maximum delay between retry attempts.
	MaxBackoffDelay = 15 * time.Second

	// DefaultBackoffBase is the default base for exponential backoff calculation.
	DefaultBackoffBase = 1.5

	// InfinityDelay is a duration representing an effectively infinite delay.
	InfinityDelay time.Duration = 100 * 365 * 24 * time.Hour
)

type msg struct {
	tid TicketId
	upd *UpdateSet
}

// Kharon is the main orchestrator for the job processing system.
// It coordinates polling, dispatching, and processing of tickets.
type Kharon struct {
	store    Store
	settings Settings
	logger   *slog.Logger
	income   chan *Ticket
	outcome  chan msg

	stats *stats
}

func (kh *Kharon) ResetStats() {
	kh.stats.reset()
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
		income:   make(chan *Ticket, s.workers),
		outcome:  make(chan msg, 10*s.workers),
		stats:    newStats(),
	}
}

func beforeUpdate(ctx context.Context, t *Ticket, o *Opts) error {
	if o.status != nil {
		t.Status = *o.status
	}

	// Calculate delay via strategy or use fixed delay
	var delay time.Duration
	if o.delayFunc != nil {
		delay = o.delayFunc(t)
	} else {
		delay = o.delay
	}
	t.Runat = time.Now().Add(delay)

	if o.errorReason != nil {
		t.ErrorReason = o.errorReason
	}
	if o.nice != nil {
		t.Nice = *o.nice
	}
	if o.payload != nil {
		t.Payload = o.payload
	}
	if o.update != nil {
		if err := o.update(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (k *Kharon) save(ctx context.Context, t *Ticket, o *Opts) error {
	if t == nil {
		return ErrTicketNil
	}
	if o.update != nil {
		return k.store.Update(ctx, t.ID, func(ctx context.Context, t *Ticket) error {
			return beforeUpdate(ctx, t, o)
		})
	}

	// Calculate delay via strategy or use fixed delay
	var delay time.Duration
	if o.delayFunc != nil {
		delay = o.delayFunc(t)
	} else {
		delay = o.delay
	}
	runat := time.Now().Add(delay)

	k.outcome <- msg{
		tid: t.ID,
		upd: &UpdateSet{
			Id:          t.ID,
			Status:      o.status,
			Nice:        o.nice,
			Runat:       &runat,
			Payload:     o.payload,
			ErrorReason: o.errorReason,
		},
	}
	return nil
}

func (k *Kharon) delete(_ context.Context, tid TicketId) error {
	k.outcome <- msg{
		tid: tid,
		upd: nil,
	}
	return nil
}

func toOpts(o *Opts, opts ...Option) *Opts {
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func (k *Kharon) Ack(ctx context.Context, t *Ticket, opts ...Option) error {
	o := toOpts(&Opts{keep: false, status: &status.Done, delay: InfinityDelay}, opts...)
	var err error
	if o.keep {
		err = k.save(ctx, t, o)
	} else {
		err = k.delete(ctx, t.ID)
	}
	if err != nil {
		return err
	}
	k.stats.acked.value.Add(1)
	return nil
}

// Done marks a ticket as successfully completed.
// It automatically adds the WithKeep option to retain the ticket in the store.
func (k *Kharon) Done(ctx context.Context, t *Ticket, opts ...Option) error {
	o := toOpts(&Opts{keep: true, status: &status.Done, delay: InfinityDelay}, opts...)
	if err := k.save(ctx, t, o); err != nil {
		return err
	}
	k.stats.done.value.Add(1)
	return nil
}

// Cancel marks a ticket as cancelled.
func (k *Kharon) Cancel(ctx context.Context, t *Ticket, opts ...Option) error {
	o := toOpts(&Opts{keep: false, status: &status.Cancelled, delay: InfinityDelay}, opts...)
	var err error
	if o.keep {
		err = k.save(ctx, t, o)
	} else {
		err = k.delete(ctx, t.ID)
	}
	if err != nil {
		return err
	}
	k.stats.canceled.value.Add(1)
	return nil
}

// Fail marks a ticket as failed.
func (k *Kharon) Fail(ctx context.Context, t *Ticket, opts ...Option) error {
	o := toOpts(&Opts{keep: true, status: &status.Failed, delay: InfinityDelay}, opts...)
	if err := k.save(ctx, t, o); err != nil {
		return err
	}
	k.stats.failed.value.Add(1)
	return nil
}

// Retry schedules a ticket for retry with updated parameters.
func (k *Kharon) Retry(ctx context.Context, t *Ticket, opts ...Option) error {
	o := toOpts(&Opts{keep: true}, opts...)
	// do not update status, it should be already 'pending'
	if err := k.save(ctx, t, o); err != nil {
		return err
	}
	k.stats.retried.value.Add(1)
	return nil
}

// Put adds a new ticket to the store with configured options.
func (k *Kharon) Put(ctx context.Context, t Ticket, opts ...Option) error {
	o := toOpts(&Opts{keep: true, status: &status.Pending}, opts...)
	if err := beforeUpdate(ctx, &t, o); err != nil {
		return err
	}
	if err := k.store.Put(ctx, t); err != nil {
		return err
	}
	k.stats.added.value.Add(1)
	return nil
}

// Delete removes a ticket from the store.
func (k *Kharon) Delete(ctx context.Context, tid TicketId) error {
	if err := k.store.Delete(ctx, tid); err != nil {
		return err
	}
	k.stats.deleted.value.Add(1)
	return nil
}

// Get retrieves a ticket from the store.
func (k *Kharon) Get(ctx context.Context, tid TicketId) (Ticket, error) {
	return k.store.Get(ctx, tid)
}

func (k *Kharon) Stats() Stats {
	return Stats{
		Added:          k.stats.added.value.Load(),
		Polled:         k.stats.polled.value.Load(),
		Scheduled:      k.stats.scheduled.value.Load(),
		Acked:          k.stats.acked.value.Load(),
		Failed:         k.stats.failed.value.Load(),
		Done:           k.stats.done.value.Load(),
		Retried:        k.stats.retried.value.Load(),
		Canceled:       k.stats.canceled.value.Load(),
		Deleted:        k.stats.deleted.value.Load(),
		Expired:        k.stats.expired.value.Load(),
		Processed:      k.stats.processed.value.Load(),
		RunningWorkers: k.stats.runningWorkers.value.Load(),
	}
}

// Run starts the Kharon job processing system with the given context and router.
// It spawns worker goroutines and begins polling for tickets to process.
// Returns when ctx is cancelled or an error occurs.
//
// On shutdown, all goroutines exit immediately. In-flight tickets are not
// drained since they are persisted and will be reprocessed on next startup.
func (k *Kharon) Run(ctx context.Context, r *Router) error {
	var wg sync.WaitGroup

	// Start pusher
	wg.Add(1)
	go k.runPusher(ctx, &wg)

	// Start workers
	for i := 0; i < k.settings.workers; i++ {
		wg.Add(1)
		go k.runWorker(ctx, r, &wg)
	}

	// Start expiration worker
	if k.settings.enableExpiration {
		wg.Add(1)
		go func() {
			defer wg.Done()
			k.runExpirationWorker(ctx)
		}()
	}

	k.logger.InfoContext(ctx, "kharon started",
		"workers", k.settings.workers,
		"min_poll_timeout", k.settings.minReactionDelay.String(),
		"max_poll_timeout", k.settings.maxReactionDelay.String(),
		"batch_size", k.settings.batchSize,
		"process_time", k.settings.processTime.String(),
	)

	// Run main polling loop (blocks until ctx cancelled)
	pollErr := k.runPoller(ctx)

	// Wait for all goroutines to exit
	wg.Wait()

	k.logger.InfoContext(ctx, "shutdown complete")
	return pollErr
}

// runWorker processes tickets from income channel.
// Exits when ctx is cancelled.
func (k *Kharon) runWorker(ctx context.Context, r *Router, wg *sync.WaitGroup) {
	k.stats.runningWorkers.value.Add(1)
	defer wg.Done()
	defer k.stats.runningWorkers.value.Add(-1)
	defer k.logger.DebugContext(ctx, "worker exiting")

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-k.income:
			k.processTicket(ctx, r, t)
			k.stats.processed.value.Add(1)
		}
	}
}

// runPusher batches and persists updates from outcome channel.
// Exits when ctx is cancelled, flushing any remaining batch.
func (k *Kharon) runPusher(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer k.logger.DebugContext(ctx, "pusher exiting")

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Track in-flight async flush goroutines
	var flushWg sync.WaitGroup
	defer flushWg.Wait()

	batch := make([]msg, 0, k.settings.workers*2)

	// asyncFlush sends batch to store asynchronously (non-blocking)
	asyncFlush := func() {
		if len(batch) == 0 {
			return
		}

		delIds := make([]TicketId, 0, len(batch))
		upds := make([]UpdateSet, 0, len(batch))

		for _, m := range batch {
			if m.upd == nil {
				delIds = append(delIds, m.tid)
			} else {
				upds = append(upds, *m.upd)
			}
		}
		batch = batch[:0]

		flushWg.Add(1)
		go func() {
			defer flushWg.Done()
			// Use independent context to ensure flush completes even if parent ctx is cancelled
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), k.settings.shutdownFlushTimeout)
			defer cancel()

			if len(delIds) > 0 {
				if err := k.store.DeleteBatch(flushCtx, delIds); err != nil {
					k.logger.ErrorContext(flushCtx, "error deleting batch", "error", err)
				}
			}
			if len(upds) > 0 {
				if err := k.store.UpdateBatch(flushCtx, upds); err != nil {
					k.logger.ErrorContext(flushCtx, "error updating batch", "error", err)
				}
			}
		}()
	}

	// syncFlush sends batch to store synchronously (for shutdown)
	syncFlush := func(flushCtx context.Context) {
		if len(batch) == 0 {
			return
		}

		delIds := make([]TicketId, 0, len(batch))
		upds := make([]UpdateSet, 0, len(batch))

		for _, m := range batch {
			if m.upd == nil {
				delIds = append(delIds, m.tid)
			} else {
				upds = append(upds, *m.upd)
			}
		}
		batch = batch[:0]

		if len(delIds) > 0 {
			if err := k.store.DeleteBatch(flushCtx, delIds); err != nil {
				k.logger.ErrorContext(flushCtx, "error deleting batch", "error", err)
			}
		}
		if len(upds) > 0 {
			if err := k.store.UpdateBatch(flushCtx, upds); err != nil {
				k.logger.ErrorContext(flushCtx, "error updating batch", "error", err)
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			// Final sync flush with fresh context
			flushCtx, cancel := context.WithTimeout(context.Background(), k.settings.shutdownFlushTimeout)
			syncFlush(flushCtx)
			cancel()
			return
		case m := <-k.outcome:
			batch = append(batch, m)
			if len(batch) >= cap(batch) {
				asyncFlush()
			}
		case <-ticker.C:
			asyncFlush()
		}
	}
}

// runPoller polls the store for pending tickets and sends them to workers.
// Returns when ctx is cancelled.
func (k *Kharon) runPoller(ctx context.Context) error {
	defer k.logger.DebugContext(ctx, "poller exiting")

	sleepDuration := k.settings.maxReactionDelay
	timer := time.NewTimer(sleepDuration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			sleepDuration = k.poll(ctx)
			if sleepDuration == 0 {
				return ctx.Err()
			}
			timer.Reset(sleepDuration)
		}
	}
}

// poll executes one polling cycle and returns the next sleep duration.
// Returns 0 if ctx is cancelled.
func (k *Kharon) poll(ctx context.Context) time.Duration {
	for {
		select {
		case <-ctx.Done():
			return 0
		default:
		}

		result, err := k.store.PollPending(ctx, PollRequest{
			Limit:           k.settings.batchSize,
			Now:             time.Now(),
			TTR:             k.settings.processTime,
			BackoffBase:     k.settings.backoffBase,
			MaxBackoffDelay: k.settings.maxBackoffDelay,
		})

		if err != nil {
			k.logger.ErrorContext(ctx, "error polling store", "error", err)
			return k.settings.maxReactionDelay
		}

		if result.SleepUntil != nil {
			d := time.Until(*result.SleepUntil)
			d = min(d, k.settings.maxReactionDelay)
			d = max(d, k.settings.minReactionDelay)
			return d
		}

		if len(result.Tickets) == 0 {
			return k.settings.maxReactionDelay
		}

		k.stats.polled.value.Add(int64(len(result.Tickets)))

		for _, t := range result.Tickets {
			select {
			case k.income <- &t:
				k.stats.scheduled.value.Add(1)
			case <-ctx.Done():
				return 0
			}
		}
	}
}

// runExpirationWorker runs a background worker that periodically expires old tickets.
// This worker is independent from the main pipeline and uses ctx.Done() for shutdown.
func (k *Kharon) runExpirationWorker(ctx context.Context) {
	k.logger.InfoContext(ctx, "ticket expiration worker started")
	ticker := time.NewTicker(k.settings.expirationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			k.logger.DebugContext(ctx, "ticket expiration worker exiting")
			return
		case <-ticker.C:
			if n, err := k.store.ExpireTickets(ctx, ExpirationBatchSize, time.Now()); err != nil {
				k.logger.ErrorContext(ctx, "error expiring tickets", "error", err)
			} else if n > 0 {
				k.stats.expired.value.Add(n)
				k.logger.DebugContext(ctx, "ticket expiration run completed", "expired_count", n)
			}
		}
	}
}

// processTicket processes a single ticket with the appropriate handler.
func (k *Kharon) processTicket(ctx context.Context, r *Router, t *Ticket) {
	handler := r.Handler(t)
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

	rctx, cancel := context.WithDeadline(ctx, t.Runat)
	defer cancel()
	err := handler.ProcessTicket(rctx, t)
	if err != nil {
		k.logger.ErrorContext(ctx, "error processing ticket",
			"ticket_id", t.ID,
			"type", t.Type,
			"error", err,
		)
	}
}
