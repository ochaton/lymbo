package lymbo

import (
	"context"
	"log/slog"
	"runtime/debug"
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
	t.Status = *o.status
	t.Runat = time.Now().Add(o.delay)
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

func (k *Kharon) save(ctx context.Context, tid TicketId, o *Opts) error {
	if o.update != nil {
		return k.store.Update(ctx, tid, func(ctx context.Context, t *Ticket) error {
			return beforeUpdate(ctx, t, o)
		})
	}

	runat := time.Now().Add(o.delay)
	k.outcome <- msg{
		tid: tid,
		upd: &UpdateSet{
			Id:          tid,
			Status:      o.status,
			Nice:        o.nice,
			Runat:       &runat,
			Payload:     o.payload,
			ErrorReason: o.errorReason,
		},
	}
	return nil
	// return k.store.UpdateSet(ctx, UpdateSet{
	// 	Id:          tid,
	// 	Status:      o.status,
	// 	Nice:        o.nice,
	// 	Runat:       &runat,
	// 	Payload:     o.payload,
	// 	ErrorReason: o.errorReason,
	// })
}

func (k *Kharon) delete(_ context.Context, tid TicketId) error {
	// we should batch deletes, but for now...
	// what if do nothing?
	k.outcome <- msg{
		tid: tid,
		upd: nil,
	}
	return nil
	// return k.store.Delete(ctx, tid)
}

func toOpts(o *Opts, opts ...Option) *Opts {
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func (k *Kharon) Ack(ctx context.Context, tid TicketId, opts ...Option) error {
	o := toOpts(&Opts{keep: false, status: &status.Done, delay: InfinityDelay}, opts...)
	var err error
	if o.keep {
		err = k.save(ctx, tid, o)
	} else {
		err = k.delete(ctx, tid)
	}
	if err != nil {
		return err
	}
	k.stats.acked.value++
	return nil
}

// Done marks a ticket as successfully completed.
// It automatically adds the WithKeep option to retain the ticket in the store.
func (k *Kharon) Done(ctx context.Context, tid TicketId, opts ...Option) error {
	o := toOpts(&Opts{keep: true, status: &status.Done, delay: InfinityDelay}, opts...)
	if err := k.save(ctx, tid, o); err != nil {
		return err
	}
	k.stats.done.value++
	return nil
}

// Cancel marks a ticket as cancelled.
func (k *Kharon) Cancel(ctx context.Context, tid TicketId, opts ...Option) error {
	o := toOpts(&Opts{keep: false, status: &status.Cancelled, delay: InfinityDelay}, opts...)
	var err error
	if o.keep {
		err = k.save(ctx, tid, o)
	} else {
		err = k.delete(ctx, tid)
	}
	if err != nil {
		return err
	}
	k.stats.canceled.value++
	return nil
}

// Fail marks a ticket as failed.
func (k *Kharon) Fail(ctx context.Context, tid TicketId, opts ...Option) error {
	o := toOpts(&Opts{keep: true, status: &status.Failed, delay: InfinityDelay}, opts...)
	if err := k.save(ctx, tid, o); err != nil {
		return err
	}
	k.stats.failed.value++
	return nil
}

// Retry schedules a ticket for retry with updated parameters.
func (k *Kharon) Retry(ctx context.Context, tid TicketId, opts ...Option) error {
	o := toOpts(&Opts{keep: true}, opts...)
	// do not update status, it should be already 'pending'
	if err := k.save(ctx, tid, o); err != nil {
		return err
	}
	k.stats.retried.value++
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
	k.stats.added.value++
	return nil
}

// Delete removes a ticket from the store.
func (k *Kharon) Delete(ctx context.Context, tid TicketId) error {
	if err := k.store.Delete(ctx, tid); err != nil {
		return err
	}
	k.stats.deleted.value++
	return nil
}

// Get retrieves a ticket from the store.
func (k *Kharon) Get(ctx context.Context, tid TicketId) (Ticket, error) {
	return k.store.Get(ctx, tid)
}

func (k *Kharon) Stats() Stats {
	return Stats{
		Added:     k.stats.added.value,
		Polled:    k.stats.polled.value,
		Scheduled: k.stats.scheduled.value,
		Acked:     k.stats.acked.value,
		Failed:    k.stats.failed.value,
		Done:      k.stats.done.value,
		Retried:   k.stats.retried.value,
		Canceled:  k.stats.canceled.value,
		Deleted:   k.stats.deleted.value,
		Expired:   k.stats.expired.value,
		Processed: k.stats.processed.value,
	}
}

// Run starts the Kharon job processing system with the given context and router.
// It spawns worker goroutines and begins polling for tickets to process.
// Returns when ctx is cancelled or an error occurs.
func (k *Kharon) Run(ctx context.Context, r *Router) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer close(k.outcome)
	defer close(k.income)

	work := func(ctx context.Context) {
		defer k.logger.DebugContext(ctx, "worker exiting")

		for {
			select {
			case <-ctx.Done():
				return
			case t, ok := <-k.income:
				if !ok {
					return
				}
				if t == nil {
					k.logger.WarnContext(ctx, "received nil ticket")
					continue
				}
				k.processTicket(ctx, r, t)
				k.stats.processed.value++
			}
		}
	}

	push := func(ctx context.Context) {
		defer k.logger.DebugContext(ctx, "push exiting")
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()

		batch := []msg{}

		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-k.outcome:
				if !ok {
					return
				}
				// There are basically 2 batches:
				// 1. those to delete
				// 2. those to save (update)
				batch = append(batch, m)
			case <-t.C:
				delIds := []TicketId{}
				for _, m := range batch {
					if m.upd == nil {
						delIds = append(delIds, m.tid)
					}
				}
				upds := []UpdateSet{}
				for _, m := range batch {
					if m.upd == nil {
						continue
					}
					upds = append(upds, *m.upd)
				}
				batch = batch[:0]
				go func() {
					if err := k.store.DeleteBatch(ctx, delIds); err != nil {
						k.logger.ErrorContext(ctx, "error deleting batch", "error", err)
					}
					if err := k.store.UpdateBatch(ctx, upds); err != nil {
						k.logger.ErrorContext(ctx, "error updating batch", "error", err)
					}
				}()
			}
		}
	}

	go push(wctx)

	for range k.settings.workers {
		go work(wctx)
	}

	if k.settings.enableExpiration {
		go k.runExpirationWorker(wctx)
	}

	k.logger.InfoContext(ctx, "kharon started",
		"workers", k.settings.workers,
		"min_poll_timeout", k.settings.minReactionDelay.String(),
		"max_poll_timeout", k.settings.maxReactionDelay.String(),
		"batch_size", k.settings.batchSize,
		"process_time", k.settings.processTime.String(),
	)

	sleepDuration := k.settings.maxReactionDelay
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
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
					sleepDuration = k.settings.maxReactionDelay
					break
				}

				if result.SleepUntil != nil {
					sleepDuration = time.Until(*result.SleepUntil)
					sleepDuration = min(sleepDuration, k.settings.maxReactionDelay)
					sleepDuration = max(sleepDuration, k.settings.minReactionDelay)
					break
				}
				if len(result.Tickets) == 0 {
					sleepDuration = k.settings.maxReactionDelay
					break
				}

				k.stats.polled.value += int64(len(result.Tickets))

				for _, t := range result.Tickets {
					k.income <- &t
					k.stats.scheduled.value++
				}
			}
		}
	}
}

// runExpirationWorker runs a background worker that periodically expires old tickets.
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
			} else {
				if n > 0 {
					k.stats.expired.value += n
					k.logger.DebugContext(ctx, "ticket expiration run completed", "expired_count", n)
				}
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
