package lymbo

import (
	"context"
	"log/slog"
	"math"
	"runtime/debug"
	"sync"
	"time"

	"github.com/ochaton/lymbo/stats"
	"github.com/ochaton/lymbo/status"
	"golang.org/x/time/rate"
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
	ExpirationInterval = MaxPollIntervalDefault
)

const (
	// MaxBackoffDelay is the maximum delay between retry attempts.
	MaxBackoffDelay = 15 * time.Second

	// DefaultBackoffBase is the default base for exponential backoff calculation.
	DefaultBackoffBase = 1.5
)

// InfinityDelay is a duration representing an effectively infinite delay.
var InfinityDelay = FixedDelay(100 * 365 * 24 * time.Hour)

type msg struct {
	tid    TicketId
	upd    *UpdateSet    // nil means delete
	intent stats.Metric  // API-level operation (Ack, Fail, Retry, etc.)
}

// Kharon is the main orchestrator for the job processing system.
// It coordinates polling, dispatching, and processing of tickets.
type Kharon struct {
	store    Store
	settings Settings
	logger   *slog.Logger
	income   chan *Ticket
	outcome  chan msg

	stats *stats.T
}

func (kh *Kharon) ResetStats() {
	kh.stats.Reset()
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

	st := stats.New()

	return &Kharon{
		store:    newObservableStore(store, st),
		settings: *s,
		logger:   logger,
		income:   make(chan *Ticket, s.workers),
		outcome:  make(chan msg, 10*s.workers),
		stats:    st,
	}
}

func beforeUpdate(ctx context.Context, t *Ticket, o *Opts) error {
	switch o.delay.how {
	case delayFixed:
		t.Runat = time.Now().Add(o.delay.fixed.duration)
	case delayExponential:
		// exponential backoff support
		delay := time.Duration(float64(time.Second) * math.Pow(o.delay.exponential.base, float64(t.Attempts)))
		delay = min(delay, o.delay.exponential.maxDelay)
		t.Runat = time.Now().Add(delay)
	default:
		// no delay
	}
	if o.status != nil {
		t.Status = *o.status
	}
	if o.errorReason != nil {
		t.ErrorReason = o.errorReason
	}
	if o.nice != nil {
		t.Nice = *o.nice
	}
	if o.payload != nil {
		t.Payload = o.payload
	}
	if o.resetAttempts {
		t.Attempts = 0
	}
	if o.transferTube != nil {
		t.Tube = *o.transferTube
	}
	if o.update != nil {
		if err := o.update(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (k *Kharon) save(ctx context.Context, tid TicketId, intent stats.Metric, o *Opts) error {
	if o.update != nil {
		return k.store.Update(ctx, tid, func(ctx context.Context, t *Ticket) error {
			return beforeUpdate(ctx, t, o)
		})
	}

	us := &UpdateSet{
		Id:          tid,
		Tube:        o.transferTube,
		Status:      o.status,
		Nice:        o.nice,
		Payload:     o.payload,
		ErrorReason: o.errorReason,
	}

	switch o.delay.how {
	case delayFixed:
		us.Runat = new(time.Time)
		*us.Runat = time.Now().Add(o.delay.fixed.duration)
	case delayExponential:
		us.Backoff = &DelayBackoff{
			Base:     o.delay.exponential.base,
			MaxDelay: o.delay.exponential.maxDelay,
			Jitter:   o.delay.exponential.jitter,
		}
	default:
		// no delay
	}

	k.outcome <- msg{
		tid:    tid,
		upd:    us,
		intent: intent,
	}
	return nil
}

func (k *Kharon) delete(_ context.Context, tid TicketId, intent stats.Metric) error {
	k.outcome <- msg{
		tid:    tid,
		upd:    nil,
		intent: intent,
	}
	return nil
}

func toOpts(o *Opts, opts ...Option) (*Opts, error) {
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}
	return o, nil
}

func (k *Kharon) Ack(ctx context.Context, tid TicketId, opts ...Option) error {
	o, err := toOpts(&Opts{keep: false, status: &status.Done, delay: InfinityDelay}, opts...)
	if err != nil {
		return err
	}
	if o.keep {
		err = k.save(ctx, tid, stats.Ack, o)
	} else {
		err = k.delete(ctx, tid, stats.Ack)
	}
	return err
}

// Done marks a ticket as successfully completed.
// It automatically adds the WithKeep option to retain the ticket in the store.
func (k *Kharon) Done(ctx context.Context, tid TicketId, opts ...Option) error {
	o, err := toOpts(&Opts{keep: true, status: &status.Done, delay: InfinityDelay}, opts...)
	if err != nil {
		return err
	}
	return k.save(ctx, tid, stats.Done, o)
}

// Cancel marks a ticket as cancelled.
func (k *Kharon) Cancel(ctx context.Context, tid TicketId, opts ...Option) error {
	o, err := toOpts(&Opts{keep: false, status: &status.Cancelled, delay: InfinityDelay}, opts...)
	if err != nil {
		return err
	}
	if o.keep {
		return k.save(ctx, tid, stats.Cancel, o)
	}
	return k.delete(ctx, tid, stats.Cancel)
}

// Fail marks a ticket as failed.
func (k *Kharon) Fail(ctx context.Context, tid TicketId, opts ...Option) error {
	o, err := toOpts(&Opts{keep: true, status: &status.Failed, delay: InfinityDelay}, opts...)
	if err != nil {
		return err
	}
	return k.save(ctx, tid, stats.Fail, o)
}

// Restart schedules a ticket for immediate retry by setting its status to pending and runat to now.
func (k *Kharon) Restart(ctx context.Context, tid TicketId, opts ...Option) error {
	o, err := toOpts(&Opts{keep: true, status: &status.Pending, delay: FixedDelay(0), resetAttempts: true}, opts...)
	if err != nil {
		return err
	}
	return k.save(ctx, tid, stats.Add, o)
}

// Retry schedules a ticket for retry with updated parameters.
func (k *Kharon) Retry(ctx context.Context, tid TicketId, opts ...Option) error {
	o, err := toOpts(&Opts{keep: true}, opts...)
	if err != nil {
		return err
	}
	return k.save(ctx, tid, stats.Retry, o)
}

// Put adds a new ticket to the store with configured options.
func (k *Kharon) Put(ctx context.Context, t Ticket, opts ...Option) error {
	o, err := toOpts(&Opts{keep: true, status: &status.Pending}, opts...)
	if err != nil {
		return err
	}
	if err := beforeUpdate(ctx, &t, o); err != nil {
		return err
	}
	return k.store.Put(ctx, t)
}

// Delete removes a ticket from the store.
func (k *Kharon) Delete(ctx context.Context, tid TicketId) error {
	return k.store.Delete(ctx, tid)
}

// Get retrieves a ticket from the store.
func (k *Kharon) Get(ctx context.Context, tid TicketId) (Ticket, error) {
	return k.store.Get(ctx, tid)
}

func (k *Kharon) Stats() stats.Stats {
	return k.stats.Snapshot()
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
	wg.Go(func() { k.runPusher(ctx) })

	// Start workers
	for i := 0; i < k.settings.workers; i++ {
		wg.Go(func() { k.runWorker(ctx, r) })
	}

	// Start expiration worker
	if k.settings.enableExpiration {
		wg.Go(func() { k.runExpirationWorker(ctx) })
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
func (k *Kharon) runWorker(ctx context.Context, r *Router) {
	k.stats.IncWorkers()
	defer k.stats.DecWorkers()
	defer k.logger.DebugContext(ctx, "worker exiting")

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-k.income:
			k.processTicket(ctx, r, t)
		}
	}
}

// runPusher batches and persists updates from outcome channel.
// Exits when ctx is cancelled, flushing any remaining batch.
func (k *Kharon) runPusher(ctx context.Context) {
	defer k.logger.DebugContext(ctx, "pusher exiting")

	ticker := time.NewTicker(k.settings.flushInterval)
	defer ticker.Stop()

	// Track in-flight async flush goroutines
	flushWg := &sync.WaitGroup{}
	defer flushWg.Wait()

	batch := make([]msg, 0, k.settings.workers*2)

	// flush sends batch to the store
	flush := func(ctx context.Context, await ...bool) {
		if len(batch) == 0 {
			return
		}

		type deleteMsg struct {
			id     TicketId
			intent stats.Metric
		}
		type updateMsg struct {
			set    UpdateSet
			intent stats.Metric
		}

		dels := make([]deleteMsg, 0, len(batch))
		upds := make([]updateMsg, 0, len(batch))

		for _, m := range batch {
			if m.upd == nil {
				dels = append(dels, deleteMsg{id: m.tid, intent: m.intent})
			} else {
				upds = append(upds, updateMsg{set: *m.upd, intent: m.intent})
			}
		}
		batch = batch[:0]
		doflush := func() {
			// Use independent context to ensure flush completes even if parent ctx is cancelled
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), k.settings.shutdownFlushTimeout)
			defer cancel()

			if len(dels) > 0 {
				delIds := make([]TicketId, len(dels))
				intentByID := make(map[TicketId]stats.Metric, len(dels))
				for i, d := range dels {
					delIds[i] = d.id
					intentByID[d.id] = d.intent
				}
				infos, err := k.store.DeleteBatch(flushCtx, delIds)
				if err != nil {
					k.logger.ErrorContext(flushCtx, "error deleting batch", "error", err)
				}
				for _, info := range infos {
					k.stats.ByKey(info.Type, string(info.Tube)).Inc(intentByID[info.Id])
				}
			}
			if len(upds) > 0 {
				sets := make([]UpdateSet, len(upds))
				intents := make([]stats.Metric, len(upds))
				for i, u := range upds {
					sets[i] = u.set
					intents[i] = u.intent
				}
				infos, err := k.store.UpdateBatch(flushCtx, sets)
				if err != nil {
					k.logger.ErrorContext(flushCtx, "error updating batch", "error", err)
				}
				for i, info := range infos {
					k.stats.ByKey(info.Type, string(info.Tube)).Inc(intents[i])
				}
			}
		}

		if len(await) > 0 && await[0] {
			doflush()
		} else {
			flushWg.Go(doflush)
		}
	}

	for {
		select {
		case <-ctx.Done():
			// Final sync flush with fresh context
			flushCtx, cancel := context.WithTimeout(context.Background(), k.settings.shutdownFlushTimeout)
			flush(flushCtx, true)
			cancel()
			return
		case m := <-k.outcome:
			batch = append(batch, m)
			if len(batch) >= cap(batch) {
				flush(ctx)
			}
		case <-ticker.C:
			flush(ctx)
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
		// run first
		sleepDuration = k.poll(ctx)
		if sleepDuration == 0 {
			return ctx.Err()
		}
		// then think
		k.logger.InfoContext(ctx, "poll sleeping", slog.Time("till", time.Now().Add(sleepDuration)))
		timer.Reset(sleepDuration)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// poll executes one polling cycle and returns the next sleep duration.
// Returns 0 if ctx is cancelled.
func (k *Kharon) poll(ctx context.Context) time.Duration {
	lim := rate.NewLimiter(rate.Every(k.settings.minReactionDelay), 1)
	slowdown := 0
	for {
		select {
		case <-ctx.Done():
			return 0
		default:
			if slowdown == 0 {
				break
			}

			mx := k.settings.maxReactionDelay.Seconds()
			mn := k.settings.minReactionDelay.Seconds()

			// sensetivity
			sense := float64(100)
			// [ 1 - 2^-x ] --> the goal is to have a formula,
			// which for x=0 gives minReactionDelay,
			// and for x=inf+ gives maxReactionDelay
			// but not linear.
			sleep := mn + (mx-mn)*(1-math.Exp2(-float64(slowdown)/sense))
			dur := time.Duration(sleep * float64(time.Second))
			k.logger.DebugContext(ctx,
				"slowdown", slog.Int("slowdown", slowdown),
				"dur", slog.Duration("dur", dur),
			)
			time.Sleep(dur)
		}

		// We shall pace PollPending
		// this will flatten the load to Postgres
		if slowdown == 0 {
			if err := lim.Wait(ctx); err != nil {
				return 0
			}
		}

		result, err := k.store.PollPending(ctx, PollRequest{
			Limit:           k.settings.batchSize,
			Now:             time.Now(),
			TTR:             k.settings.processTime,
			BackoffBase:     k.settings.backoffBase,
			MaxBackoffDelay: k.settings.maxBackoffDelay,
			RequestTubes:    k.settings.tubes,
		})

		if err != nil {
			k.logger.ErrorContext(ctx, "error polling store", "error", err)
			return k.settings.maxReactionDelay
		}

		resultSize := len(result.Tickets)
		if resultSize == 0 {
			// no tickets => no work
			// slow down
			slowdown++
			continue
		}

		// As soon as some tickets received, speedup
		if slowdown > 0 {
			slowdown--
			if resultSize == k.settings.batchSize {
				// if resultSize == requestedSize
				// go full-speed
				slowdown = 0
			}
		}

		for _, t := range result.Tickets {
			k.stats.ByKey(t.Type, t.Tube.String()).Inc(stats.Poll)
		}

		for _, t := range result.Tickets {
			select {
			case k.income <- &t:
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
			if expired, err := k.store.ExpireTickets(ctx, ExpirationBatchSize, time.Now()); err != nil {
				k.logger.ErrorContext(ctx, "error expiring tickets", "error", err)
			} else if len(expired) > 0 {
				k.logger.DebugContext(ctx, "ticket expiration run completed", "expired_count", len(expired))
			}
		}
	}
}

// processTicket processes a single ticket with the appropriate handler.
func (k *Kharon) processTicket(ctx context.Context, r *Router, t *Ticket) {
	s := time.Now()

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

	k.stats.ObserveQueueWaitDuration(t.Type, t.Tube.String(), s.Sub(t.ReadyAt))

	err := handler.ProcessTicket(rctx, t)
	k.stats.ObserveTaskProcessDuration(t.Type, t.Tube.String(), time.Since(s))

	if err != nil {
		k.logger.ErrorContext(ctx, "error processing ticket",
			"ticket_id", t.ID,
			"type", t.Type,
			"error", err,
		)
		return
	}
	k.stats.ByKey(t.Type, t.Tube.String()).Inc(stats.Process)
}
