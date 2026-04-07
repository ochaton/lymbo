package lymbo_test

// This file contains typed-handler variants of every test in store_testsuite_test.go.
// Each test is identical to its untyped counterpart except that handlers are registered
// via lymbo.HandleFuncTyped instead of router.HandleFunc.
//
// Tests that do not register a handler (TestFixedDelayStrategy) or that use
// router.NotFoundFunc (TestNotFoundHandler) delegate directly to the original method.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ochaton/lymbo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RunAllTyped runs all store tests with handlers registered via HandleFuncTyped.
func (s *StoreTestSuite) RunAllTyped(t *testing.T) {
	t.Run("BasicWorkflow", s.TestBasicWorkflowTyped)
	t.Run("FixedDelayStrategy", s.TestFixedDelayStrategy)           // no handler — identical
	t.Run("ExponentialBackoffStrategy", s.TestExponentialBackoffStrategyTyped)
	t.Run("RetryWithFixedDelay", s.TestRetryWithFixedDelayTyped)
	t.Run("DoneKeepsTicket", s.TestDoneKeepsTicketTyped)
	t.Run("FailWithErrorReason", s.TestFailWithErrorReasonTyped)
	t.Run("CancelRemovesTicket", s.TestCancelRemovesTicketTyped)
	t.Run("PriorityOrdering", s.TestPriorityOrderingTyped)
	t.Run("NotFoundHandler", s.TestNotFoundHandler)                 // uses NotFoundFunc — identical
	t.Run("MultipleTicketsParallelProcessing", s.TestMultipleTicketsParallelProcessingTyped)
	t.Run("ExponentialBackoffMaxDelay", s.TestExponentialBackoffMaxDelayTyped)
}

func (s *StoreTestSuite) TestBasicWorkflowTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(100 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	processed := atomic.Bool{}
	var processedTicket *lymbo.Ticket

	err := lymbo.HandleFuncTyped(router, "test-task", func(ctx context.Context, t *lymbo.Ticket, _ struct{}) error {
		if processed.Load() {
			return nil
		}
		processedTicket = t
		processed.Store(true)
		return kh.Ack(ctx, t.ID)
	})
	require.NoError(t, err)

	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "test-task")
	require.NoError(t, err)
	ticket.Payload = map[string]string{"key": "value"}

	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	go func() { _ = kh.Run(ctx, router) }()

	require.Eventually(t, func() bool {
		return processed.Load()
	}, 5*time.Second, 10*time.Millisecond, "ticket should be processed")

	cancel()
	time.Sleep(200 * time.Millisecond)

	assert.NotNil(t, processedTicket)
	assert.Equal(t, ticket.ID, processedTicket.ID)

	_, err = kh.Get(context.Background(), ticket.ID)
	assert.ErrorIs(t, err, lymbo.ErrTicketNotFound, "ticket should be deleted after ack")

	stats := kh.Stats()
	assert.Equal(t, int64(1), stats.Added)
	assert.Equal(t, int64(1), stats.Acked)
	assert.Equal(t, int64(1), stats.Processed)
}

func (s *StoreTestSuite) TestExponentialBackoffStrategyTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(100 * time.Millisecond).
		WithWorkers(1)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	base := 2.0
	maxDelay := 5 * time.Second
	jitter := 100 * time.Millisecond

	retryCount := atomic.Int32{}
	var attempts []int
	var processTimes []time.Time
	mu := sync.Mutex{}

	err := lymbo.HandleFuncTyped(router, "backoff-task", func(ctx context.Context, t *lymbo.Ticket, _ struct{}) error {
		mu.Lock()
		attempts = append(attempts, t.Attempts)
		processTimes = append(processTimes, time.Now())
		mu.Unlock()

		count := retryCount.Add(1)
		if count < 4 {
			return kh.Retry(ctx, t.ID, lymbo.WithDelay(lymbo.BackoffDelay(base, maxDelay, jitter)))
		}
		return kh.Ack(ctx, t.ID)
	})
	require.NoError(t, err)

	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "backoff-task")
	require.NoError(t, err)
	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	go func() { _ = kh.Run(ctx, router) }()

	require.Eventually(t, func() bool {
		return retryCount.Load() >= 4
	}, 20*time.Second, 10*time.Millisecond, "should complete 4 attempts")

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, attempts, 4, "should have 4 attempts")

	for i := 1; i < len(processTimes); i++ {
		actualDelay := processTimes[i].Sub(processTimes[i-1])
		t.Logf("Attempt %d: actualDelay=%v, attempts[i-1]=%d", i, actualDelay, attempts[i-1])
	}

	totalTime := processTimes[len(processTimes)-1].Sub(processTimes[0])
	t.Logf("Total time for 4 attempts: %v", totalTime)
	assert.Greater(t, totalTime, 5*time.Second, "total time should show exponential delays")

	stats := kh.Stats()
	assert.Equal(t, int64(1), stats.Added)
	assert.Equal(t, int64(3), stats.Retried)
	assert.Equal(t, int64(1), stats.Acked)
	assert.Equal(t, int64(4), stats.Processed)
}

func (s *StoreTestSuite) TestRetryWithFixedDelayTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(100 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	delay := 300 * time.Millisecond
	retryCount := atomic.Int32{}
	var timestamps []time.Time
	mu := sync.Mutex{}

	err := lymbo.HandleFuncTyped(router, "retry-task", func(ctx context.Context, t *lymbo.Ticket, _ struct{}) error {
		mu.Lock()
		timestamps = append(timestamps, time.Now())
		mu.Unlock()

		count := retryCount.Add(1)
		if count < 3 {
			return kh.Retry(ctx, t.ID, lymbo.WithDelay(lymbo.FixedDelay(delay)))
		}
		return kh.Ack(ctx, t.ID)
	})
	require.NoError(t, err)

	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "retry-task")
	require.NoError(t, err)
	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	go func() { _ = kh.Run(ctx, router) }()

	require.Eventually(t, func() bool {
		return retryCount.Load() >= 3
	}, 5*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	for i := 1; i < len(timestamps); i++ {
		actualDelay := timestamps[i].Sub(timestamps[i-1])
		t.Logf("Retry %d: actual delay=%v, expected delay=%v", i, actualDelay, delay)
		assert.Greater(t, actualDelay, 50*time.Millisecond,
			"delay %d should have some delay, got %v", i, actualDelay)
	}
	totalTime := timestamps[len(timestamps)-1].Sub(timestamps[0])
	t.Logf("Total time for 3 attempts: %v", totalTime)
	assert.Greater(t, totalTime, 200*time.Millisecond, "total time should show retries were delayed")
}

func (s *StoreTestSuite) TestDoneKeepsTicketTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(100 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	processed := atomic.Bool{}

	err := lymbo.HandleFuncTyped(router, "done-task", func(ctx context.Context, t *lymbo.Ticket, _ struct{}) error {
		if processed.Load() {
			return nil
		}
		processed.Store(true)
		return kh.Done(ctx, t.ID)
	})
	require.NoError(t, err)

	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "done-task")
	require.NoError(t, err)
	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	go func() { _ = kh.Run(ctx, router) }()

	require.Eventually(t, func() bool {
		return processed.Load()
	}, 5*time.Second, 10*time.Millisecond)

	time.Sleep(200 * time.Millisecond)

	retrieved, err := kh.Get(ctx, ticket.ID)
	require.NoError(t, err)
	assert.Equal(t, ticket.ID, retrieved.ID)

	stats := kh.Stats()
	assert.Equal(t, int64(1), stats.Done)
}

func (s *StoreTestSuite) TestFailWithErrorReasonTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(100 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	errorMsg := "something went wrong"
	processed := atomic.Bool{}

	err := lymbo.HandleFuncTyped(router, "fail-task", func(ctx context.Context, t *lymbo.Ticket, _ struct{}) error {
		if processed.Load() {
			return nil
		}
		processed.Store(true)
		return kh.Fail(ctx, t.ID, lymbo.WithErrorReason(errorMsg))
	})
	require.NoError(t, err)

	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "fail-task")
	require.NoError(t, err)
	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	go func() { _ = kh.Run(ctx, router) }()

	require.Eventually(t, func() bool {
		return processed.Load()
	}, 5*time.Second, 10*time.Millisecond)

	var retrieved lymbo.Ticket
	require.Eventually(t, func() bool {
		var err error
		retrieved, err = kh.Get(ctx, ticket.ID)
		if err != nil {
			return false
		}
		if retrieved.ErrorReason == nil {
			return false
		}
		if b, ok := retrieved.ErrorReason.([]byte); ok && len(b) == 0 {
			return false
		}
		return true
	}, 2*time.Second, 10*time.Millisecond, "error reason should be set")

	assert.NotNil(t, retrieved.ErrorReason)

	stats := kh.Stats()
	assert.Equal(t, int64(1), stats.Failed)
}

func (s *StoreTestSuite) TestCancelRemovesTicketTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(100 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	processed := atomic.Bool{}

	err := lymbo.HandleFuncTyped(router, "cancel-task", func(ctx context.Context, t *lymbo.Ticket, _ struct{}) error {
		if processed.Load() {
			return nil
		}
		processed.Store(true)
		return kh.Cancel(ctx, t.ID)
	})
	require.NoError(t, err)

	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "cancel-task")
	require.NoError(t, err)
	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	go func() { _ = kh.Run(ctx, router) }()

	require.Eventually(t, func() bool {
		return processed.Load()
	}, 5*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		_, err := kh.Get(ctx, ticket.ID)
		return err == lymbo.ErrTicketNotFound
	}, 1*time.Second, 10*time.Millisecond, "ticket should be deleted after cancel")

	stats := kh.Stats()
	assert.Equal(t, int64(1), stats.Canceled)
}

// TestPriorityOrderingTyped uses a pre-built ticket-ID→order map instead of reading
// the payload, because the memory store's DecodePayload does not decode concrete
// structs (only *any destinations). Priority ordering itself is driven by Nice values.
func (s *StoreTestSuite) TestPriorityOrderingTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithWorkers(1).
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(100 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	var processedOrder []int
	var mu sync.Mutex
	processedCount := atomic.Int32{}

	// Map ticket ID → expected processing order; populated before kh.Run starts.
	orderByID := sync.Map{}

	err := lymbo.HandleFuncTyped(router, "priority-task", func(ctx context.Context, t *lymbo.Ticket, _ struct{}) error {
		val, _ := orderByID.Load(t.ID)
		order, _ := val.(int)

		mu.Lock()
		processedOrder = append(processedOrder, order)
		mu.Unlock()

		processedCount.Add(1)
		return kh.Ack(ctx, t.ID)
	})
	require.NoError(t, err)

	ticketDefs := []struct {
		nice  int
		order int
	}{
		{nice: 100, order: 1}, // highest priority
		{nice: 500, order: 3}, // lowest priority
		{nice: 200, order: 2}, // medium priority
	}

	for _, tc := range ticketDefs {
		ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "priority-task")
		require.NoError(t, err)
		ticket.Runat = time.Now().Add(10 * time.Millisecond)
		orderByID.Store(ticket.ID, tc.order)
		err = kh.Put(ctx, *ticket, lymbo.WithNice(tc.nice))
		require.NoError(t, err)
	}

	time.Sleep(20 * time.Millisecond)

	go func() { _ = kh.Run(ctx, router) }()

	require.Eventually(t, func() bool {
		return processedCount.Load() == 3
	}, 5*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, processedOrder, 3, "all 3 tickets should be processed")
	t.Logf("Processing order: %v", processedOrder)

	assert.Equal(t, 1, processedOrder[0], "highest priority ticket should be processed first")
}

func (s *StoreTestSuite) TestMultipleTicketsParallelProcessingTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithWorkers(4).
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(100 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	processedCount := atomic.Int32{}
	processedTickets := sync.Map{}
	const numTickets = 10

	err := lymbo.HandleFuncTyped(router, "parallel-task", func(ctx context.Context, ticket *lymbo.Ticket, _ struct{}) error {
		if _, loaded := processedTickets.LoadOrStore(ticket.ID.String(), true); loaded {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
		count := processedCount.Add(1)
		t.Logf("Processed ticket %s, count: %d", ticket.ID, count)
		return kh.Ack(ctx, ticket.ID)
	})
	require.NoError(t, err)

	for i := 0; i < numTickets; i++ {
		ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "parallel-task")
		require.NoError(t, err)
		err = kh.Put(ctx, *ticket)
		require.NoError(t, err)
	}

	startTime := time.Now()

	go func() { _ = kh.Run(ctx, router) }()

	require.Eventually(t, func() bool {
		return processedCount.Load() == numTickets
	}, 10*time.Second, 10*time.Millisecond)

	duration := time.Since(startTime)
	t.Logf("Processed %d tickets in %v with %d workers", numTickets, duration, 4)
	assert.Less(t, duration, 3*time.Second, "parallel processing should complete in reasonable time")

	stats := kh.Stats()
	assert.Equal(t, int64(numTickets), stats.Added)
	assert.GreaterOrEqual(t, stats.Processed, int64(numTickets))
	assert.Equal(t, int64(numTickets), stats.Acked)
}

func (s *StoreTestSuite) TestExponentialBackoffMaxDelayTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(100 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	base := 2.0
	maxDelay := 500 * time.Millisecond
	jitter := 0 * time.Millisecond

	retryCount := atomic.Int32{}
	var processTimes []time.Time
	mu := sync.Mutex{}

	err := lymbo.HandleFuncTyped(router, "maxdelay-task", func(ctx context.Context, t *lymbo.Ticket, _ struct{}) error {
		mu.Lock()
		processTimes = append(processTimes, time.Now())
		mu.Unlock()

		count := retryCount.Add(1)
		if count < 5 {
			return kh.Retry(ctx, t.ID, lymbo.WithDelay(lymbo.BackoffDelay(base, maxDelay, jitter)))
		}
		return kh.Ack(ctx, t.ID)
	})
	require.NoError(t, err)

	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "maxdelay-task")
	require.NoError(t, err)
	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	go func() { _ = kh.Run(ctx, router) }()

	require.Eventually(t, func() bool {
		return retryCount.Load() >= 5
	}, 10*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	for i := 2; i < len(processTimes); i++ {
		delay := processTimes[i].Sub(processTimes[i-1])
		t.Logf("Delay %d: %v", i, delay)
		assert.LessOrEqual(t, delay, maxDelay+300*time.Millisecond,
			"delay should not exceed max delay of %v (got %v)", maxDelay, delay)
	}
}
