package lymbo_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ochaton/lymbo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StoreFactory is a function that creates a store and returns a cleanup function
type StoreFactory func(t *testing.T) (store lymbo.Store, cleanup func())

// StoreTestSuite runs a comprehensive test suite against any Store implementation
type StoreTestSuite struct {
	factory StoreFactory
}

// NewStoreTestSuite creates a new test suite with the given store factory
func NewStoreTestSuite(factory StoreFactory) *StoreTestSuite {
	return &StoreTestSuite{factory: factory}
}

// RunAll runs all tests in the suite
func (s *StoreTestSuite) RunAll(t *testing.T) {
	t.Run("BasicWorkflow", s.TestBasicWorkflow)
	t.Run("FixedDelayStrategy", s.TestFixedDelayStrategy)
	t.Run("ExponentialBackoffStrategy", s.TestExponentialBackoffStrategy)
	t.Run("RetryWithFixedDelay", s.TestRetryWithFixedDelay)
	t.Run("DoneKeepsTicket", s.TestDoneKeepsTicket)
	t.Run("FailWithErrorReason", s.TestFailWithErrorReason)
	t.Run("CancelRemovesTicket", s.TestCancelRemovesTicket)
	t.Run("PriorityOrdering", s.TestPriorityOrdering)
	t.Run("NotFoundHandler", s.TestNotFoundHandler)
	t.Run("MultipleTicketsParallelProcessing", s.TestMultipleTicketsParallelProcessing)
	t.Run("ExponentialBackoffMaxDelay", s.TestExponentialBackoffMaxDelay)
	t.Run("GroupPendingCount", s.TestGroupPendingCount)
	t.Run("GroupAllTerminal", s.TestGroupAllTerminal)
	t.Run("GroupMixedStates", s.TestGroupMixedStates)
	t.Run("GroupEmpty", s.TestGroupEmpty)
	t.Run("GroupUngrouped", s.TestGroupUngrouped)
}

// TestBasicWorkflow tests the basic Put → Poll → Process → Acknowledge flow
func (s *StoreTestSuite) TestBasicWorkflow(t *testing.T) {
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

	// Register handler
	err := router.HandleFunc("test-task", func(ctx context.Context, t *lymbo.Ticket) error {
		// Only process once
		if processed.Load() {
			return nil
		}
		processedTicket = t
		processed.Store(true)
		return kh.Ack(ctx, t.ID)
	})
	require.NoError(t, err)

	// Create and put a ticket
	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "test-task")
	require.NoError(t, err)
	ticket.Payload, err = json.Marshal(map[string]string{"key": "value"})
	require.NoError(t, err)

	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	// Start kharon in background
	startKharon(t, ctx, kh, router)

	// Wait for processing
	require.Eventually(t, func() bool {
		return processed.Load()
	}, 5*time.Second, 10*time.Millisecond, "ticket should be processed")

	// Stop kharon to prevent re-polling
	cancel()
	time.Sleep(200 * time.Millisecond) // Wait for shutdown and final batch flush

	// Verify ticket was processed
	assert.NotNil(t, processedTicket)
	assert.Equal(t, ticket.ID, processedTicket.ID)

	// Verify ticket was deleted (acknowledged)
	ctx2 := context.Background()
	_, err = kh.Get(ctx2, ticket.ID)
	assert.ErrorIs(t, err, lymbo.ErrTicketNotFound, "ticket should be deleted after ack")

	// Verify stats
	stats := kh.Stats().Total()
	assert.Equal(t, int64(1), stats.Added, "should have 1 added ticket")
	assert.Equal(t, int64(1), stats.Acked, "should have 1 acked ticket")
	assert.Equal(t, int64(1), stats.Processed, "should have 1 processed ticket")
}

// TestFixedDelayStrategy tests that tickets with fixed delay are scheduled correctly
func (s *StoreTestSuite) TestFixedDelayStrategy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithProcessTime(100 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)

	delay := 500 * time.Millisecond
	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "test-task")
	require.NoError(t, err)

	beforePut := time.Now()
	err = kh.Put(ctx, *ticket, lymbo.WithDelay(lymbo.FixedDelay(delay)))
	require.NoError(t, err)
	afterPut := time.Now()

	// Retrieve and verify Runat
	retrieved, err := kh.Get(ctx, ticket.ID)
	require.NoError(t, err)

	expectedRunat := beforePut.Add(delay)
	// Allow some tolerance for test execution time
	assert.True(t, retrieved.Runat.After(expectedRunat.Add(-50*time.Millisecond)),
		"Runat should be approximately %v, got %v", expectedRunat, retrieved.Runat)
	assert.True(t, retrieved.Runat.Before(afterPut.Add(delay).Add(50*time.Millisecond)),
		"Runat should be approximately %v, got %v", expectedRunat, retrieved.Runat)
}

// TestExponentialBackoffStrategy tests exponential backoff delay calculation
func (s *StoreTestSuite) TestExponentialBackoffStrategy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	settings := lymbo.DefaultSettings().
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(1 * time.Millisecond).
		WithMaxReactionDelay(10 * time.Millisecond).
		WithWorkers(1)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	base := 1.5
	maxDelay := 3 * time.Second
	jitter := 100 * time.Millisecond

	retryCount := atomic.Int32{}
	var attempts []int
	var processTimes []time.Time
	mu := sync.Mutex{}

	// Handler that retries with exponential backoff
	err := router.HandleFunc("backoff-task", func(ctx context.Context, t *lymbo.Ticket) error {
		mu.Lock()
		attempts = append(attempts, t.Attempts)
		processTimes = append(processTimes, time.Now())
		mu.Unlock()

		count := retryCount.Add(1)
		if count < 4 {
			// Retry with exponential backoff
			return kh.Retry(ctx, t.ID, lymbo.WithDelay(
				lymbo.BackoffDelay(base, maxDelay, jitter),
			))
		}
		// Acknowledge after 4 attempts
		return kh.Ack(ctx, t.ID)
	})
	require.NoError(t, err)

	// Create and put ticket
	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "backoff-task")
	require.NoError(t, err)

	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	// Start kharon
	startKharon(t, ctx, kh, router)

	// Wait for all retries to complete
	require.Eventually(t, func() bool {
		return retryCount.Load() >= 4
	}, 12*time.Second, 10*time.Millisecond, "should complete 4 attempts")

	// Verify exponential backoff worked
	mu.Lock()
	defer mu.Unlock()

	require.Len(t, attempts, 4, "should have 4 attempts")

	// Log delays for debugging
	for i := 1; i < len(processTimes); i++ {
		actualDelay := processTimes[i].Sub(processTimes[i-1])
		t.Logf("Attempt %d: actualDelay=%v, attempts[i-1]=%d", i, actualDelay, attempts[i-1])
	}

	// Verify the total time shows that retries were delayed
	totalTime := processTimes[len(processTimes)-1].Sub(processTimes[0])
	t.Logf("Total time for 4 attempts: %v", totalTime)
	// With exponential backoff (base 1.5), we expect roughly 1.5s + 2.25s + 3s = 6.75s total
	// Allow significant tolerance due to batching
	assert.Greater(t, totalTime, 3*time.Second, "total time should show exponential delays")

	// Wait for final batch flush
	cancel()
	time.Sleep(200 * time.Millisecond)

	// Verify stats
	stats := kh.Stats().Total()
	assert.Equal(t, int64(1), stats.Added)
	assert.Equal(t, int64(3), stats.Retried)
	assert.Equal(t, int64(1), stats.Acked)
	assert.Equal(t, int64(4), stats.Processed)
}

// TestRetryWithFixedDelay tests retry operations with fixed delay
func (s *StoreTestSuite) TestRetryWithFixedDelay(t *testing.T) {
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

	const (
		delay        = 300 * time.Millisecond
		totalRetries = 3
	)

	var (
		retryCount atomic.Int32
		timestamps = make([]time.Time, 0, totalRetries)
		done       = make(chan []time.Time, 1)
	)

	err := router.HandleFunc("retry-task", func(ctx context.Context, t *lymbo.Ticket) error {
		// Handler invocations are serialized: workers=1 and the ticket is only
		// re-enqueued via Retry/Ack after the handler returns, so timestamps is
		// safe to append without a mutex.
		timestamps = append(timestamps, time.Now())

		count := retryCount.Add(1)
		if count < totalRetries {
			return kh.Retry(ctx, t.ID, lymbo.WithDelay(lymbo.FixedDelay(delay)))
		}
		done <- timestamps
		close(done)
		return kh.Ack(ctx, t.ID)
	})
	require.NoError(t, err)

	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "retry-task")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *ticket))

	var wg sync.WaitGroup
	wg.Go(func() { _ = kh.Run(ctx, router) })

	var result []time.Time
	select {
	case v := <-done:
		result = v
	case <-ctx.Done():
		t.Fatalf("timeout: retries=%d/%d", retryCount.Load(), totalRetries)
	}

	cancel()
	wg.Wait()

	require.Len(t, result, totalRetries)
	for i := 1; i < len(result); i++ {
		actualDelay := result[i].Sub(result[i-1])
		t.Logf("Retry %d: actual delay=%v, expected delay=%v", i, actualDelay, delay)
		assert.Greater(t, actualDelay, 50*time.Millisecond,
			"delay %d should have some delay, got %v", i, actualDelay)
	}
	totalTime := result[len(result)-1].Sub(result[0])
	t.Logf("Total time for %d attempts: %v", totalRetries, totalTime)
	assert.Greater(t, totalTime, 200*time.Millisecond, "total time should show retries were delayed")
}

// TestDoneKeepsTicket tests that Done() keeps the ticket in the store
func (s *StoreTestSuite) TestDoneKeepsTicket(t *testing.T) {
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

	err := router.HandleFunc("done-task", func(ctx context.Context, t *lymbo.Ticket) error {
		// Only process once
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

	startKharon(t, ctx, kh, router)

	require.Eventually(t, func() bool {
		return processed.Load()
	}, 5*time.Second, 10*time.Millisecond)

	// Wait for batch flush + stats increment (happens after DB UpdateBatch)
	require.Eventually(t, func() bool {
		return kh.Stats().Total().Done == 1
	}, 5*time.Second, 10*time.Millisecond, "stats.Done should reach 1 after batch flush")

	// Verify ticket still exists
	retrieved, err := kh.Get(ctx, ticket.ID)
	require.NoError(t, err)
	assert.Equal(t, ticket.ID, retrieved.ID)

	// Verify stats
	stats := kh.Stats().Total()
	assert.Equal(t, int64(1), stats.Done)
}

// TestFailWithErrorReason tests that Fail() stores error reason
func (s *StoreTestSuite) TestFailWithErrorReason(t *testing.T) {
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

	err := router.HandleFunc("fail-task", func(ctx context.Context, t *lymbo.Ticket) error {
		// Only process once
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

	startKharon(t, ctx, kh, router)

	require.Eventually(t, func() bool {
		return processed.Load()
	}, 5*time.Second, 10*time.Millisecond)

	// Wait for batch flush and verify ticket has error reason
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
		// Check if it's an empty byte slice (postgres store)
		if len(retrieved.ErrorReason) == 0 {
			return false
		}
		return true
	}, 2*time.Second, 10*time.Millisecond, "error reason should be set")

	// Verify error reason is set (exact format may vary by store)
	assert.NotNil(t, retrieved.ErrorReason)
	// For memory store, it's stored as-is; for postgres it's JSON bytes
	var errorReasonStr string
	err = json.Unmarshal(retrieved.ErrorReason, &errorReasonStr)
	require.NoError(t, err)
	assert.Contains(t, errorReasonStr, errorMsg, "error reason should contain the error message")

	// Verify stats
	stats := kh.Stats().Total()
	assert.Equal(t, int64(1), stats.Failed)
}

// TestCancelRemovesTicket tests that Cancel() removes the ticket by default
func (s *StoreTestSuite) TestCancelRemovesTicket(t *testing.T) {
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

	err := router.HandleFunc("cancel-task", func(ctx context.Context, t *lymbo.Ticket) error {
		// Only process once
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

	startKharon(t, ctx, kh, router)

	require.Eventually(t, func() bool {
		return processed.Load()
	}, 5*time.Second, 10*time.Millisecond)

	// Verify ticket was removed (wait for batch flush)
	require.Eventually(t, func() bool {
		_, err := kh.Get(ctx, ticket.ID)
		return err == lymbo.ErrTicketNotFound
	}, 1*time.Second, 10*time.Millisecond, "ticket should be deleted after cancel")

	// Verify stats
	stats := kh.Stats().Total()
	assert.Equal(t, int64(1), stats.Canceled)
}

// TestPriorityOrdering tests that tickets are processed by priority (nice value)
func (s *StoreTestSuite) TestPriorityOrdering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, cleanup := s.factory(t)
	defer cleanup()

	// Use single worker to ensure sequential processing
	settings := lymbo.DefaultSettings().
		WithWorkers(1).
		WithProcessTime(100 * time.Millisecond).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(100 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)
	router := lymbo.NewRouter()

	var processedOrder []int
	mu := sync.Mutex{}
	processedCount := atomic.Int32{}

	err := router.HandleFunc("priority-task", func(ctx context.Context, t *lymbo.Ticket) error {
		// Handle both memory store (map) and postgres store (JSON bytes)
		var payload map[string]int
		// Postgres stores as JSON bytes
		if err := json.Unmarshal(t.Payload, &payload); err != nil {
			return err
		}
		order := payload["order"]

		mu.Lock()
		processedOrder = append(processedOrder, order)
		mu.Unlock()

		processedCount.Add(1)
		return kh.Ack(ctx, t.ID)
	})
	require.NoError(t, err)

	// Create tickets with different priorities (lower nice = higher priority)
	tickets := []struct {
		nice  int
		order int
	}{
		{nice: 100, order: 1}, // Highest priority
		{nice: 500, order: 3}, // Lowest priority
		{nice: 200, order: 2}, // Medium priority
	}

	for _, tc := range tickets {
		ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "priority-task")
		require.NoError(t, err)
		ticket.Payload, err = json.Marshal(map[string]int{"order": tc.order})
		require.NoError(t, err)
		// Set the same Runat for all tickets to ensure priority is the only factor
		ticket.Runat = time.Now().Add(10 * time.Millisecond)

		err = kh.Put(ctx, *ticket, lymbo.WithNice(tc.nice))
		require.NoError(t, err)
	}

	// Small delay to ensure all tickets are in the store
	time.Sleep(20 * time.Millisecond)

	startKharon(t, ctx, kh, router)

	require.Eventually(t, func() bool {
		return processedCount.Load() == 3
	}, 5*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Verify all tickets were processed
	require.Len(t, processedOrder, 3, "all 3 tickets should be processed")
	t.Logf("Processing order: %v", processedOrder)

	// Verify highest priority (nice=100, order=1) was processed first
	assert.Equal(t, 1, processedOrder[0], "highest priority ticket should be processed first")
}

// TestNotFoundHandler tests the behavior when no handler is registered
func (s *StoreTestSuite) TestNotFoundHandler(t *testing.T) {
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

	handlerCalled := atomic.Bool{}
	var handlerError error
	var errorMu sync.Mutex

	router.NotFoundFunc(func(ctx context.Context, t *lymbo.Ticket) error {
		errorMu.Lock()
		handlerError = lymbo.ErrHandlerNotFound
		errorMu.Unlock()
		handlerCalled.Store(true)
		return kh.Ack(ctx, t.ID)
	})

	// Create ticket with unregistered type
	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "unknown-task")
	require.NoError(t, err)

	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	startKharon(t, ctx, kh, router)

	require.Eventually(t, func() bool {
		return handlerCalled.Load()
	}, 5*time.Second, 10*time.Millisecond)

	errorMu.Lock()
	defer errorMu.Unlock()
	assert.ErrorIs(t, handlerError, lymbo.ErrHandlerNotFound)
}

// TestMultipleTicketsParallelProcessing tests concurrent ticket processing
func (s *StoreTestSuite) TestMultipleTicketsParallelProcessing(t *testing.T) {
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

	err := router.HandleFunc("parallel-task", func(ctx context.Context, ticket *lymbo.Ticket) error {
		// Only process each ticket once (prevent re-polling due to batching delays)
		if _, loaded := processedTickets.LoadOrStore(ticket.ID.String(), true); loaded {
			return nil
		}

		// Simulate some work
		time.Sleep(50 * time.Millisecond)
		count := processedCount.Add(1)
		t.Logf("Processed ticket %s, count: %d", ticket.ID, count)
		return kh.Ack(ctx, ticket.ID)
	})
	require.NoError(t, err)

	// Create multiple tickets
	for i := 0; i < numTickets; i++ {
		ticket, err := lymbo.NewTicket(
			lymbo.TicketId(uuid.NewString()),
			"parallel-task",
		)
		require.NoError(t, err)
		err = kh.Put(ctx, *ticket)
		require.NoError(t, err)
	}

	startTime := time.Now()

	startKharon(t, ctx, kh, router)

	require.Eventually(t, func() bool {
		return processedCount.Load() == numTickets
	}, 10*time.Second, 10*time.Millisecond)

	duration := time.Since(startTime)
	t.Logf("Processed %d tickets in %v with %d workers", numTickets, duration, 4)

	// Verify parallel processing provided some speedup
	assert.Less(t, duration, 3*time.Second,
		"parallel processing should complete in reasonable time")

	// Wait for final batch flush
	cancel()
	time.Sleep(200 * time.Millisecond)

	stats := kh.Stats().Total()
	assert.Equal(t, int64(numTickets), stats.Added)
	assert.GreaterOrEqual(t, stats.Processed, int64(numTickets))
	assert.Equal(t, int64(numTickets), stats.Acked)
}

// TestExponentialBackoffMaxDelay tests that backoff respects max delay
func (s *StoreTestSuite) TestExponentialBackoffMaxDelay(t *testing.T) {
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

	base := 1.5
	maxDelay := 500 * time.Millisecond
	jitter := 0 * time.Millisecond

	retryCount := atomic.Int32{}
	var processTimes []time.Time
	mu := sync.Mutex{}

	err := router.HandleFunc("maxdelay-task", func(ctx context.Context, t *lymbo.Ticket) error {
		mu.Lock()
		processTimes = append(processTimes, time.Now())
		mu.Unlock()

		count := retryCount.Add(1)
		if count < 5 { // 5 attempts should exceed max delay
			return kh.Retry(ctx, t.ID, lymbo.WithDelay(
				lymbo.BackoffDelay(base, maxDelay, jitter),
			))
		}
		return kh.Ack(ctx, t.ID)
	})
	require.NoError(t, err)

	ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "maxdelay-task")
	require.NoError(t, err)

	err = kh.Put(ctx, *ticket)
	require.NoError(t, err)

	startKharon(t, ctx, kh, router)

	require.Eventually(t, func() bool {
		return retryCount.Load() >= 5
	}, 10*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Check that actual delays between processing times respect maxDelay
	// The first attempt has no retry delay, so we start from i=2
	for i := 2; i < len(processTimes); i++ {
		delay := processTimes[i].Sub(processTimes[i-1])
		t.Logf("Delay %d: %v", i, delay)
		// Allow some tolerance for system timing (TTR + tolerance)
		assert.LessOrEqual(t, delay, maxDelay+300*time.Millisecond,
			"delay should not exceed max delay of %v (got %v)", maxDelay, delay)
	}
}
