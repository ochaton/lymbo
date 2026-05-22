package lymbo_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ochaton/lymbo"
)

// ConcurrentTestSuite exercises behaviors that require multiple Kharon
// instances sharing the same Store.
type ConcurrentTestSuite struct {
	factory StoreFactory
}

// NewConcurrentTestSuite creates a new concurrent-behavior suite.
func NewConcurrentTestSuite(factory StoreFactory) *ConcurrentTestSuite {
	return &ConcurrentTestSuite{factory: factory}
}

// RunAll runs all tests in the suite.
func (s *ConcurrentTestSuite) RunAll(t *testing.T) {
	t.Run("TwoKharonsShareStore", s.TestTwoKharonsShareStore)
}

// TestTwoKharonsShareStore verifies that two Kharon instances backed by the
// same Store concurrently consume tickets. A single ticket is put by an
// external producer; both consumers Retry it with a small FixedDelay so it
// cycles through the queue. After totalAttempts attempts, each Kharon must
// have processed it at least minPerKharon times.
func (s *ConcurrentTestSuite) TestTwoKharonsShareStore(t *testing.T) {
	const (
		totalAttempts = 20
		minPerKharon  = 3
		retryDelay    = 100 * time.Millisecond
		warmup        = 100 * time.Millisecond
		runTimeout    = 30 * time.Second
		loadTimeout   = 20 * time.Millisecond
	)

	delayOpt := lymbo.WithDelay(lymbo.FixedDelay(retryDelay))

	store, cleanup := s.factory(t)
	defer cleanup()

	mkSettings := func() *lymbo.Settings {
		return lymbo.DefaultSettings().
			WithoutExpiration().
			WithMinReactionDelay(50 * time.Millisecond).
			WithMaxReactionDelay(150 * time.Millisecond).
			WithProcessTime(2 * time.Second).
			WithFlushInterval(10 * time.Millisecond)
	}

	k1 := lymbo.NewKharon(store, mkSettings(), nil)
	k2 := lymbo.NewKharon(store, mkSettings(), nil)

	var (
		count1 atomic.Int32
		count2 atomic.Int32
		total  atomic.Int32
	)
	done := make(chan struct{})

	mkRouter := func(kh *lymbo.Kharon, counter *atomic.Int32) *lymbo.Router {
		r := lymbo.NewRouter()
		_ = r.HandleFunc("concurrent", func(ctx context.Context, tk *lymbo.Ticket) error {
			tm := time.NewTimer(loadTimeout)
			defer tm.Stop()
			<-tm.C
			counter.Add(1)
			return kh.Retry(ctx, tk.ID, delayOpt)
		})
		return r
	}

	ctx, cancel := context.WithTimeout(t.Context(), runTimeout)
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() { _ = k1.Run(ctx, mkRouter(k1, &count1)) })
	wg.Go(func() { _ = k2.Run(ctx, mkRouter(k2, &count2)) })

	wg.Go(func() {
		defer close(done)
		tm := time.NewTicker(100 * time.Millisecond)
		defer tm.Stop()

		// External producer (not one of the two Kharons running consumers).
		producer := lymbo.NewKharon(store, lymbo.DefaultSettings().WithoutExpiration(), nil)

		time.Sleep(warmup)

		id, _ := uuid.NewV7()
		ticket, err := lymbo.NewTicket(lymbo.TicketId(id.String()), "concurrent")
		if err != nil {
			t.Fatalf("NewTicket: %v", err)
		}
		if err := producer.Put(ctx, *ticket); err != nil {
			t.Fatalf("Put: %v", err)
		}

		for range tm.C {
			select {
			case <-ctx.Done():
				return
			default:
				tkt, err := producer.Get(ctx, ticket.ID)
				if err != nil {
					t.Logf("producer fails to get ticket: %s", err.Error())
					continue
				}
				if tkt.Attempts >= totalAttempts {
					return
				}
			}
		}
	})

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for %d attempts: total=%d k1=%d k2=%d",
			totalAttempts, total.Load(), count1.Load(), count2.Load())
	}

	cancel()
	wg.Wait()

	c1, c2 := count1.Load(), count2.Load()
	t.Logf("k1=%d k2=%d total=%d", c1, c2, total.Load())

	if c1 < minPerKharon {
		t.Errorf("k1 processed %d attempts, want at least %d", c1, minPerKharon)
	}
	if c2 < minPerKharon {
		t.Errorf("k2 processed %d attempts, want at least %d", c2, minPerKharon)
	}
}
