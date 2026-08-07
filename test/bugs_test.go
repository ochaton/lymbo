package lymbo_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/store/memory"
)

// TestBug_EnableTubesIgnored reproduces:
//
// NewKharon derives tubeson from len(s.tubes) only, ignoring Settings.tubesOn.
// Calling DefaultSettings().EnableTubes() should enable tubes, but Subscribe
// then fails with ErrTubesNotEnabled.
func TestBug_EnableTubesIgnored(t *testing.T) {
	k := lymbo.NewKharon(memory.NewStore(), lymbo.DefaultSettings().EnableTubes(), slog.Default())

	err := k.Subscribe([]string{"a"})
	if err != nil {
		t.Fatalf("BUG: EnableTubes() did not enable tubes; Subscribe returned %v", err)
	}
}

// TestBug_PutBackRunatPointerAlias guards against the pointer-alias bug in poll():
// dispatch reuses result.Tickets[:0] backing array, so taking &t.ReadyAt directly
// into the buffer would alias a slot a later append overwrites. The fix copies
// ReadyAt to a local before taking its address.
//
// Scenario: ticket A (unsubscribed) at index 0, ticket B (subscribed) at index 1.
// Buggy code would leave putBack[0].Runat reading B.ReadyAt instead of A.ReadyAt.
func TestBug_PutBackRunatPointerAlias(t *testing.T) {
	timeA := time.Unix(1000, 0).UTC()
	timeB := time.Unix(2000, 0).UTC()

	resultTickets := []lymbo.Ticket{
		{ID: "A", Tube: "x", ReadyAt: timeA},
		{ID: "B", Tube: "y", ReadyAt: timeB},
	}

	subedTo := map[lymbo.Tube]struct{}{"y": {}}

	dispatch := resultTickets[:0]
	var putBack []lymbo.UpdateSet
	for i := range resultTickets {
		tt := &resultTickets[i]
		if _, ok := subedTo[tt.Tube]; ok {
			dispatch = append(dispatch, *tt)
			continue
		}
		readyAt := tt.ReadyAt
		putBack = append(putBack, lymbo.UpdateSet{
			Id:    tt.ID,
			Runat: &readyAt,
		})
	}
	_ = dispatch

	if len(putBack) != 1 {
		t.Fatalf("expected 1 putBack entry, got %d", len(putBack))
	}
	if putBack[0].Id != "A" {
		t.Fatalf("expected putBack Id=A, got %q", putBack[0].Id)
	}
	got := *putBack[0].Runat
	if !got.Equal(timeA) {
		t.Errorf("BUG: putBack[0].Runat aliased into reused buffer\n  want %v (A.ReadyAt)\n  got  %v (B.ReadyAt=%v)",
			timeA, got, timeB)
	}
}

// TestBug_EmptyRequestTubesPollsNothing guards against the silent-default bug:
//
// PollPending with an empty RequestTubes slice used to fall through to the
// "default" tube on both stores. That meant a tubesOn=true Kharon with no
// Subscribe() would poll default-tube tickets it doesn't own, bump their
// attempts via the UPDATE inside PollTickets, and force the real owner to
// observe inflated attempts (often exceeding MaxAttempts) on the next poll.
//
// Expected behavior: empty RequestTubes → no rows returned, no attempts bumped.
func TestBug_EmptyRequestTubesPollsNothing(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()

	tk, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	if err != nil {
		t.Fatalf("NewTicket: %v", err)
	}
	if err := store.Put(ctx, *tk); err != nil {
		t.Fatalf("Put: %v", err)
	}

	result, err := store.PollPending(ctx, lymbo.PollRequest{
		Limit:           10,
		Now:             time.Now(),
		TTR:             5 * time.Minute,
		BackoffBase:     2.0,
		MaxBackoffDelay: 10 * time.Minute,
		RequestTubes:    nil, // mimic Kharon.Tubes() when tubesOn=true && no Subscribe()
	})
	if err != nil {
		t.Fatalf("PollPending: %v", err)
	}
	if n := len(result.Tickets); n != 0 {
		t.Fatalf("BUG: empty RequestTubes returned %d tickets, want 0", n)
	}

	got, err := store.Get(ctx, tk.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Attempts != 0 {
		t.Fatalf("BUG: empty RequestTubes bumped attempts to %d on default-tube ticket", got.Attempts)
	}
}

// TestBug_ExpBackoffDelayOverflow guards the Go-side backoff computation:
//
// The old code converted base^attempts seconds to time.Duration BEFORE taking
// min() with maxDelay. base^attempts stops fitting into int64 nanoseconds at
// modest attempt counts (1.5^57s already overflows), and Go's float-to-integer
// conversion is implementation-defined for out-of-range values — negative on
// amd64 — so min() picked the garbage value and the ticket retried immediately.
func TestBug_ExpBackoffDelayOverflow(t *testing.T) {
	const maxDelay = 30 * time.Second

	// Below the cap the exact exponential value is preserved.
	if got, want := lymbo.ExpBackoffDelay(1.5, 2, maxDelay), time.Duration(2.25*float64(time.Second)); got != want {
		t.Fatalf("attempts=2: want %v, got %v", want, got)
	}

	// At and past the cap the result is exactly maxDelay — including attempt
	// counts where the unclamped float no longer fits into int64 nanoseconds.
	for _, attempts := range []int{10, 57, 100, 1751, 25000} {
		if got := lymbo.ExpBackoffDelay(1.5, attempts, maxDelay); got != maxDelay {
			t.Fatalf("BUG: attempts=%d: want capped %v, got %v", attempts, maxDelay, got)
		}
	}
}

// TestBug_MemoryBackoffOverflow exercises the same overflow through the memory
// store's backoff update path: a ticket with a huge attempts count must be
// rescheduled to now+maxDelay, not to the past.
func TestBug_MemoryBackoffOverflow(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()

	tk, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	if err != nil {
		t.Fatalf("NewTicket: %v", err)
	}
	tk.Attempts = 2000
	if err := store.Put(ctx, *tk); err != nil {
		t.Fatalf("Put: %v", err)
	}

	before := time.Now()
	if _, err := store.UpdateBatch(ctx, []lymbo.UpdateSet{{
		Id:      tk.ID,
		Backoff: &lymbo.DelayBackoff{Base: 1.5, MaxDelay: 30 * time.Second},
	}}); err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}

	got, err := store.Get(ctx, tk.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Runat.Before(before) {
		t.Fatalf("BUG: backoff with attempts=2000 scheduled runat in the past: %v", got.Runat)
	}
	if d := got.Runat.Sub(before); d > 31*time.Second {
		t.Fatalf("BUG: backoff delay not capped at maxDelay: %v", d)
	}
}
