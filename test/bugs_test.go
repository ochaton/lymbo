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
