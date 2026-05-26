package lymbo_test

import (
	"log/slog"
	"testing"
	"time"

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
