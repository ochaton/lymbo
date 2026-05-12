package lymbo_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FinalizerTestSuite tests the AfterGroup / finalizer feature.
type FinalizerTestSuite struct {
	factory StoreFactory
}

func NewFinalizerTestSuite(factory StoreFactory) *FinalizerTestSuite {
	return &FinalizerTestSuite{factory: factory}
}

func (s *FinalizerTestSuite) RunAll(t *testing.T) {
	t.Run("BlockedInvisibleToPoll", s.TestBlockedInvisibleToPoll)
	t.Run("BlockingIgnoresTubeAndRunat", s.TestBlockingIgnoresTubeAndRunat)
	t.Run("DoneUnblocks", s.TestDoneUnblocks)
	t.Run("FailedUnblocks", s.TestFailedUnblocks)
	t.Run("CancelledUnblocks", s.TestCancelledUnblocks)
	t.Run("DeleteUnblocks", s.TestDeleteUnblocks)
	t.Run("MultiBlockerPartialUnblock", s.TestMultiBlockerPartialUnblock)
	t.Run("ZeroMembersImmediatelyEligible", s.TestZeroMembersImmediatelyEligible)
	t.Run("LateMemberNotCaptured", s.TestLateMemberNotCaptured)
	t.Run("FinalizerInGroupError", s.TestFinalizerInGroupError)
	t.Run("IdempotentResubmission", s.TestIdempotentResubmission)
	t.Run("WithDelayIndependentGate", s.TestWithDelayIndependentGate)
	t.Run("FinalizerIsRegularTicket", s.TestFinalizerIsRegularTicket)
}

// pollStore calls PollPending directly on the store with sensible defaults.
// TTR is intentionally long so polled tickets don't resurface during the test.
func pollStore(ctx context.Context, t *testing.T, store lymbo.Store) []lymbo.Ticket {
	t.Helper()
	result, err := store.PollPending(ctx, lymbo.PollRequest{
		Limit:           20,
		Now:             time.Now(),
		TTR:             5 * time.Minute,
		BackoffBase:     2.0,
		MaxBackoffDelay: 10 * time.Minute,
		RequestTubes:    []lymbo.Tube{lymbo.DefaultTube},
	})
	require.NoError(t, err)
	return result.Tickets
}

func hasID(tickets []lymbo.Ticket, id lymbo.TicketId) bool {
	for _, t := range tickets {
		if t.ID == id {
			return true
		}
	}
	return false
}

func terminalUpdate(id lymbo.TicketId, s status.Status) lymbo.UpdateSet {
	return lymbo.UpdateSet{Id: id, Status: &s}
}

// TestBlockedInvisibleToPoll verifies REQ 1–2: blocked ticket never surfaces via PollPending.
func (s *FinalizerTestSuite) TestBlockedInvisibleToPoll(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	member, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *member, lymbo.WithGroup("g")))

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("g")))

	// Finalizer blocked — only member is polled.
	tickets := pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, member.ID), "member must be returned")
	assert.False(t, hasID(tickets, finalizer.ID), "finalizer must not be returned while blocked")

	// Complete member → dep deleted → finalizer unblocked.
	_, err = store.UpdateBatch(ctx, []lymbo.UpdateSet{terminalUpdate(member.ID, status.Done)})
	require.NoError(t, err)

	// Member was rescheduled (TTR=5m) so it won't reappear. Only finalizer should surface.
	tickets = pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, finalizer.ID), "finalizer must be eligible after member done")
	assert.False(t, hasID(tickets, member.ID), "rescheduled member must not reappear")
}

// TestBlockingIgnoresTubeAndRunat verifies REQ 2: blocking is row-existence only.
// Even with Runat in the past, a blocked ticket must not surface.
func (s *FinalizerTestSuite) TestBlockingIgnoresTubeAndRunat(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	member, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *member, lymbo.WithGroup("g")))

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)
	finalizer.Runat = time.Now().Add(-10 * time.Second) // clearly in the past
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("g")))

	tickets := pollStore(ctx, t, store)
	assert.False(t, hasID(tickets, finalizer.ID), "finalizer blocked even with past Runat")
	assert.True(t, hasID(tickets, member.ID))
}

// TestDoneUnblocks verifies REQ 3: Done terminal transition deletes dep rows.
func (s *FinalizerTestSuite) TestDoneUnblocks(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	member, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *member, lymbo.WithGroup("g")))

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("g")))

	_, err = store.UpdateBatch(ctx, []lymbo.UpdateSet{terminalUpdate(member.ID, status.Done)})
	require.NoError(t, err)

	tickets := pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, finalizer.ID), "finalizer must be eligible after member Done")
}

// TestFailedUnblocks verifies REQ 3: Failed terminal transition deletes dep rows.
func (s *FinalizerTestSuite) TestFailedUnblocks(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	member, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *member, lymbo.WithGroup("g")))

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("g")))

	_, err = store.UpdateBatch(ctx, []lymbo.UpdateSet{terminalUpdate(member.ID, status.Failed)})
	require.NoError(t, err)

	tickets := pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, finalizer.ID), "finalizer must be eligible after member Failed")
}

// TestCancelledUnblocks verifies REQ 3: Cancelled (kept) transition deletes dep rows.
func (s *FinalizerTestSuite) TestCancelledUnblocks(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	member, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *member, lymbo.WithGroup("g")))

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("g")))

	_, err = store.UpdateBatch(ctx, []lymbo.UpdateSet{terminalUpdate(member.ID, status.Cancelled)})
	require.NoError(t, err)

	tickets := pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, finalizer.ID), "finalizer must be eligible after member Cancelled")
}

// TestDeleteUnblocks verifies REQ 3+5: DeleteBatch deletes dep rows before the ticket row.
func (s *FinalizerTestSuite) TestDeleteUnblocks(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	member, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *member, lymbo.WithGroup("g")))

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("g")))

	// Simulate Ack (no-keep path): DeleteBatch must clean deps before deleting ticket.
	_, err = store.DeleteBatch(ctx, []lymbo.TicketId{member.ID})
	require.NoError(t, err)

	tickets := pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, finalizer.ID), "finalizer must be eligible after member deleted")
}

// TestMultiBlockerPartialUnblock verifies REQ 3: finalizer stays blocked until all members complete.
func (s *FinalizerTestSuite) TestMultiBlockerPartialUnblock(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	m1, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *m1, lymbo.WithGroup("g")))

	m2, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *m2, lymbo.WithGroup("g")))

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("g")))

	// Only m1 done — finalizer still blocked by m2.
	_, err = store.UpdateBatch(ctx, []lymbo.UpdateSet{terminalUpdate(m1.ID, status.Done)})
	require.NoError(t, err)

	tickets := pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, m2.ID), "m2 must still be polled")
	assert.False(t, hasID(tickets, finalizer.ID), "finalizer still blocked by m2")

	// m2 was rescheduled by previous poll. Complete it directly.
	_, err = store.UpdateBatch(ctx, []lymbo.UpdateSet{terminalUpdate(m2.ID, status.Done)})
	require.NoError(t, err)

	tickets = pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, finalizer.ID), "finalizer eligible after all members done")
}

// TestZeroMembersImmediatelyEligible verifies REQ 9: no pending members → finalizer immediately eligible.
func (s *FinalizerTestSuite) TestZeroMembersImmediatelyEligible(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)
	// Group "empty-g" has no pending members.
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("empty-g")))

	tickets := pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, finalizer.ID), "finalizer must be immediately eligible for empty group")
}

// TestLateMemberNotCaptured verifies REQ 8: members added after PutAfterGroup are not captured.
func (s *FinalizerTestSuite) TestLateMemberNotCaptured(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	m1, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *m1, lymbo.WithGroup("g")))

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("g")))

	// Late member — added after PutAfterGroup; must not be captured.
	late, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *late, lymbo.WithGroup("g")))

	// Complete m1 (the only captured blocker).
	_, err = store.UpdateBatch(ctx, []lymbo.UpdateSet{terminalUpdate(m1.ID, status.Done)})
	require.NoError(t, err)

	// Finalizer is unblocked (no dep on late member). Both finalizer and late are eligible.
	tickets := pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, finalizer.ID), "finalizer eligible after captured member done")
	assert.True(t, hasID(tickets, late.ID), "late member polled independently")
}

// TestFinalizerInGroupError verifies REQ 7: AfterGroup + WithGroup same ID → ErrFinalizerInGroup.
func (s *FinalizerTestSuite) TestFinalizerInGroupError(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)

	err = kh.Put(ctx, *finalizer, lymbo.WithGroup("g"), lymbo.AfterGroup("g"))
	require.ErrorIs(t, err, lymbo.ErrFinalizerInGroup)
}

// TestIdempotentResubmission verifies REQ 12: same finalizer ticket re-submitted → no-op.
func (s *FinalizerTestSuite) TestIdempotentResubmission(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	member, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *member, lymbo.WithGroup("g")))

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)

	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("g")))
	// Second submission of the same ticket — must be a no-op.
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("g")))

	// Finalizer still blocked (dep set unchanged, not duplicated).
	tickets := pollStore(ctx, t, store)
	require.Len(t, tickets, 1, "only member must be returned")
	assert.Equal(t, member.ID, tickets[0].ID)

	// Complete member → finalizer unblocks exactly once.
	_, err = store.UpdateBatch(ctx, []lymbo.UpdateSet{terminalUpdate(member.ID, status.Done)})
	require.NoError(t, err)

	tickets = pollStore(ctx, t, store)
	require.Len(t, tickets, 1, "only finalizer must surface")
	assert.Equal(t, finalizer.ID, tickets[0].ID)
}

// TestWithDelayIndependentGate verifies REQ 11: WithDelay and AfterGroup are independent gates.
// Finalizer is not eligible until BOTH: all members done AND delay expired.
func (s *FinalizerTestSuite) TestWithDelayIndependentGate(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	member, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *member, lymbo.WithGroup("g")))

	delay := 400 * time.Millisecond
	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "finalizer")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *finalizer,
		lymbo.AfterGroup("g"),
		lymbo.WithDelay(lymbo.FixedDelay(delay)),
	))

	// Complete member — dep deleted, but delay not yet expired.
	_, err = store.UpdateBatch(ctx, []lymbo.UpdateSet{terminalUpdate(member.ID, status.Done)})
	require.NoError(t, err)

	// Finalizer not eligible yet: delay gate still active.
	tickets := pollStore(ctx, t, store)
	assert.False(t, hasID(tickets, finalizer.ID), "finalizer must not be eligible before delay expires")

	// Wait for delay to expire.
	time.Sleep(delay + 100*time.Millisecond)

	tickets = pollStore(ctx, t, store)
	assert.True(t, hasID(tickets, finalizer.ID), "finalizer must be eligible after delay expires")
}

// TestFinalizerIsRegularTicket verifies REQ 10: finalizer has no special context — normal ticket shape.
func (s *FinalizerTestSuite) TestFinalizerIsRegularTicket(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	finalizer, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "my-finalizer-type")
	require.NoError(t, err)
	finalizer = finalizer.WithPayload(map[string]string{"result": "ok"})
	// Empty group — no members, immediately eligible.
	require.NoError(t, kh.Put(ctx, *finalizer, lymbo.AfterGroup("empty-g")))

	tickets := pollStore(ctx, t, store)
	require.Len(t, tickets, 1)

	got := tickets[0]
	assert.Equal(t, finalizer.ID, got.ID)
	assert.Equal(t, "my-finalizer-type", got.Type)
	assert.NotNil(t, got.Payload)
	assert.Equal(t, lymbo.DefaultTube, string(got.Tube))
}
