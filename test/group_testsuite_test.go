package lymbo_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ochaton/lymbo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGroupPendingCount verifies that tickets put into a group are counted correctly.
func (s *StoreTestSuite) TestGroupPendingCount(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)
	g := kh.Group("batch-count")

	for range 3 {
		ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "test-task")
		require.NoError(t, err)
		require.NoError(t, kh.Put(ctx, *ticket, lymbo.WithGroup(g.ID())))
	}

	count, err := g.PendingCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	terminal, err := g.AllTerminal(ctx)
	require.NoError(t, err)
	assert.False(t, terminal)
}

// TestGroupAllTerminal verifies AllTerminal returns true once every ticket in the group is processed.
func (s *StoreTestSuite) TestGroupAllTerminal(t *testing.T) {
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

	err := router.HandleFunc("group-task", func(ctx context.Context, t *lymbo.Ticket) error {
		return kh.Done(ctx, t.ID) // Done keeps ticket in store with status=done
	})
	require.NoError(t, err)

	g := kh.Group("batch-terminal")
	for range 3 {
		ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "group-task")
		require.NoError(t, err)
		require.NoError(t, kh.Put(ctx, *ticket, lymbo.WithGroup(g.ID())))
	}

	startKharon(t, ctx, kh, router)

	require.Eventually(t, func() bool {
		terminal, err := g.AllTerminal(ctx)
		return err == nil && terminal
	}, 5*time.Second, 10*time.Millisecond, "all group tickets should reach terminal state")

	count, err := g.PendingCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// TestGroupMixedStates verifies PendingCount reflects only the actually-pending tickets.
// One ticket is processed immediately; two are scheduled far in the future (pending but not ready).
func (s *StoreTestSuite) TestGroupMixedStates(t *testing.T) {
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

	processed := atomic.Int32{}
	err := router.HandleFunc("mixed-task", func(ctx context.Context, t *lymbo.Ticket) error {
		processed.Add(1)
		return kh.Done(ctx, t.ID)
	})
	require.NoError(t, err)

	g := kh.Group("batch-mixed")

	// One ticket ready immediately.
	immediate, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "mixed-task")
	require.NoError(t, err)
	require.NoError(t, kh.Put(ctx, *immediate, lymbo.WithGroup(g.ID())))

	// Two tickets scheduled far in the future — pending but not yet polled.
	for range 2 {
		ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "mixed-task")
		require.NoError(t, err)
		require.NoError(t, kh.Put(ctx, *ticket, lymbo.WithGroup(g.ID()), lymbo.WithDelay(lymbo.FixedDelay(1*time.Hour))))
	}

	startKharon(t, ctx, kh, router)

	require.Eventually(t, func() bool {
		return processed.Load() >= 1
	}, 5*time.Second, 10*time.Millisecond, "immediate ticket should be processed")

	// Wait for the pusher to flush the Done update.
	time.Sleep(200 * time.Millisecond)

	count, err := g.PendingCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "two future-scheduled tickets should still be pending")

	terminal, err := g.AllTerminal(ctx)
	require.NoError(t, err)
	assert.False(t, terminal)
}

// TestGroupEmpty verifies that querying a group with no tickets returns zero and AllTerminal true.
func (s *StoreTestSuite) TestGroupEmpty(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	count, err := kh.Group("non-existent-group").PendingCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	terminal, err := kh.Group("non-existent-group").AllTerminal(ctx)
	require.NoError(t, err)
	assert.True(t, terminal, "group with no pending tickets is terminal")
}

// TestGroupUngrouped verifies that tickets without WithGroup are invisible to group queries.
func (s *StoreTestSuite) TestGroupUngrouped(t *testing.T) {
	ctx := context.Background()
	store, cleanup := s.factory(t)
	defer cleanup()

	kh := lymbo.NewKharon(store, lymbo.DefaultSettings(), nil)

	for range 3 {
		ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "test-task")
		require.NoError(t, err)
		require.NoError(t, kh.Put(ctx, *ticket))
	}

	count, err := kh.Group("some-group").PendingCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "ungrouped tickets must not appear in group counts")
}
