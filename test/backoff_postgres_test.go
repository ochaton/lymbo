package lymbo_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/store/postgres"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestBug_PostgresBackoffPowerOverflow reproduces the float8 overflow in the
// backoff reschedule query:
//
// POWER(base, attempts) was evaluated in float8 BEFORE LEAST clamped it to
// maxDelay, so once attempts >= ~1751 (base 1.5) the UPDATE failed with
// SQLSTATE 22003 (value out of range: overflow). Because UpdateBatch sends one
// pgx.Batch — a single implicit transaction — the poisoned ticket also
// discarded every sibling update in the same flush, and since its runat never
// advanced it stayed permanently ready: re-polled, re-failed, attempts
// climbing, with no way to recover.
func TestBug_PostgresBackoffPowerOverflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in short mode")
	}

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer setupCancel()

	container, err := pgcontainer.Run(setupCtx,
		"postgres:16-alpine",
		pgcontainer.WithDatabase("testdb"),
		pgcontainer.WithUsername("test"),
		pgcontainer.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	})

	connStr, err := container.ConnectionString(setupCtx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(setupCtx, connStr)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	store := postgres.NewTicketsRepository(pool)
	require.NoError(t, store.Migrate(setupCtx))

	ctx := context.Background()

	// A sibling ticket whose update must survive sharing a batch with the
	// poisoned one.
	sibling, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, *sibling))

	poisoned, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "worker")
	require.NoError(t, err)
	poisoned.Attempts = 2000 // past the float8 overflow threshold (~1751 for base 1.5)
	require.NoError(t, store.Put(ctx, *poisoned))

	const maxDelay = 30 * time.Second
	siblingRunat := time.Now().Add(5 * time.Minute).UTC().Truncate(time.Millisecond)

	before := time.Now()
	_, err = store.UpdateBatch(ctx, []lymbo.UpdateSet{
		{Id: poisoned.ID, Backoff: &lymbo.DelayBackoff{Base: 1.5, MaxDelay: maxDelay}},
		{Id: sibling.ID, Runat: &siblingRunat},
	})
	require.NoError(t, err, "BUG: backoff reschedule overflowed float8")

	got, err := store.Get(ctx, poisoned.ID)
	require.NoError(t, err)
	require.False(t, got.Runat.Before(before), "BUG: poisoned ticket runat did not advance: %v", got.Runat)
	require.LessOrEqual(t, got.Runat.Sub(before), maxDelay+time.Second, "BUG: backoff delay not capped at maxDelay")

	gotSibling, err := store.Get(ctx, sibling.ID)
	require.NoError(t, err)
	require.True(t, gotSibling.Runat.Equal(siblingRunat),
		"BUG: sibling update lost in the poisoned batch: want %v, got %v", siblingRunat, gotSibling.Runat)
}
