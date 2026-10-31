package lymbo_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/store/postgres"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestPostgresStore runs the full test suite against the PostgreSQL store
func TestPostgresStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in short mode")
	}

	// Start PostgreSQL container once for all subtests
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
	err = store.Migrate(setupCtx)
	require.NoError(t, err)

	// Factory reuses the shared pool; truncates the table for test isolation
	factory := func(t *testing.T) (lymbo.Store, func()) {
		t.Helper()
		_, err := pool.Exec(context.Background(), "TRUNCATE tickets CASCADE")
		require.NoError(t, err)
		return store, func() {}
	}

	suite := NewStoreTestSuite(factory)
	suite.RunAll(t)

	finalizerSuite := NewFinalizerTestSuite(factory)
	finalizerSuite.RunAll(t)

	concurrentSuite := NewConcurrentTestSuite(factory)
	concurrentSuite.RunAll(t)

	t.Run("PollPendingOrderUnderSeqScan", func(t *testing.T) {
		testPollPendingOrderUnderSeqScan(t, connStr)
	})
}

// testPollPendingOrderUnderSeqScan pins the PollPending contract — due tickets
// come out by Runat, then Nice — for the plan Postgres picks once autoanalyze
// has seen that the table is small: a sequential scan, which yields rows in heap
// order. Disabling index scans for the session makes that plan deterministic.
// Without the ordering, a due ticket early in the heap wins every poll and the
// tickets behind it starve for as long as it stays due.
func testPollPendingOrderUnderSeqScan(t *testing.T, connStr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(connStr)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["enable_indexscan"] = "off"
	cfg.ConnConfig.RuntimeParams["enable_indexonlyscan"] = "off"
	cfg.ConnConfig.RuntimeParams["enable_bitmapscan"] = "off"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	store, err := postgres.NewTicketsRepositoryWithConfig(postgres.Config{
		TableName: "tickets_seqscan",
		Pool:      pool,
	})
	require.NoError(t, err)
	require.NoError(t, store.Migrate(ctx))

	now := time.Now()
	// Inserted in this order, so the heap order is the order of this slice.
	put := []struct {
		id    string
		runat time.Time
		nice  int
	}{
		{"due-last", now.Add(-1 * time.Second), 0},
		{"due-first", now.Add(-3 * time.Second), 512},
		{"due-second-low-priority", now.Add(-2 * time.Second), 900},
		{"due-second-high-priority", now.Add(-2 * time.Second), 100},
	}
	ids := make(map[lymbo.TicketId]string, len(put))
	for _, p := range put {
		ticket, err := lymbo.NewTicket(lymbo.MustID(), "order-task")
		require.NoError(t, err)
		ticket.Runat = p.runat
		ticket.Nice = p.nice
		require.NoError(t, store.Put(ctx, *ticket))
		ids[ticket.ID] = p.id
	}

	res, err := store.PollPending(ctx, lymbo.PollRequest{
		Limit:           3,
		Now:             now,
		TTR:             time.Minute,
		BackoffBase:     2,
		MaxBackoffDelay: time.Minute,
		RequestTubes:    []lymbo.Tube{lymbo.DefaultTube},
	})
	require.NoError(t, err)

	got := make([]string, 0, len(res.Tickets))
	for _, tk := range res.Tickets {
		got = append(got, ids[tk.ID])
	}
	require.Equal(t, []string{"due-first", "due-second-high-priority", "due-second-low-priority"}, got)
}
