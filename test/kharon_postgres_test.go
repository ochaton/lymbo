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
		_, err := pool.Exec(context.Background(), "TRUNCATE tickets")
		require.NoError(t, err)
		return store, func() {}
	}

	suite := NewStoreTestSuite(factory)
	suite.RunAll(t)
}
