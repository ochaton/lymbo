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

// postgresStoreFactory creates a PostgreSQL store with testcontainers for testing
func postgresStoreFactory(t *testing.T) (lymbo.Store, func()) {
	t.Helper()

	// Create a context with extended timeout for container setup
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer setupCancel()

	// Start PostgreSQL container
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

	// Get connection string
	connStr, err := container.ConnectionString(setupCtx, "sslmode=disable")
	require.NoError(t, err)

	// Create connection pool
	pool, err := pgxpool.New(setupCtx, connStr)
	require.NoError(t, err)

	// Create store and run migrations
	store := postgres.NewTicketsRepository(pool)
	err = store.Migrate(setupCtx)
	require.NoError(t, err)

	// Return store and cleanup function
	cleanup := func() {
		pool.Close()
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}

	return store, cleanup
}

// TestPostgresStore runs the full test suite against the PostgreSQL store
func TestPostgresStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in short mode")
	}

	suite := NewStoreTestSuite(postgresStoreFactory)
	suite.RunAll(t)
}
