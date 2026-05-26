package lymbo_test

import (
	"context"
	"testing"
	"time"

	"github.com/ochaton/lymbo"
)

// startKharon launches kh.Run in a goroutine and registers a t.Cleanup that
// synchronously waits for shutdown (flush + goroutine drain) before the test
// returns. This is mandatory whenever multiple subtests share a backing store
// (e.g. the Postgres factory with TRUNCATE): without it, the next subtest can
// race against the previous Kharon's in-flight transactions and trigger
// SQL-level deadlocks.
func startKharon(tb testing.TB, ctx context.Context, kh *lymbo.Kharon, r *lymbo.Router) {
	tb.Helper()
	go func() { _ = kh.Run(ctx, r) }()
	tb.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := kh.Stop(stopCtx); err != nil {
			tb.Logf("kh.Stop returned %v", err)
		}
	})
}
