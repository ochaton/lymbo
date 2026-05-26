package lymbo_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/store/memory"
	"golang.org/x/time/rate"
)

func BenchmarkMemoryThroughput(b *testing.B) {
	ctx := b.Context()

	store := memory.NewStore()
	settings := lymbo.DefaultSettings().
		WithMinReactionDelay(100 * time.Microsecond).
		WithMaxReactionDelay(5 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)

	var processed atomic.Int64

	router := lymbo.NewRouter()
	router.HandleFunc("bench", func(ctx context.Context, t *lymbo.Ticket) error {
		time.Sleep(time.Nanosecond)
		processed.Add(1)
		return kh.Ack(ctx, t.ID)
	})

	startKharon(b, ctx, kh, router)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ticket, _ := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "bench")
		if err := kh.Put(ctx, *ticket); err != nil {
			b.Fatal(err)
		}
	}

	// wait for all tickets to be processed
	deadline := time.Now().Add(30 * time.Second)
	for processed.Load() < int64(b.N) {
		if time.Now().After(deadline) {
			b.Fatalf("timeout: processed %d / %d", processed.Load(), b.N)
		}
		time.Sleep(time.Millisecond)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tickets/sec")
}

func BenchmarkMemoryQueueWait(b *testing.B) {
	const putRatePerSec = 20_000

	ctx := b.Context()

	store := memory.NewStore()
	settings := lymbo.DefaultSettings().
		WithMinReactionDelay(100 * time.Microsecond).
		WithMaxReactionDelay(5 * time.Millisecond)
	kh := lymbo.NewKharon(store, settings, nil)

	var (
		processed atomic.Int64
		totalWait atomic.Int64 // nanoseconds
	)

	router := lymbo.NewRouter()
	router.HandleFunc("bench-wait", func(ctx context.Context, t *lymbo.Ticket) error {
		time.Sleep(time.Nanosecond)
		totalWait.Add(time.Since(t.ReadyAt).Nanoseconds())
		processed.Add(1)
		return kh.Ack(ctx, t.ID)
	})

	startKharon(b, ctx, kh, router)

	rl := rate.NewLimiter(putRatePerSec, putRatePerSec)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := rl.Wait(ctx); err != nil {
			b.Fatal(err)
		}
		ticket, _ := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "bench-wait")
		if err := kh.Put(ctx, *ticket); err != nil {
			b.Fatal(err)
		}
	}

	deadline := time.Now().Add(30 * time.Second)
	for processed.Load() < int64(b.N) {
		if time.Now().After(deadline) {
			b.Fatalf("timeout: processed %d / %d", processed.Load(), b.N)
		}
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()

	avgWaitNs := totalWait.Load() / int64(b.N)
	b.ReportMetric(float64(avgWaitNs), "ns/wait")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tickets/sec")
}
