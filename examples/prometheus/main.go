package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ochaton/lymbo"
	lymbo_prom "github.com/ochaton/lymbo/prometheus"
	"github.com/ochaton/lymbo/store/postgres"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable&pool_max_conns=16"
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	store, err := postgres.NewTicketsRepositoryWithConfig(postgres.Config{
		Pool:      pool,
		TableName: "kharon_prom_tickets",
	})
	if err != nil {
		slog.Error("failed to create store", "error", err)
		os.Exit(1)
	}
	if err := store.Migrate(ctx); err != nil {
		slog.Error("failed to migrate", "error", err)
		os.Exit(1)
	}

	hostname, _ := os.Hostname()

	settings := lymbo.DefaultSettings().
		WithWorkers(32).
		WithBatchSize(64).
		WithExpiration().
		WithExpirationInterval(10 * time.Second).
		WithProcessTime(30 * time.Second).
		WithMinReactionDelay(200 * time.Millisecond).
		WithMaxReactionDelay(5 * time.Second)

	kh := lymbo.NewKharon(store, settings, slog.Default())

	slowSettings := lymbo.DefaultSettings().
		WithWorkers(4).
		WithBatchSize(8).
		WithExpiration().
		WithExpirationInterval(5 * time.Second).
		WithProcessTime(60 * time.Second).
		WithMinReactionDelay(100 * time.Millisecond).
		WithMaxReactionDelay(5 * time.Second).
		WithOnlyTubes(lymbo.Tube("slow"))

	khSlow := lymbo.NewKharon(store, slowSettings, slog.Default())

	router := lymbo.NewRouter()

	// Handler: simulate work with random sleep 100ms-2s, then ack
	router.HandleFunc("work", func(ctx context.Context, t *lymbo.Ticket) error {
		sleep := time.Duration(100+rand.IntN(1900)) * time.Millisecond
		time.Sleep(sleep)
		return kh.Ack(ctx, t.ID)
	})

	// Handler: flaky, long processing 1-25s, sometimes fail/retry
	// On retry, moves ticket to "slow" tube
	router.HandleFunc("flaky", func(ctx context.Context, t *lymbo.Ticket) error {
		sleep := time.Duration(1+rand.IntN(24)) * time.Second
		time.Sleep(sleep)

		switch r := rand.IntN(10); {
		case r < 6:
			return kh.Ack(ctx, t.ID)
		case r < 8:
			return kh.Retry(ctx, t.ID,
				lymbo.WithTube(lymbo.Tube("slow")),
				lymbo.WithDelay(lymbo.BackoffDelay(1.5, 10*time.Second, 0)))
		default:
			return kh.Fail(ctx, t.ID,
				lymbo.WithErrorReason("random failure"),
				lymbo.WithDelay(lymbo.FixedDelay(5*time.Second)),
			)
		}
	})

	router.NotFoundFunc(func(ctx context.Context, t *lymbo.Ticket) error {
		return kh.Fail(ctx, t.ID, lymbo.WithErrorReason("unknown ticket type"))
	})

	// Register prometheus collectors with hostname + kharon instance label
	prometheus.MustRegister(
		lymbo_prom.NewCollector(kh, prometheus.Labels{"hostname": hostname, "kharon": "default"}),
		lymbo_prom.NewCollector(khSlow, prometheus.Labels{"hostname": hostname, "kharon": "slow"}),
	)

	// Start kharon instances
	go func() {
		if err := kh.Run(ctx, router); err != nil {
			slog.Error("kharon default exited", "error", err)
		}
	}()
	go func() {
		if err := khSlow.Run(ctx, router); err != nil {
			slog.Error("kharon slow exited", "error", err)
		}
	}()

	// Background ticket pusher: ~100 tickets/min (random interval 200ms-1.4s)
	go func() {
		for {
			// Random sleep: average ~600ms → ~100/min
			sleep := time.Duration(200+rand.IntN(1200)) * time.Millisecond
			select {
			case <-ctx.Done():
				return
			case <-time.After(sleep):
			}

			types := []string{"work", "work", "work", "flaky"}
			typ := types[rand.IntN(len(types))]
			tkt, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), typ)
			if err != nil {
				slog.Error("failed to create ticket", "error", err)
				continue
			}
			if err := kh.Put(ctx, *tkt); err != nil {
				slog.Error("failed to put ticket", "error", err)
			}
		}
	}()

	// HTTP server with /metrics
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	addr := ":8080"
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		addr = v
	}

	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		slog.Info("starting HTTP server", "addr", addr, "hostname", hostname)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	slog.Info("shutting down")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)
}
