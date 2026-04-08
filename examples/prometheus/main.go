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

	settings := lymbo.DefaultSettings().
		WithWorkers(8).
		WithBatchSize(16).
		WithExpiration().
		WithExpirationInterval(5 * time.Second).
		WithProcessTime(30 * time.Second).
		WithMinReactionDelay(10 * time.Millisecond).
		WithMaxReactionDelay(2 * time.Second)

	kh := lymbo.NewKharon(store, settings, slog.Default())

	router := lymbo.NewRouter()

	// Handler: simulate work with random sleep, then ack
	router.HandleFunc("work", func(ctx context.Context, t *lymbo.Ticket) error {
		sleep := time.Duration(10+rand.IntN(190)) * time.Millisecond
		time.Sleep(sleep)
		return kh.Ack(ctx, t.ID)
	})

	// Handler: sometimes fail, sometimes retry with backoff
	router.HandleFunc("flaky", func(ctx context.Context, t *lymbo.Ticket) error {
		sleep := time.Duration(5+rand.IntN(50)) * time.Millisecond
		time.Sleep(sleep)

		switch r := rand.IntN(10); {
		case r < 6:
			return kh.Ack(ctx, t.ID)
		case r < 8:
			return kh.Retry(ctx, t.ID,
				lymbo.WithDelay(lymbo.BackoffDelay(1.5, 10*time.Second, 0)))
		default:
			return kh.Fail(ctx, t.ID, lymbo.WithErrorReason("random failure"))
		}
	})

	router.NotFoundFunc(func(ctx context.Context, t *lymbo.Ticket) error {
		return kh.Fail(ctx, t.ID, lymbo.WithErrorReason("unknown ticket type"))
	})

	// Register prometheus collector
	prometheus.MustRegister(lymbo_prom.NewCollector(kh, nil))

	// Start kharon
	go func() {
		if err := kh.Run(ctx, router); err != nil {
			slog.Error("kharon exited", "error", err)
		}
	}()

	// Background ticket pusher: ~50 tickets/sec, mix of "work" and "flaky"
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		types := []string{"work", "work", "work", "flaky"}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
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
		slog.Info("starting HTTP server", "addr", addr)
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
