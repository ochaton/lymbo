package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/store/memory"
	"github.com/ochaton/lymbo/store/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	ctx := context.Background()

	logger := setupLogger()
	settings := lymbo.DefaultSettings().
		WithExpiration().
		WithExpirationInterval(1 * time.Second).
		WithProcessTime(100 * time.Microsecond).
		WithWorkers(32).
		WithBatchSize(64).
		WithMinReactionDelay(500 * time.Microsecond).
		WithMaxReactionDelay(1 * time.Second)

	var kh *lymbo.Kharon
	dbtype := os.Getenv("DB_TYPE")
	if dbtype == "" {
		dbtype = "memory"
	}
	switch dbtype {
	case "memory":
		slog.InfoContext(ctx, "using in-memory storage")
		kh = lymbo.NewKharon(memory.NewStore(), settings, logger)
	default:
		cf, err := pgxpool.ParseConfig(os.Getenv("DB_DSN"))
		cf.MaxConns = 32
		if err != nil {
			slog.ErrorContext(ctx, "failed to connect to pgpool", "error", err)
			os.Exit(1)
		}
		cf.HealthCheckPeriod = 300 * time.Millisecond
		pool, err := pgxpool.NewWithConfig(ctx, cf)
		if err != nil {
			slog.ErrorContext(ctx, "failed to connect to pgpool", "error", err)
			os.Exit(1)
		}
		store := postgres.NewTicketsRepository(pool)
		if err := store.Migrate(ctx); err != nil {
			slog.ErrorContext(ctx, "failed to migrate database", "error", err)
			os.Exit(1)
		}
		slog.InfoContext(ctx, "migration complete")
		kh = lymbo.NewKharon(store, settings, logger)
	}

	r := lymbo.NewRouter()
	r.HandleFunc("ack", func(ctx context.Context, t *lymbo.Ticket) error {
		return kh.Ack(ctx, t.ID)
	})
	r.HandleFunc("fail", func(ctx context.Context, t *lymbo.Ticket) error {
		return kh.Fail(ctx, t.ID, lymbo.WithErrorReason("failed by handler"), lymbo.WithDelay(10*time.Second))
	})
	r.HandleFunc("done", func(ctx context.Context, t *lymbo.Ticket) error {
		return kh.Done(ctx, t.ID)
	})
	r.HandleFunc("cancel", func(ctx context.Context, t *lymbo.Ticket) error {
		return kh.Cancel(ctx, t.ID)
	})
	r.NotFoundFunc(func(ctx context.Context, t *lymbo.Ticket) error {
		slog.DebugContext(ctx, "unknown ticket type", "ticket_id", t.ID, "ticket_type", t.Type)
		return kh.Fail(ctx, t.ID, lymbo.WithErrorReason("unsupported ticket type"), lymbo.WithDelay(30*time.Second))
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		if err := kh.Run(ctx, r); err != nil {
			slog.ErrorContext(ctx, "kharon failed", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		var prev lymbo.Stats
		ts := time.Now()
		for {
			select {
			case <-ticker.C:
				stats := kh.Stats()
				slog.InfoContext(ctx, "kharon stats",
					"scheduled", stats.Scheduled,
					"polled", stats.Polled,
					"processed", stats.Processed,
					"failed", stats.Failed,
					"expired", stats.Expired,
				)
				// Evaluate rates
				interval := time.Since(ts).Seconds()
				if interval > 0 {
					slog.InfoContext(ctx, "kharon rates",
						"schedule_rate", hRate(float64(stats.Scheduled-prev.Scheduled)/interval),
						// "add_rate", hRate(float64(stats.Added-prev.Added)/interval),
						"poll_rate", hRate(float64(stats.Polled-prev.Polled)/interval),
						"ack_rate", hRate(float64(stats.Acked-prev.Acked)/interval),
						// "done_rate", hRate(float64(stats.Done-prev.Done)/interval),
						// "cancel_rate", hRate(float64(stats.Canceled-prev.Canceled)/interval),
						"fail_rate", hRate(float64(stats.Failed-prev.Failed)/interval),
						"expire_rate", hRate(float64(stats.Expired-prev.Expired)/interval),
					)
				}
				ts = time.Now()
				prev = stats
			case <-ctx.Done():
				return
			}
		}
	}()

	mux := http.NewServeMux()

	hs := &Handlers{
		GetTicket:    &GetTicketHandler{kh},
		CancelTicket: &CancelTicketHandler{kh},
		DeleteTicket: &DeleteTicketHandler{kh},
		AddTicket:    &AddTicketHandler{kh},
		ResetStats:   &ResetStatsHandler{kh},
	}
	hs.Register(mux)

	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: mux,
	}

	runServer(ctx, server, cancel)
}

type Handlers struct {
	GetTicket    *GetTicketHandler
	CancelTicket *CancelTicketHandler
	DeleteTicket *DeleteTicketHandler
	AddTicket    *AddTicketHandler
	ResetStats   *ResetStatsHandler
}

func (h *Handlers) Register(mux *http.ServeMux) {
	mux.Handle(h.GetTicket.Handle(), h.WithMiddleware(h.GetTicket))
	mux.Handle(h.CancelTicket.Handle(), h.WithMiddleware(h.CancelTicket))
	mux.Handle(h.DeleteTicket.Handle(), h.WithMiddleware(h.DeleteTicket))
	mux.Handle(h.AddTicket.Handle(), h.WithMiddleware(h.AddTicket))
	mux.Handle(h.ResetStats.Handle(), h.WithMiddleware(h.ResetStats))
}

type ResponseWriterWithStatus struct {
	http.ResponseWriter
	code int
}

func (w *ResponseWriterWithStatus) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (h *Handlers) WithMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		s := time.Now()
		slog.InfoContext(ctx, "[START]",
			"http", r.Method, "path", r.URL.Path, "remote_addr", r.RemoteAddr,
			"user_agent", r.UserAgent(),
		)
		wr := &ResponseWriterWithStatus{ResponseWriter: w}
		next.ServeHTTP(wr, r)
		status := wr.code
		if status == 0 {
			status = http.StatusOK
		}
		slog.InfoContext(ctx, fmt.Sprintf("[END=%d]", status),
			"T", time.Since(s).Round(100*time.Microsecond).Seconds(),
			"http", r.Method, "path", r.URL.Path, "remote_addr", r.RemoteAddr,
			"user_agent", r.UserAgent(),
		)
	})
}

type GetTicketHandler struct {
	kh *lymbo.Kharon
}

func (h *GetTicketHandler) Handle() string {
	return "GET /ticket/{id}"
}

func (h *GetTicketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "ticket ID is required", http.StatusBadRequest)
		return
	}

	ticket, err := h.kh.Get(r.Context(), lymbo.TicketId(id))
	switch err {
	case nil:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(ticket); err != nil {
			slog.ErrorContext(ctx, "failed to encode ticket", "error", err)
			http.Error(w, "failed to encode ticket", http.StatusInternalServerError)
		}
	case lymbo.ErrTicketNotFound:
		http.Error(w, "ticket not found", http.StatusNotFound)
	case lymbo.ErrTicketIDInvalid:
		http.Error(w, "invalid ticket ID", http.StatusBadRequest)
	default:
		slog.ErrorContext(ctx, "failed to get ticket", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

type CancelTicketHandler struct {
	kh *lymbo.Kharon
}

func (h *CancelTicketHandler) Handle() string {
	return "POST /ticket/{id}/cancel"
}

func (h *CancelTicketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "ticket ID is required", http.StatusBadRequest)
		return
	}

	err := h.kh.Cancel(r.Context(), lymbo.TicketId(id), lymbo.WithErrorReason("cancelled via api"))
	switch err {
	case nil:
		w.WriteHeader(http.StatusNoContent)
	case lymbo.ErrTicketNotFound:
		http.Error(w, "ticket not found", http.StatusNotFound)
	default:
		slog.ErrorContext(ctx, "failed to cancel ticket", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

type DeleteTicketHandler struct {
	kh *lymbo.Kharon
}

func (h *DeleteTicketHandler) Handle() string {
	return "DELETE /ticket/{id}"
}

func (h *DeleteTicketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "ticket ID is required", http.StatusBadRequest)
		return
	}

	err := h.kh.Delete(r.Context(), lymbo.TicketId(id))
	switch err {
	case nil, lymbo.ErrTicketNotFound:
		w.WriteHeader(http.StatusNoContent)
	default:
		slog.ErrorContext(ctx, "failed to delete ticket", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

type AddTicketHandler struct {
	kh *lymbo.Kharon
}

func (h *AddTicketHandler) Handle() string {
	return "POST /ticket/{type}/{id}"
}

func (h *AddTicketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "ticket ID is required", http.StatusBadRequest)
		return
	}

	typ := r.PathValue("type")
	if typ == "" {
		http.Error(w, "ticket type is required", http.StatusBadRequest)
		return
	}

	opts := []lymbo.Option{}
	{
		if n := r.URL.Query().Get("nice"); n != "" {
			nx, err := strconv.Atoi(n)
			if err != nil {
				slog.WarnContext(ctx, "invalid nice given", "nice", n)
				http.Error(w, "invalid parameters", http.StatusBadRequest)
				return
			}
			opts = append(opts, lymbo.WithNice(nx))
		}

		if d := r.URL.Query().Get("delay"); d != "" {
			dx, err := strconv.ParseFloat(d, 64)
			if err != nil {
				slog.WarnContext(ctx, "invalid delay given", "delay", d)
				http.Error(w, "invalid parameters", http.StatusBadRequest)
				return
			}
			if dx < 0 {
				slog.WarnContext(ctx, "negative delay given", "delay", d)
				http.Error(w, "invalid parameters", http.StatusBadRequest)
				return
			}
			opts = append(opts, lymbo.WithDelay(time.Duration(dx*float64(time.Second))))
		}
	}

	payload, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	ticket, err := lymbo.NewTicket(lymbo.TicketId(id), typ)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create ticket", "error", err)
		http.Error(w, "failed to create ticket", http.StatusInternalServerError)
		return
	}

	if len(payload) > 0 {
		ticket = ticket.WithPayload(payload)
	}

	err = h.kh.Put(r.Context(), *ticket, opts...)
	switch err {
	case nil:
		break
	case lymbo.ErrTicketIDInvalid:
		slog.ErrorContext(ctx, "invalid ticket id", "error", err, "id", id)
		http.Error(w, "invalid ticket ID", http.StatusBadRequest)
		return
	default:
		slog.ErrorContext(ctx, "failed to add ticket", "error", err)
		http.Error(w, "failed to add ticket", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Location", "http://"+r.Host+"/ticket/"+id)
	w.WriteHeader(http.StatusAccepted)
}

func runServer(ctx context.Context, server *http.Server, cancel context.CancelFunc) {
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	serverErr := make(chan error, 1)
	go func() {
		slog.InfoContext(ctx, "starting HTTP server", "addr", server.Addr)
		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			slog.Error("server error", "error", err)
		}
	case sig := <-shutdown:
		slog.InfoContext(ctx, "Shutdown signal received", slog.String("signal", sig.String()))
		cancel()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.ErrorContext(ctx, "HTTP server shutdown error", slog.Any("error", err))
			_ = server.Close()
		}

		slog.InfoContext(ctx, "HTTP server stopped gracefully")
	}
}

type ResetStatsHandler struct {
	kh *lymbo.Kharon
}

func (h *ResetStatsHandler) Handle() string {
	return "POST /stats/reset"
}

func (h *ResetStatsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.kh.ResetStats()
	w.WriteHeader(http.StatusNoContent)
}

func setupLogger() *slog.Logger {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))
	slog.SetDefault(logger)
	return logger
}

func hRate(r float64) string {
	if r > 1_000_000 {
		return fmt.Sprintf("%.2fM/s", r/1_000_000)
	}
	if r > 1000 {
		return fmt.Sprintf("%.2fk/s", r/1000)
	}
	return fmt.Sprintf("%.2f/s", r)
}
