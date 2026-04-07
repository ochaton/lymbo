package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/examples/common"
	"github.com/ochaton/lymbo/store/postgres"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	pool := common.InitPgPool(ctx)
	defer pool.Close()

	common.InitLogger()

	pg, err := postgres.NewTicketsRepositoryWithConfig(postgres.Config{
		TableName: "kharon_tickets",
		Pool:      pool,
	})
	if err != nil {
		panic(err)
	}
	if err := pg.Migrate(ctx); err != nil {
		panic(err)
	}

	khPinger := lymbo.NewKharon(pg,
		lymbo.DefaultSettings().
			WithBatchSize(8).
			WithWorkers(8).
			WithOnlyTubes(lymbo.Tube("ping")),
		slog.Default(),
	)

	khPonger := lymbo.NewKharon(pg,
		lymbo.DefaultSettings().
			WithBatchSize(8).
			WithWorkers(8).
			WithOnlyTubes(lymbo.Tube("pong")),
		slog.Default(),
	)

	router := lymbo.NewRouter()

	registerRoutes(khPinger, router)
	registerRoutes(khPonger, router)

	eg, ctx := errgroup.WithContext(ctx)

	// Start pinger
	eg.Go(func() error { return khPinger.Run(ctx, router) })
	// Start ponger
	eg.Go(func() error { return khPonger.Run(ctx, router) })

	// Start arbitrator
	eg.Go(func() error {
		tkt, err := lymbo.NewTubeTicket(
			lymbo.Tube("ping"),
			lymbo.TicketId(uuid.NewString()),
			"ball",
		)
		if err != nil {
			return err
		}

		return khPinger.Put(ctx, *tkt, lymbo.WithPayload(Ball{"start"}))
	})

	eg.Go(func() error {
		defer cancel()
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		sig := <-c
		slog.InfoContext(ctx, "received signal", slog.String("signal", sig.String()))
		return nil
	})

	if err := eg.Wait(); err != nil {
		slog.ErrorContext(ctx, "failed", slog.Any("err", err))
	}
}

type Ball struct {
	LastMessage string `json:"last_message"`
}

func registerRoutes(kh *lymbo.Kharon, router *lymbo.Router) {
	router.HandleFunc("ball", func(ctx context.Context, t *lymbo.Ticket) error {
		var ball Ball
		if err := json.Unmarshal(t.Payload, &ball); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal payload", slog.Any("err", err))
			return kh.Fail(ctx, t.ID, lymbo.WithErrorReason("invalid payload: "+err.Error()))
		}
		slog.InfoContext(ctx, "-> ball",
			slog.String("tube", t.Tube.String()),
			slog.String("payload", ball.LastMessage),
			slog.Int("attempts", t.Attempts),
		)

		opts := []lymbo.Option{
			lymbo.WithDelay(lymbo.FixedDelay(time.Second)),
		}

		switch t.Tube {
		case "ping":
			slog.InfoContext(ctx, "ping received")
			opts = append(opts, lymbo.WithTube(lymbo.Tube("pong")),
				lymbo.WithPayload(Ball{"msg:ping"}))
		case "pong":
			slog.InfoContext(ctx, "pong received")
			opts = append(opts, lymbo.WithTube(lymbo.Tube("ping")),
				lymbo.WithPayload(Ball{"msg:pong"}))
		default:
			slog.WarnContext(ctx, "unknown tube", slog.String("tube", t.Tube.String()))
			return kh.Fail(ctx, t.ID, lymbo.WithErrorReason("unknown tube"))
		}

		if t.Attempts >= 5 {
			return kh.Ack(ctx, t.ID, lymbo.WithKeep())
		}

		return kh.Retry(ctx, t.ID, opts...)
	})
}
