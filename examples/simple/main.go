package main

import (
	"context"
	"fmt"
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
	"golang.org/x/time/rate"
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

	kh := lymbo.NewKharon(pg, lymbo.DefaultSettings().WithBatchSize(8).WithWorkers(8), slog.Default())
	router := lymbo.NewRouter()

	registerRoutes(kh, router)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return kh.Run(ctx, router) })

	eg.Go(func() error { return pusher(ctx, kh) })

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

func registerRoutes(kh *lymbo.Kharon, router *lymbo.Router) {
	// TODO: middlewares

	router.HandleFunc("global:ping", func(ctx context.Context, t *lymbo.Ticket) error {
		slog.DebugContext(ctx, "pong",
			slog.String("id", t.ID.String()),
			slog.Int("attempts", t.Attempts),
			slog.Time("ctime", t.Ctime),
			slog.Time("runat", t.Runat),
			slog.String("payload", fmt.Sprintf("%s", t.Payload)),
		)

		if time.Now().UnixMilli()%10 < 5 {
			kh.Retry(ctx, t.ID, lymbo.WithDelay(lymbo.FixedDelay(1*time.Second)))
		}

		return kh.Ack(ctx, t.ID)
	})
}

type PingTicket struct {
	lymbo.Ticket
	Payload struct {
		Num int64 `json:"num"`
	}
}

func getTicket() *lymbo.Ticket {
	tkt, _ := lymbo.NewTicket(
		lymbo.TicketId(uuid.NewString()),
		"global:ping",
	)

	return tkt
}

func pusher(ctx context.Context, kh *lymbo.Kharon) error {
	lim := rate.NewLimiter(rate.Every(1*time.Second), 100)

	// like a wave:
	run := 0

	batches := []int{1, 5, 10}

	for {
		if err := lim.Wait(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		run++
		for i := 0; i < batches[run%len(batches)]; i++ {
			if err := kh.Put(ctx, *getTicket()); err != nil {
				return err
			}
		}
	}
}
