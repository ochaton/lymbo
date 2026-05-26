package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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

	def := lymbo.NewKharon(pg, lymbo.DefaultSettings(), slog.Default())
	router := lymbo.NewRouter()

	eg, ctx := errgroup.WithContext(ctx)

	pubsub := lymbo.NewKharon(pg, lymbo.DefaultSettings().EnableTubes(), slog.Default())
	if err := def.Put(ctx, *lymbo.MustTubeTicket("tube-a", lymbo.MustID(), "type-a")); err != nil {
		panic(err)
	}

	m := &Manager{k: pubsub}
	eg.Go(func() error { return m.Run(ctx) })

	// Start pubsub
	// Yeah, we just fire and forget it
	eg.Go(func() error { return pubsub.Run(ctx, router) })

	eg.Go(func() error {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		sig := <-c
		slog.InfoContext(ctx, "received signal", slog.String("signal", sig.String()))
		cancel()
		return nil
	})

	if err := eg.Wait(); err != nil {
		slog.ErrorContext(ctx, "failed", slog.Any("err", err))
	}
}

type Manager struct {
	k *lymbo.Kharon
}

// TODO: drive Subscribe/Unsubscribe and route handlers; sketch only.
func (m *Manager) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
