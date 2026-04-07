package common

import (
	"context"
	"log/slog"
	"os"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func InitPgPool(ctx context.Context) *pgxpool.Pool {
	pgconf, err := pgxpool.ParseConfig(
		"postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable&pool_max_conns=32",
	)

	pgconf.ConnConfig.Tracer = &tracer{}
	pool, err := pgxpool.NewWithConfig(ctx, pgconf)
	if err != nil {
		panic(err)
	}
	return pool
}

type tracer struct {
}

// traceCtxKey is used to store trace data in context.
type traceCtxKey struct{}

type traceData struct {
	startTime time.Time
	sql       string
	args      []any
}

func (t *tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, traceCtxKey{}, traceData{
		startTime: time.Now(),
		sql:       data.SQL,
		args:      data.Args,
	})
}

var cmdRegexp = regexp.MustCompile(`^-- name: (\S+):\n`)

func (t *tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	td, ok := ctx.Value(traceCtxKey{}).(traceData)
	if !ok {
		return
	}

	duration := time.Since(td.startTime)

	var tag string
	if sub := cmdRegexp.FindStringSubmatch(td.sql); len(sub) > 1 {
		tag = sub[1]
	}

	nrows := data.CommandTag.RowsAffected()
	slog.InfoContext(ctx, "sql",
		slog.String("sql", tag),
		slog.Duration("dur", duration),
		slog.Int64("nrows", nrows),
	)
}

func InitLogger() {
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				a.Value = slog.StringValue(a.Value.Time().Format("2006-01-02T15:04:05.000"))
				return slog.Attr{Key: a.Key, Value: a.Value}
			default:
				return a
			}
		},
	})
	slog.SetDefault(slog.New(h))
}
