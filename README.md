# Kharon

A Go library for delayed task processing and state reconciliation.

## Table of Contents

- [Kharon](#kharon)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Installation](#installation)
  - [Quick Start](#quick-start)
  - [Usage](#usage)
    - [Creating Tickets](#creating-tickets)
    - [Handling Tickets](#handling-tickets)
    - [Ticket Lifecycle](#ticket-lifecycle)
    - [Options](#options)
    - [Delay Strategies](#delay-strategies)
    - [Groups](#groups)
  - [Configuration](#configuration)
  - [Storage](#storage)
    - [In-Memory](#in-memory)
    - [PostgreSQL](#postgresql)
    - [Custom Store](#custom-store)
  - [Prometheus Metrics](#prometheus-metrics)
  - [Examples](#examples)
  - [License](#license)

## Features

- **Pluggable Storage**: In-memory and PostgreSQL backends
- **Priority Scheduling**: Nice values (lower = higher priority)
- **Retry Strategies**: Fixed delays or exponential backoff
- **Tubes**: Route tickets to separate queues
- **Groups**: Track batches of related tickets and query their collective progress
- **Automatic Expiration**: Cleanup of completed/expired tickets
- **Concurrent Processing**: Configurable worker pools
- **Prometheus Metrics**: Per-type and per-tube counters, processing duration and queue wait histograms

## Installation

```bash
go get github.com/ochaton/lymbo
```

## Quick Start

```go
package main

import (
    "context"
    "log/slog"

    "github.com/google/uuid"
    "github.com/ochaton/lymbo"
    "github.com/ochaton/lymbo/store/memory"
)

func main() {
    ctx := context.Background()

    kh := lymbo.NewKharon(memory.NewStore(), lymbo.DefaultSettings(), slog.Default())

    router := lymbo.NewRouter()
    router.HandleFunc("example", func(ctx context.Context, t *lymbo.Ticket) error {
        slog.Info("processing", "id", t.ID, "payload", t.Payload)
        return kh.Ack(ctx, t.ID)
    })

    go kh.Run(ctx, router)

    ticket, _ := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "example")
    kh.Put(ctx, *ticket)
}
```

## Usage

### Creating Tickets

```go
ticket, err := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "task-type")

// With payload (auto-marshaled to JSON)
ticket.WithPayload(map[string]any{"key": "value"})

// With priority and delayed execution
ticket.WithNice(5).WithRunat(time.Now().Add(time.Hour))

// With tube routing
ticket, err := lymbo.NewTubeTicket("emails", lymbo.TicketId(uuid.NewString()), "send-email")

// Put with options
kh.Put(ctx, *ticket,
    lymbo.WithDelay(lymbo.FixedDelay(5*time.Minute)),
    lymbo.WithNice(10),
)
```

### Handling Tickets

```go
router := lymbo.NewRouter()

router.HandleFunc("email", func(ctx context.Context, t *lymbo.Ticket) error {
    if err := sendEmail(t.Payload); err != nil {
        if isTransient(err) {
            return kh.Retry(ctx, t.ID, lymbo.WithDelay(lymbo.FixedDelay(5*time.Minute)))
        }
        return kh.Fail(ctx, t.ID, lymbo.WithErrorReason(err.Error()))
    }
    return kh.Ack(ctx, t.ID)
})

router.NotFoundFunc(func(ctx context.Context, t *lymbo.Ticket) error {
    return kh.Fail(ctx, t.ID, lymbo.WithErrorReason("unsupported type"))
})
```

### Ticket Lifecycle

| Method   | Default Behavior             | Notes                               |
| -------- | ---------------------------- | ----------------------------------- |
| `Ack`    | Removes ticket               | Use `WithKeep()` to retain          |
| `Done`   | Keeps ticket (status=done)   | Equivalent to `Ack` + `WithKeep()`  |
| `Fail`   | Keeps ticket (status=failed) | Use `WithErrorReason()` for details |
| `Cancel` | Removes ticket               | Use `WithKeep()` to retain          |
| `Retry`  | Reschedules as pending       | Increments attempts counter         |
| `Delete` | Removes ticket               | Permanent removal                   |

All methods accept options:

```go
// Retry with exponential backoff
kh.Retry(ctx, id, lymbo.WithDelay(lymbo.BackoffDelay(1.5, 15*time.Second, 0)))

// Done with TTL (auto-removed after 24h)
kh.Done(ctx, id, lymbo.WithDelay(lymbo.FixedDelay(24*time.Hour)))

// Fail with error reason
kh.Fail(ctx, id,
    lymbo.WithErrorReason("connection timeout"),
    lymbo.WithDelay(lymbo.FixedDelay(7*24*time.Hour)),
)

// Cancel but keep for audit
kh.Cancel(ctx, id, lymbo.WithKeep(), lymbo.WithErrorReason("cancelled by user"))
```

### Options

| Option | Description |
| ------ | ----------- |
| `WithDelay(DelayStrategy)` | Set processing delay or TTL for auto-removal |
| `WithNice(n)` | Set priority (lower = higher) |
| `WithKeep()` | Keep ticket in store instead of removing |
| `WithErrorReason(reason)` | Store error/cancellation reason |
| `WithPayload(v)` | Set ticket payload |
| `WithTube(tube)` | Transfer ticket to another tube |
| `WithGroup(id)` | Assign or transfer ticket to a group |
| `WithUpdate(fn)` | Custom ticket modification (executed last) |
| `WithResetAttempts()` | Reset attempt counter |

### Delay Strategies

```go
// Fixed delay
lymbo.FixedDelay(5 * time.Minute)

// Exponential backoff: base^attempts seconds, capped at maxDelay, with optional jitter
lymbo.BackoffDelay(1.5, 15*time.Second, 0)
lymbo.BackoffDelay(2.0, time.Minute, 500*time.Millisecond)
```

### Groups

A group is a named set of tickets that can be tracked collectively. Submit tickets into a group
with `WithGroup`, then poll the group to see how many are still pending or whether all have
reached a terminal state (Done, Failed, or Cancelled).

Groups are optional and persistent — the `group_id` is stored alongside each ticket so progress
survives process restarts.

```go
// Create a group handle (lightweight, no DB write)
g := kh.Group("order-42-notifications")

// Submit tickets into the group
for _, userID := range recipients {
    ticket, _ := lymbo.NewTicket(lymbo.TicketId(uuid.NewString()), "send-notification")
    kh.Put(ctx, *ticket,
        lymbo.WithGroup(g.ID()),
        lymbo.WithPayload(userID),
    )
}

// Poll group progress from anywhere in your code
n, err := g.PendingCount(ctx)   // tickets still pending
ok, err := g.AllTerminal(ctx)   // true when none are pending
```

Alternatively, set the group directly on the ticket using the builder:

```go
ticket.WithGroup("order-42-notifications")
kh.Put(ctx, *ticket)
```

**Transitioning between groups** works the same way as transferring tubes — pass `WithGroup` to
any lifecycle method and the ticket moves to the new group atomically:

```go
// Move a ticket to a different group on retry
kh.Retry(ctx, t.ID,
    lymbo.WithGroup("retry-batch"),
    lymbo.WithDelay(lymbo.FixedDelay(5*time.Minute)),
)

// Clear the group by moving to an empty-string group is not supported;
// use a dedicated "archived" group name instead.
```

Tickets submitted without `WithGroup` are ungrouped and never appear in any group query.

## Configuration

```go
settings := lymbo.DefaultSettings().
    WithWorkers(10).
    WithBatchSize(20).
    WithProcessTime(5 * time.Minute).
    WithBackoffBase(2.0).
    WithOnlyTubes("emails", "notifications")
```

| Method | Default | Description |
| ------ | ------- | ----------- |
| `WithWorkers(n)` | 4 | Concurrent ticket processors |
| `WithBatchSize(n)` | 4 | Max tickets per poll (capped at workers) |
| `WithProcessTime(d)` | 30s | TTR before re-poll |
| `WithBackoffBase(f)` | 1.5 | Exponential backoff base |
| `WithOnlyTubes(t...)` | `"default"` | Tubes to process |
| `WithExpiration()` | enabled | Auto-cleanup of expired tickets |
| `WithoutExpiration()` | - | Disable auto-cleanup |

## Storage

### In-Memory

```go
import "github.com/ochaton/lymbo/store/memory"

store := memory.NewStore()
```

### PostgreSQL

```go
import (
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/ochaton/lymbo/store/postgres"
)

pool, _ := pgxpool.New(ctx, "postgres://user:pass@localhost/dbname")

store := postgres.NewTicketsRepository(pool)
store.Migrate(ctx) // creates schema automatically
```

Custom table name:

```go
store, _ := postgres.NewTicketsRepositoryWithConfig(postgres.Config{
    TableName: "my_tickets",
    Pool:      pool,
})
store.Migrate(ctx)
```

### Custom Store

Implement the `Store` interface for your own backend:

```go
type Store interface {
    Get(context.Context, TicketId) (Ticket, error)
    Put(context.Context, Ticket) error
    Delete(context.Context, TicketId) error
    Update(context.Context, TicketId, UpdateFunc) error
    DeleteBatch(ctx context.Context, ids []TicketId) ([]TransitionInfo, error)
    UpdateBatch(ctx context.Context, updates []UpdateSet) ([]TransitionInfo, error)
    PollPending(context.Context, PollRequest) (PollResult, error)
    ExpireTickets(ctx context.Context, limit int, now time.Time) ([]TransitionInfo, error)
    CountPendingInGroup(ctx context.Context, groupID string) (int, error)
}
```

## Prometheus Metrics

The `github.com/ochaton/lymbo/prometheus` module provides a collector. It is a **separate Go module** so importing `lymbo` does not pull in the prometheus client dependency.

```bash
go get github.com/ochaton/lymbo/prometheus
```

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    lymbo_prom "github.com/ochaton/lymbo/prometheus"
)

// Single instance, default registry:
prometheus.MustRegister(lymbo_prom.NewCollector(kh, nil))

// Multiple instances in the same process:
reg := prometheus.NewRegistry()
reg.MustRegister(lymbo_prom.NewCollector(khPinger, prometheus.Labels{"kharon": "pinger"}))
reg.MustRegister(lymbo_prom.NewCollector(khPonger, prometheus.Labels{"kharon": "ponger"}))
```

Exported metrics:

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `kharon_tickets_total` | counter | `operation`, `type`, `tube` | Ticket operation counts per (type, tube) pair |
| `kharon_workers_running` | gauge | - | Current active worker goroutines |
| `kharon_task_process_duration_seconds` | histogram | `type`, `tube` | Processing duration per (type, tube) pair |
| `kharon_queue_wait_duration_seconds` | histogram | `type`, `tube` | Queue wait time per (type, tube) pair |

Use `sum by (operation)` for global totals, `sum by (operation, type)` for per-type, `sum by (operation, tube)` for per-tube.

A complete example with Docker Compose (Postgres + Prometheus + Grafana with pre-built dashboards) is in [examples/prometheus/](examples/prometheus/).

## Examples

See [examples/](examples/) for complete working examples:

- [basic](examples/basic/) — HTTP API, memory or postgres storage, stats logging
- [simple](examples/simple/) — Rate-limited ticket pusher with postgres
- [tubes](examples/tubes/) — Ping-pong between two tubes with two Kharon instances
- [prometheus](examples/prometheus/) — Full observability stack with Grafana dashboards

## License

MIT
