# Kharon

A simple and efficient Go library for delayed task processing and state reconciliation.

## Overview

Kharon is a task orchestration library that allows you to schedule, process, and manage tickets (tasks) with built-in retry logic, expiration, and priority handling. Perfect for background job processing, state reconciliation, and delayed task execution.

## Features

- **Flexible Storage**: In-memory and PostgreSQL backends with pluggable Store interface
- **Priority Scheduling**: Nice values for task prioritization (lower = higher priority)
- **Retry Logic**: Exponential backoff with configurable base and max delay
- **Automatic Expiration**: Built-in cleanup of completed/expired tickets
- **Flexible Options**: Fine-grained control over ticket lifecycle with options
- **Concurrent Processing**: Configurable worker pools for parallel execution

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
    "time"

    "github.com/google/uuid"
    "github.com/ochaton/lymbo"
    "github.com/ochaton/lymbo/store/memory"
)

func main() {
    ctx := context.Background()
    logger := slog.Default()

    // Create Kharon with default settings
    settings := lymbo.DefaultSettings()
    kh := lymbo.NewKharon(memory.NewStore(), settings, logger)

    // Create a router to handle different ticket types
    r := lymbo.NewRouter()
    r.HandleFunc("example", func(ctx context.Context, t *lymbo.Ticket) error {
        logger.InfoContext(ctx, "processing ticket", "id", t.ID, "payload", t.Payload)
        // Do your work here
        return kh.Done(ctx, t)
    })

    // Start processing tickets
    go kh.Run(ctx, r)

    // Add a ticket (use UUIDv7 for IDs)
    ticketID := lymbo.TicketId(uuid.NewString()) // Use UUIDv7 recommended
    ticket, _ := lymbo.NewTicket(ticketID, "example")
    kh.Put(ctx, *ticket)
}
```

## Usage

### Creating and Adding Tickets

Tickets are the unit of work in Kharon. **Use UUIDs (UUIDv7 recommended) for ticket IDs.**

```go
import "github.com/google/uuid"

// Basic ticket (use UUIDv7 for IDs)
ticketID := lymbo.TicketId(uuid.NewString()) // UUIDv7 recommended
ticket, err := lymbo.NewTicket(ticketID, "task-type")

// Ticket with structured payload (Store handles JSON marshaling)
type MyPayload struct {
    Key   string `json:"key"`
    Value int    `json:"value"`
}
ticket = ticket.WithPayload(MyPayload{Key: "example", Value: 42})

// Or with simple types
ticket = ticket.WithPayload(map[string]any{"key": "value"})

// Ticket with priority (nice value: lower = higher priority)
ticket = ticket.WithNice(5)

// Ticket with delayed execution
ticket = ticket.WithRunat(time.Now().Add(1 * time.Hour))

// Add ticket to Kharon
err = kh.Put(ctx, *ticket)

// Add ticket with options (applied during Put)
err = kh.Put(ctx, *ticket,
    lymbo.WithDelay(lymbo.FixedDelay(5*time.Minute)),  // Delay first execution
    lymbo.WithNice(10),                                 // Set priority
)
```

### Handling Tickets

Use the Router to register handlers for different ticket types:

```go
r := lymbo.NewRouter()

// Register a handler for a specific type
r.HandleFunc("email", func(ctx context.Context, t *lymbo.Ticket) error {
    // Process email ticket
    sendEmail(t.Payload)
    return kh.Done(ctx, t)
})

// Handle unknown ticket types
r.NotFoundFunc(func(ctx context.Context, t *lymbo.Ticket) error {
    return kh.Fail(ctx, t, lymbo.WithErrorReason("unsupported type"))
})
```

### Managing Ticket State

Kharon provides several methods to manage ticket lifecycle, each accepting options for flexible control.

#### Ack - Acknowledge and Remove

Acknowledges successful processing and removes the ticket from the store (unless `WithKeep()` is used).

```go
// Acknowledge and remove ticket immediately
err := kh.Ack(ctx, t)

// Acknowledge but keep ticket in store forever
err := kh.Ack(ctx, t, lymbo.WithKeep())

// Keep ticket with TTL (will be auto-removed after delay)
err := kh.Ack(ctx, t,
    lymbo.WithKeep(),
    lymbo.WithDelay(lymbo.FixedDelay(24*time.Hour)), // Remove after 24 hours
)
```

#### Done - Mark as Successfully Completed

Marks ticket as done and **automatically keeps it in the store**. This is equivalent to `Ack(ctx, id, WithKeep())`.

```go
// Mark as done (kept in store indefinitely)
err := kh.Done(ctx, t)

// Mark as done with TTL for auto-removal
err := kh.Done(ctx, t,
    lymbo.WithDelay(lymbo.FixedDelay(1*time.Hour)), // Auto-remove after 1 hour
)

// Update ticket payload before marking done
err := kh.Done(ctx, t,
    lymbo.WithDelay(lymbo.FixedDelay(24*time.Hour)),
    lymbo.WithUpdate(func(ctx context.Context, t *lymbo.Ticket) error {
        // Store struct directly - no need to pre-marshal
        t.Payload = map[string]any{"result": "success", "completedAt": time.Now()}
        return nil
    }),
)
```

#### Fail - Mark as Failed

Marks ticket as failed and **keeps it in the store** for debugging/audit.

```go
// Mark ticket as failed with error reason
err := kh.Fail(ctx, t,
    lymbo.WithErrorReason("connection timeout"),
)

// Fail with TTL for auto-cleanup
err := kh.Fail(ctx, t,
    lymbo.WithErrorReason("database error"),
    lymbo.WithDelay(lymbo.FixedDelay(7*24*time.Hour)), // Keep for 7 days
)

// Fail and update ticket data
err := kh.Fail(ctx, t,
    lymbo.WithErrorReason(map[string]any{
        "error": "invalid input",
        "code": 400,
    }),
    lymbo.WithUpdate(func(ctx context.Context, t *lymbo.Ticket) error {
        // Add error context to payload
        t.Payload = map[string]any{
            "originalPayload": t.Payload,
            "failedAt": time.Now(),
        }
        return nil
    }),
)
```

#### Cancel - Cancel Ticket Processing

Cancels a ticket. By default, removes it from the store unless `WithKeep()` is used.

```go
// Cancel and remove immediately
err := kh.Cancel(ctx, t)

// Cancel but keep in store
err := kh.Cancel(ctx, t,
    lymbo.WithKeep(),
    lymbo.WithErrorReason("cancelled by user"),
)

// Cancel with TTL
err := kh.Cancel(ctx, t,
    lymbo.WithKeep(),
    lymbo.WithDelay(lymbo.FixedDelay(30*24*time.Hour)), // Keep for 30 days
)
```

#### Retry - Reschedule for Processing

Reschedules a ticket for future processing with updated parameters.

```go
// Retry immediately with default backoff
err := kh.Retry(ctx, t)

// Retry with custom delay
err := kh.Retry(ctx, t,
    lymbo.WithDelay(lymbo.FixedDelay(5*time.Minute)), // Retry in 5 minutes
)

// Retry with priority change
err := kh.Retry(ctx, t,
    lymbo.WithDelay(lymbo.FixedDelay(1*time.Minute)),
    lymbo.WithNice(1), // Higher priority for retry
)

// Retry with payload update
err := kh.Retry(ctx, t,
    lymbo.WithDelay(lymbo.FixedDelay(10*time.Second)),
    lymbo.WithUpdate(func(ctx context.Context, t *lymbo.Ticket) error {
        // Add retry metadata to payload
        payload := t.Payload.(map[string]any)
        payload["retryCount"] = t.Attempts + 1
        payload["lastRetry"] = time.Now()
        return nil
    }),
)
```

#### Other Operations

```go
// Delete a ticket permanently
err := kh.Delete(ctx, ticketID)

// Get ticket status
ticket, err := kh.Get(ctx, ticketID)
```

### Common Options

All state management methods (`Retry`, `Done`, `Cancel`, `Fail`, `Put`, `Ack`) support these options:

| Option | Description | Applicable Methods |
|--------|-------------|-------------------|
| `WithDelay(d DelayFunc)` | Delay next processing or set TTL for auto-removal | All |
| `WithNice(n int)` | Change ticket priority (lower = higher priority) | All |
| `WithUpdate(fn func(context.Context, *Ticket) error)` | Custom ticket modification (executed after other options) | All |
| `WithKeep()` | Keep ticket in store instead of removing | `Ack`, `Cancel` |
| `WithErrorReason(reason any)` | Store error/cancellation reason | `Fail`, `Cancel`, `Retry` |

**Delay Functions:**

- `FixedDelay(d time.Duration)` - Returns a fixed delay duration
- `ExponentialBackoff(base float64, max time.Duration)` - Returns exponential backoff based on ticket attempts: `min(base^attempts * second, max)`

**Important Notes:**

- `WithUpdate()` is **always executed last**, after all other options have been applied, ensuring you have full control over the final ticket state
- `WithDelay()` for `Done`/`Cancelled`/`Failed` tickets sets when the ticket should be auto-removed (TTL)
- `WithDelay()` for `Retry`/`Pending` tickets sets when the ticket should be processed next
- `Done()` automatically applies `WithKeep()` - tickets are kept in store
- `Fail()` automatically applies `WithKeep()` - failed tickets are kept for debugging
- `Ack()` and `Cancel()` remove tickets by default unless `WithKeep()` is used

## Configuration

### Default Settings

```go
settings := lymbo.DefaultSettings()
```

Default values:

- **Workers**: 4 concurrent workers
- **BatchSize**: 10 tickets per poll (capped at workers)
- **ProcessTime**: 30 seconds (time-to-run before retry)
- **BackoffBase**: 1.5 (exponential backoff: delay = 1.5^attempts seconds)
- **MaxBackoffDelay**: 15 seconds (maximum retry delay)
- **MaxPollInterval**: 15 seconds (max time between polls)
- **MinPollInterval**: 10 milliseconds (min time between polls)
- **EnableExpiration**: true (automatic cleanup of expired tickets)
- **ExpirationInterval**: 100 milliseconds (how often to check for expired tickets)

### Customizing Settings

```go
settings := lymbo.DefaultSettings().
    WithWorkers(10).                          // 10 concurrent workers
    WithBatchSize(20).                        // Poll up to 20 tickets at once
    WithProcessTime(5 * time.Minute).         // 5 minutes time-to-run before retry
    WithBackoffBase(2.0).                     // Exponential backoff: 2^attempts seconds
    WithExpiration()                          // Enable automatic expiration cleanup

kh := lymbo.NewKharon(memory.NewStore(), settings, logger)
```

### Configuration Options

| Method | Description | Default |
|--------|-------------|---------|
| `WithWorkers(n)` | Number of concurrent ticket processors | 4 |
| `WithBatchSize(n)` | Max tickets to poll at once (capped at workers) | 10 |
| `WithProcessTime(d)` | Time-to-run before retry (prevents re-polling during processing) | 30s |
| `WithBackoffBase(base float64)` | Base for exponential backoff calculation (delay = base^attempts seconds) | 1.5 |
| `WithExpiration()` | Enable automatic cleanup of expired tickets | true |
| `WithoutExpiration()` | Disable automatic cleanup | - |

## Storage

Kharon supports pluggable storage backends through the `Store` interface.

### In-Memory Store

Perfect for development, testing, or single-instance deployments.

```go
import "github.com/ochaton/lymbo/store/memory"

store := memory.NewStore()
kh := lymbo.NewKharon(store, settings, logger)
```

### PostgreSQL Store

Production-ready persistent storage with ACID guarantees, powered by [sqlc](https://sqlc.dev/).

```go
import (
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/ochaton/lymbo/store/postgres"
)

// Create connection pool
pool, err := pgxpool.New(ctx, "postgres://user:pass@localhost/dbname")
if err != nil {
    log.Fatal(err)
}

// Create store
store := postgres.NewTicketsRepository(pool)
kh := lymbo.NewKharon(store, settings, logger)
```

**PostgreSQL Setup:**

1. Create the database schema (see [sql/schema.sql](sql/schema.sql))
2. The store uses `pgx/v5` for database connectivity
3. Automatically handles ticket locking and atomic updates with optimistic concurrency

### Custom Store Implementation

Implement the `Store` interface for your own backend (Redis, MongoDB, etc.):

```go
type Store interface {
    // Put inserts or updates a ticket in the store (REPLACE operation)
    Put(ctx context.Context, t Ticket) error

    // Get retrieves a ticket by ID
    Get(ctx context.Context, id TicketId) (Ticket, error)

    // Update modifies a ticket atomically using the provided function
    Update(ctx context.Context, id TicketId, fn UpdateFunc) error

    // Delete removes a ticket from the store
    Delete(ctx context.Context, id TicketId) error

    // PollPending retrieves pending tickets ready for processing
    PollPending(ctx context.Context, req PollRequest) (PollResult, error)

    // ExpireTickets removes expired non-pending tickets
    ExpireTickets(ctx context.Context, limit int, now time.Time) (int64, error)
}

type UpdateFunc func(ctx context.Context, t *Ticket) error

type PollRequest struct {
    Limit           int           // Max tickets to return
    Now             time.Time     // Current time
    TTR             time.Duration // Time-to-run
    BackoffBase     float64       // Exponential backoff base
    MaxBackoffDelay time.Duration // Max backoff delay
}

type PollResult struct {
    Tickets    []Ticket   // Ready tickets
    SleepUntil *time.Time // When to poll next (if no tickets ready)
}
```

## Best Practices

### Ticket IDs

**Always use UUIDs for ticket IDs, with UUIDv7 recommended.**

```go
import "github.com/google/uuid"

// Recommended: UUIDv7 provides time-ordered IDs
ticketID := lymbo.TicketId(uuid.NewString())

// Create ticket with UUID
ticket, err := lymbo.NewTicket(ticketID, "task-type")
```

UUIDv7 provides:

- Time-ordered IDs for better database index performance
- Guaranteed uniqueness across distributed systems
- Compatibility with PostgreSQL UUID type
- Sortability by creation time

### Error Handling in Handlers

Always handle errors appropriately in ticket handlers:

```go
r.HandleFunc("task", func(ctx context.Context, t *lymbo.Ticket) error {
    if err := doWork(t.Payload); err != nil {
        if isTransientError(err) {
            // Retry transient errors with backoff
            return kh.Retry(ctx, t, lymbo.WithDelay(lymbo.FixedDelay(5*time.Minute)))
        }
        // Permanent failure - keep for debugging
        return kh.Fail(ctx, t,
            lymbo.WithErrorReason(err.Error()),
            lymbo.WithDelay(lymbo.FixedDelay(7*24*time.Hour)),
        )
    }
    // Success - acknowledge and remove
    return kh.Ack(ctx, t)
})
```

### Payload Design

Store structured data directly - the Store handles JSON marshaling automatically (JSONB in PostgreSQL):

```go
type TaskPayload struct {
    UserID string `json:"user_id"`
    Action string `json:"action"`
}

// Store the struct directly - no need to marshal manually
payload := TaskPayload{UserID: "123", Action: "sync"}
ticket := ticket.WithPayload(payload)

// In your handler, retrieve and use the payload
r.HandleFunc("task", func(ctx context.Context, t *lymbo.Ticket) error {
    var payload TaskPayload
    if err := json.Unmarshal(t.Payload.([]byte), &payload); err != nil {
        return kh.Fail(ctx, t, lymbo.WithErrorReason(err))
    }
    // Use payload.UserID, payload.Action, etc.
    return kh.Ack(ctx, t)
})
```

**Note:** The Store automatically marshals your payload to JSON (JSONB in PostgreSQL), so pass your structs directly to `WithPayload()` - don't pre-marshal them.

## Examples

### Basic HTTP API

See [examples/basic/main.go](examples/basic/main.go) for a complete working example with:

- Creating tickets via HTTP POST with query parameters (`?nice=5&delay=10.5`)
- Querying ticket status (GET)
- Cancelling tickets (POST)
- Deleting tickets (DELETE)
- Support for both in-memory and PostgreSQL stores (via `DB_TYPE` and `DB_DSN` env vars)

### Example Usage Patterns

**Background job processing:**

```go
r.HandleFunc("send-email", func(ctx context.Context, t *lymbo.Ticket) error {
    if err := sendEmail(t.Payload); err != nil {
        // Retry with exponential backoff
        return kh.Retry(ctx, t)
    }
    // Success - remove from store
    return kh.Ack(ctx, t)
})
```

**State reconciliation with audit trail:**

```go
r.HandleFunc("sync-user", func(ctx context.Context, t *lymbo.Ticket) error {
    if err := syncUserToExternalSystem(t.Payload); err != nil {
        // Keep failed ticket for debugging, auto-remove after 7 days
        return kh.Fail(ctx, t,
            lymbo.WithErrorReason(err.Error()),
            lymbo.WithDelay(lymbo.FixedDelay(7*24*time.Hour)),
        )
    }
    // Keep successful sync record for 24 hours
    return kh.Done(ctx, t, lymbo.WithDelay(lymbo.FixedDelay(24*time.Hour)))
})
```

**Rate-limited API calls:**

```go
r.HandleFunc("api-call", func(ctx context.Context, t *lymbo.Ticket) error {
    if err := callRateLimitedAPI(t.Payload); err != nil {
        if isRateLimitError(err) {
            // Retry with custom delay and lower priority
            return kh.Retry(ctx, t,
                lymbo.WithDelay(lymbo.FixedDelay(5*time.Minute)),
                lymbo.WithNice(100), // Lower priority
            )
        }
        return kh.Fail(ctx, t, lymbo.WithErrorReason(err))
    }
    return kh.Ack(ctx, t)
})
```

**Ticket modification during processing:**

```go
type WorkflowPayload struct {
    Step   int    `json:"step"`
    UserID string `json:"user_id"`
}

r.HandleFunc("multi-step", func(ctx context.Context, t *lymbo.Ticket) error {
    var payload WorkflowPayload
    json.Unmarshal(t.Payload.([]byte), &payload)

    if payload.Step < 3 {
        // Move to next step
        return kh.Retry(ctx, t,
            lymbo.WithDelay(lymbo.FixedDelay(1*time.Second)),
            lymbo.WithUpdate(func(ctx context.Context, ticket *lymbo.Ticket) error {
                // Increment step in payload
                payload.Step++
                ticket.Payload = payload
                return nil
            }),
        )
    }

    // All steps complete
    return kh.Done(ctx, t)
})
```

## License

MIT
