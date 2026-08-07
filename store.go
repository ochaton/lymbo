package lymbo

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"github.com/ochaton/lymbo/status"
)

type UpdateFunc func(context.Context, *Ticket) error

type PollRequest struct {
	Limit           int
	Now             time.Time
	TTR             time.Duration
	BackoffBase     float64
	MaxBackoffDelay time.Duration
	RequestTubes    []Tube
}

type DelayBackoff struct {
	Base     float64
	Jitter   time.Duration
	MaxDelay time.Duration
}

// ExpBackoffDelay returns min(base^attempts, maxDelay). The cap is applied in
// float64 seconds before converting to time.Duration: base^attempts stops
// fitting into int64 nanoseconds at modest attempt counts (1.5^57s already
// overflows), and Go's float-to-integer conversion is implementation-defined
// for out-of-range values. The comparison is inverted so that +Inf and NaN
// also fall back to maxDelay.
func ExpBackoffDelay(base float64, attempts int, maxDelay time.Duration) time.Duration {
	seconds := math.Pow(base, float64(attempts))
	if maxSeconds := maxDelay.Seconds(); !(seconds < maxSeconds) {
		return maxDelay
	}
	return time.Duration(seconds * float64(time.Second))
}

type UpdateSet struct {
	Id          TicketId
	Tube        *Tube
	GroupId     *string
	Status      *status.Status
	Nice        *int
	Runat       *time.Time
	Backoff     *DelayBackoff
	Payload     json.RawMessage
	ErrorReason json.RawMessage
}

// Store defines the interface for ticket storage and management.
// Implementations must be safe for concurrent use.
type Store interface {
	// Get retrieves a ticket by ID.
	// Returns ErrTicketNotFound if the ticket doesn't exist.
	Get(context.Context, TicketId) (Ticket, error)

	// Put adds a new ticket to the store or updates an existing one.
	// The ticket status will be set to Pending.
	// Returns ErrTicketIDEmpty if the ticket ID is empty.
	Put(context.Context, Ticket) error

	// Delete removes a ticket from the store.
	// This operation is idempotent and won't return an error if the ticket doesn't exist.
	Delete(context.Context, TicketId) error

	// Update modifies an existing ticket using the provided UpdateFunc.
	// The UpdateFunc receives a pointer to the ticket to modify.
	Update(context.Context, TicketId, UpdateFunc) error

	// PollPending retrieves pending tickets ready for processing.
	// Returns up to limit tickets sorted by priority (Runat, then Nice).
	// The backoffBase parameter controls the exponential backoff calculation.
	// Returns ErrLimitInvalid if limit <= 0.
	PollPending(context.Context, PollRequest) (PollResult, error)

	// ExpireTickets removes expired tickets from the store.
	// Only removes non-pending tickets where Runat is before now.
	// Returns info about each expired ticket.
	ExpireTickets(ctx context.Context, limit int, now time.Time) ([]TransitionInfo, error)

	// DeleteBatch removes multiple tickets from the store.
	// Returns info about each deleted ticket.
	DeleteBatch(ctx context.Context, ids []TicketId) ([]TransitionInfo, error)

	// UpdateBatch modifies multiple tickets using the provided UpdateSets.
	// Returns info about each updated ticket.
	UpdateBatch(ctx context.Context, updates []UpdateSet) ([]TransitionInfo, error)

	// CountPendingInGroup returns the number of pending tickets with the given group ID.
	CountPendingInGroup(ctx context.Context, groupID string) (int, error)

	// PutAfterGroup atomically inserts the ticket and wires it as a finalizer for groupID.
	// It scans all currently pending group members and creates dep rows blocking the finalizer.
	// Returns nil if the ticket ID already exists (idempotent re-submission).
	// Returns ErrFinalizerInGroup if the ticket belongs to the same group it finalizes.
	PutAfterGroup(ctx context.Context, ticket Ticket, groupID string) error
}

// TransitionInfo describes a ticket that was affected by a store operation.
type TransitionInfo struct {
	Id     TicketId
	Type   string
	Tube   Tube
	Status status.Status
}

// PollResult contains the result of a store polling operation.
type PollResult struct {
	// Tickets contains the tickets ready for processing.
	// Will be empty if SleepUntil is non-nil.
	Tickets []Ticket
}
