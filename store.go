package lymbo

import (
	"context"
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
}

type UpdateSet struct {
	Id          TicketId
	Status      *status.Status
	Nice        *int
	Runat       *time.Time
	Payload     any
	ErrorReason any
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

	// UpdateSet modifies an existing ticket using the provided UpdateSet.
	// it does not fetch the ticket, the request is only Update.
	UpdateSet(context.Context, UpdateSet) error

	// PollPending retrieves pending tickets ready for processing.
	// Returns up to limit tickets sorted by priority (Runat, then Nice).
	// The backoffBase parameter controls the exponential backoff calculation.
	// Returns ErrLimitInvalid if limit <= 0.
	PollPending(context.Context, PollRequest) (PollResult, error)

	// ExpireTickets removes expired tickets from the store.
	// Only removes non-pending tickets where Runat is before now.
	// Deletes up to limit tickets.
	ExpireTickets(ctx context.Context, limit int, now time.Time) (int64, error)

	// DeleteBatch removes multiple tickets from the store.
	// This operation is idempotent and won't return an error if some tickets don't exist.
	DeleteBatch(ctx context.Context, ids []TicketId) error

	// UpdateBatch modifies multiple tickets using the provided UpdateFunc.
	// The UpdateFunc receives a pointer to each ticket to modify.
	UpdateBatch(ctx context.Context, updates []UpdateSet) error
}

// PollResult contains the result of a store polling operation.
type PollResult struct {
	// SleepUntil indicates when the next poll should occur.
	// nil means tickets are available for immediate processing.
	// A time in the future means no tickets are ready, sleep until this time.
	SleepUntil *time.Time

	// Tickets contains the tickets ready for processing.
	// Will be empty if SleepUntil is non-nil.
	Tickets []Ticket
}
