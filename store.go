package lymbo

import (
	"context"
	"time"
)

type UpdateFunc func(context.Context, *Ticket) error

// Store defines the interface for ticket storage and management.
// Implementations must be safe for concurrent use.
type Store interface {
	// Get retrieves a ticket by ID.
	// Returns ErrTicketNotFound if the ticket doesn't exist.
	Get(context.Context, TicketId) (Ticket, error)

	// Add adds a new ticket to the store.
	// The ticket status will be set to Pending.
	// Returns ErrTicketIDEmpty if the ticket ID is empty.
	Add(context.Context, Ticket) error

	// Delete removes a ticket from the store.
	// This operation is idempotent and won't return an error if the ticket doesn't exist.
	Delete(context.Context, TicketId) error

	// Update modifies an existing ticket using the provided UpdateFunc.
	// The UpdateFunc receives a pointer to the ticket to modify.
	Update(context.Context, TicketId, UpdateFunc) error

	// PollPending retrieves pending tickets ready for processing.
	// Returns up to limit tickets sorted by priority (Runat, then Nice).
	// Returns ErrLimitInvalid if limit <= 0.
	PollPending(limit int, now time.Time, ttr time.Duration, maxBackoffDelay time.Duration, backoffBase float64) (PollResult, error)

	// ExpireTickets removes expired tickets from the store.
	// Only removes non-pending tickets where Runat is before now.
	// Deletes up to limit tickets.
	ExpireTickets(limit int, now time.Time) error
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
