package lymbo

import (
	"context"
	"math"
	"time"

	"github.com/ochaton/lymbo/status"
)

// DelayContext provides ticket data for delay calculation.
type DelayContext interface {
	GetAttempts() int
}

// DelayFunc calculates retry delay based on ticket context.
type DelayFunc func(ctx DelayContext) time.Duration

// FixedDelay returns a strategy with fixed delay.
func FixedDelay(d time.Duration) DelayFunc {
	return func(_ DelayContext) time.Duration { return d }
}

// ExponentialBackoff returns a strategy with exponential growth.
// Formula: min(maxDelay, baseDelay * factor^attempts)
func ExponentialBackoff(baseDelay, maxDelay time.Duration, factor float64) DelayFunc {
	return func(ctx DelayContext) time.Duration {
		delay := time.Duration(float64(baseDelay) * math.Pow(factor, float64(ctx.GetAttempts())))
		if delay > maxDelay {
			return maxDelay
		}
		return delay
	}
}

// Option is a functional option for configuring ticket operations.
type Option func(o *Opts)

// Opts contains options for ticket operations.
type Opts struct {
	// delay sets a delay before the ticket becomes eligible for processing.
	delay time.Duration

	// delayFunc calculates delay based on ticket context (takes precedence over delay).
	delayFunc DelayFunc

	// status sets the ticket's status.
	status *status.Status

	// keep indicates whether to retain the ticket in the store after the operation.
	// By default, completed/failed/cancelled tickets are removed.
	keep bool

	// errorReason stores the reason for failure (for Fail operations).
	errorReason any

	// nice sets the ticket's nice value (priority).
	nice *int

	// payload sets the ticket's payload data.
	payload any

	// update allows custom modification of the ticket.
	update func(ctx context.Context, t *Ticket) error
}

// WithKeep indicates that the ticket should be kept in the store after processing.
// Useful for audit trails or tracking completed work.
func WithKeep() Option {
	return func(o *Opts) {
		o.keep = true
	}
}

// WithErrorReason sets an error reason for failed ticket operations.
// The reason will be stored in the ticket's ErrorReason field.
func WithErrorReason(reason any) Option {
	return func(o *Opts) {
		o.errorReason = reason
	}
}

// WithDelay sets a delay strategy for calculating when the ticket becomes eligible for processing.
func WithDelay(d DelayFunc) Option {
	return func(o *Opts) {
		o.delayFunc = d
	}
}

// WithNice sets the ticket's nice value (priority).
func WithNice(nice int) Option {
	return func(o *Opts) {
		o.nice = &nice
	}
}

// WithUpdate allows custom modification of the ticket before storing.
func WithUpdate(update func(ctx context.Context, t *Ticket) error) Option {
	return func(o *Opts) {
		o.update = update
	}
}

func WithPayload(payload any) Option {
	return func(o *Opts) {
		o.payload = payload
	}
}

