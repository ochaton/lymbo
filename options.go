package lymbo

import (
	"context"
	"fmt"
	"time"

	"github.com/ochaton/lymbo/status"
)

// Option is a functional option for configuring ticket operations.
type Option func(o *Opts) error

type delayHow int

const (
	delayUnset delayHow = iota
	delayFixed
	delayExponential
)

type DelayStrategy struct {
	how         delayHow
	fixed       *struct{ duration time.Duration }
	exponential *struct {
		base     float64
		maxDelay time.Duration
		jitter   time.Duration
	}
}

// Opts contains options for ticket operations.
type Opts struct {
	// delay sets a delay before the ticket becomes eligible for processing.
	delay DelayStrategy

	// status sets the ticket's status.
	status *status.Status

	// keep indicates whether to retain the ticket in the store after the operation.
	// By default, completed/failed/cancelled tickets are removed.
	keep bool

	// errorReason stores the reason for failure (for Fail operations).
	errorReason []byte

	// nice sets the ticket's nice value (priority).
	nice *int

	// payload sets the ticket's payload data.
	payload []byte

	// resetAttempts indicates whether to reset the ticket's attempt count.
	resetAttempts bool

	// transferTube indicates a tube to transfer the ticket to.
	transferTube *Tube

	// update allows custom modification of the ticket.
	update func(ctx context.Context, t *Ticket) error
}

// WithKeep indicates that the ticket should be kept in the store after processing.
// Useful for audit trails or tracking completed work.
func WithKeep() Option {
	return func(o *Opts) error {
		o.keep = true
		return nil
	}
}

func WithResetAttempts() Option {
	return func(o *Opts) error {
		o.resetAttempts = true
		return nil
	}
}

// WithErrorReason sets an error reason for failed ticket operations.
// The reason will be stored in the ticket's ErrorReason field.
func WithErrorReason(reason any) Option {
	rv := toDefaultErrorMarshaller(reason)
	return func(o *Opts) error {
		v, err := rv.MarshalError()
		if err != nil {
			return fmt.Errorf("failed to marshal ErrorReason: %w", err)
		}
		o.errorReason = v
		return nil
	}
}

func BackoffDelay(base float64, maxDelay time.Duration, jitter time.Duration) DelayStrategy {
	return DelayStrategy{
		how: delayExponential,
		exponential: &struct {
			base     float64
			maxDelay time.Duration
			jitter   time.Duration
		}{base, maxDelay, jitter},
	}
}

func FixedDelay(duration time.Duration) DelayStrategy {
	return DelayStrategy{
		how:   delayFixed,
		fixed: &struct{ duration time.Duration }{duration},
	}
}

// WithDelay sets a delay before the ticket becomes eligible for processing.
func WithDelay(delay DelayStrategy) Option {
	return func(o *Opts) error {
		o.delay = delay
		return nil
	}
}

// WithNice sets the ticket's nice value (priority).
func WithNice(nice int) Option {
	return func(o *Opts) error {
		o.nice = &nice
		return nil
	}
}

// WithUpdate allows custom modification of the ticket before storing.
func WithUpdate(update func(ctx context.Context, t *Ticket) error) Option {
	return func(o *Opts) error {
		o.update = update
		return nil
	}
}

func WithPayload(payload any) Option {
	rv := toDefaultPayloadMarshaller(payload)
	return func(o *Opts) error {
		v, err := rv.MarshalPayload()
		if err != nil {
			return fmt.Errorf("failed to marshal Payload: %w", err)
		}
		o.payload = v
		return nil
	}
}

// WithTube sets the tube to transfer the ticket to.
func WithTube(tube Tube) Option {
	return func(o *Opts) error {
		if tube == "" {
			return ErrTubeEmpty
		}
		o.transferTube = &tube
		return nil
	}
}
