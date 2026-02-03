package lymbo

import (
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	tests := []struct {
		name     string
		attempts int
		expected time.Duration
	}{
		{"attempt 0", 0, 1 * time.Minute},
		{"attempt 1", 1, 2 * time.Minute},
		{"attempt 2", 2, 4 * time.Minute},
		{"attempt 3", 3, 8 * time.Minute},
		{"attempt 4 (capped)", 4, 15 * time.Minute},
		{"attempt 5 (capped)", 5, 15 * time.Minute},
		{"attempt 10 (capped)", 10, 15 * time.Minute},
	}

	baseDelay := 1 * time.Minute
	maxDelay := 15 * time.Minute
	factor := 2.0

	strategy := ExponentialBackoff(baseDelay, maxDelay, factor)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ticket := &Ticket{Attempts: tt.attempts}
			delay := strategy(ticket)
			if delay != tt.expected {
				t.Errorf("ExponentialBackoff(%v, %v, %v)(attempts=%d) = %v, want %v",
					baseDelay, maxDelay, factor, tt.attempts, delay, tt.expected)
			}
		})
	}
}

func TestFixedDelay(t *testing.T) {
	fixedDuration := 30 * time.Second
	strategy := FixedDelay(fixedDuration)

	tests := []struct {
		name     string
		attempts int
	}{
		{"attempt 0", 0},
		{"attempt 5", 5},
		{"attempt 100", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ticket := &Ticket{Attempts: tt.attempts}
			delay := strategy(ticket)
			if delay != fixedDuration {
				t.Errorf("FixedDelay(%v)(attempts=%d) = %v, want %v",
					fixedDuration, tt.attempts, delay, fixedDuration)
			}
		})
	}
}

func TestWithDelay(t *testing.T) {
	fixedDuration := 45 * time.Second
	strategy := FixedDelay(fixedDuration)

	opts := &Opts{}
	WithDelay(strategy)(opts)

	if opts.delayFunc == nil {
		t.Error("WithDelay should set delayFunc, got nil")
	}

	ticket := &Ticket{Attempts: 3}
	delay := opts.delayFunc(ticket)
	if delay != fixedDuration {
		t.Errorf("opts.delayFunc(ticket) = %v, want %v", delay, fixedDuration)
	}
}
