package store

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/status"
)

// MemoryStore is an in-memory implementation of the lymbo.Store interface.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[lymbo.TicketId]lymbo.Ticket
}

// Ensure MemoryStore implements lymbo.Store interface.
var _ lymbo.Store = (*MemoryStore)(nil)

// NewMemoryStore creates a new in-memory ticket store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[lymbo.TicketId]lymbo.Ticket),
	}
}

// Get retrieves a ticket by ID.
func (m *MemoryStore) Get(_ context.Context, id lymbo.TicketId) (lymbo.Ticket, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ticket, exists := m.data[id]
	if !exists {
		return lymbo.Ticket{}, lymbo.ErrTicketNotFound
	}
	return ticket, nil
}

// Add adds a new ticket to the store.
func (m *MemoryStore) Add(_ context.Context, t lymbo.Ticket) error {
	if t.ID == "" {
		return lymbo.ErrTicketIDEmpty
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	t.Status = status.Pending
	m.data[t.ID] = t

	return nil
}

// Delete removes a ticket from the store.
func (m *MemoryStore) Delete(_ context.Context, id lymbo.TicketId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, id)
	return nil
}

func (m *MemoryStore) Update(ctx context.Context, tid lymbo.TicketId, fn lymbo.UpdateFunc) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, exists := m.data[tid]
	if !exists {
		return lymbo.ErrTicketNotFound
	}

	if err := fn(ctx, &t); err != nil {
		return err
	}

	m.data[tid] = t
	return nil
}

// PollPending retrieves pending tickets ready for processing.
// It returns up to limit tickets that are ready to run, sorted by priority.
func (m *MemoryStore) PollPending(limit int, now time.Time, ttr time.Duration, maxBackoffDelay time.Duration, backoffBase float64) (lymbo.PollResult, error) {
	if limit <= 0 {
		return lymbo.PollResult{}, lymbo.ErrLimitInvalid
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var closest *time.Time
	var ready []lymbo.Ticket

	for _, t := range m.data {
		if t.Status != status.Pending {
			continue
		}

		if t.Runat.After(now) {
			if closest == nil || t.Runat.Before(*closest) {
				runat := t.Runat
				closest = &runat
			}
			continue
		}

		ready = append(ready, t)
	}

	if len(ready) == 0 {
		return lymbo.PollResult{
			Tickets:    nil,
			SleepUntil: closest,
		}, nil
	}

	// Sort by runat time, then by priority (nice value).
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Runat.Equal(ready[j].Runat) {
			return ready[i].Nice < ready[j].Nice
		}
		return ready[i].Runat.Before(ready[j].Runat)
	})

	ready = ready[:min(limit, len(ready))]

	// Update tickets with exponential backoff for next attempt.
	for _, t := range ready {
		delay := time.Duration(math.Pow(backoffBase, float64(t.Attempts)))
		delay = min(delay, maxBackoffDelay)
		delay += ttr
		t.Runat = now.Add(delay)
		t.Attempts++
		m.data[t.ID] = t
	}

	return lymbo.PollResult{
		Tickets:    ready,
		SleepUntil: nil,
	}, nil
}

// ExpireTickets removes expired non-pending tickets from the store.
// It deletes up to limit tickets that have expired (runat is before now).
func (m *MemoryStore) ExpireTickets(limit int, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for tid, t := range m.data {
		if limit <= 0 {
			break
		}

		if t.Status == status.Pending {
			continue
		}

		if t.Runat.After(now) {
			continue
		}

		delete(m.data, tid)
		limit--
	}

	return nil
}
