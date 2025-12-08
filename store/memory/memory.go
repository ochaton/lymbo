package memory

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/status"
)

// Store is an in-memory implementation of the lymbo.Store interface.
type Store struct {
	mu   sync.RWMutex
	data map[lymbo.TicketId]lymbo.Ticket
}

// Ensure Store implements lymbo.Store interface.
var _ lymbo.Store = (*Store)(nil)

// NewStore creates a new in-memory ticket store.
func NewStore() *Store {
	return &Store{
		data: make(map[lymbo.TicketId]lymbo.Ticket),
	}
}

// Get retrieves a ticket by ID.
func (m *Store) Get(_ context.Context, id lymbo.TicketId) (lymbo.Ticket, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ticket, exists := m.data[id]
	if !exists {
		return lymbo.Ticket{}, lymbo.ErrTicketNotFound
	}
	return ticket, nil
}

// Put adds a new ticket to the store.
func (m *Store) Put(_ context.Context, t lymbo.Ticket) error {
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
func (m *Store) Delete(_ context.Context, id lymbo.TicketId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, id)
	return nil
}

func (m *Store) DeleteBatch(_ context.Context, ids []lymbo.TicketId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, id := range ids {
		delete(m.data, id)
	}
	return nil
}

func updateOne(t *lymbo.Ticket, us lymbo.UpdateSet) {
	if us.Nice != nil {
		t.Nice = *us.Nice
	}
	if us.Runat != nil {
		t.Runat = *us.Runat
	}
	if us.Payload != nil {
		t.Payload = us.Payload
	}
	if us.ErrorReason != nil {
		t.ErrorReason = us.ErrorReason
	}
}

func (m *Store) UpdateBatch(ctx context.Context, updates []lymbo.UpdateSet) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, us := range updates {
		t, exists := m.data[us.Id]
		if !exists {
			return lymbo.ErrTicketNotFound
		}

		updateOne(&t, us)
		m.data[t.ID] = t
	}

	return nil
}

func (m *Store) Update(ctx context.Context, tid lymbo.TicketId, fn lymbo.UpdateFunc) error {
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

func (m *Store) UpdateSet(ctx context.Context, us lymbo.UpdateSet) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, exists := m.data[us.Id]
	if !exists {
		return lymbo.ErrTicketNotFound
	}

	updateOne(&t, us)
	m.data[us.Id] = t
	return nil
}

// PollPending retrieves pending tickets ready for processing.
// It returns up to limit tickets that are ready to run, sorted by priority.
func (m *Store) PollPending(_ context.Context, req lymbo.PollRequest) (lymbo.PollResult, error) {
	if req.Limit <= 0 {
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

		if t.Runat.After(req.Now) {
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

	ready = ready[:min(req.Limit, len(ready))]

	// Update tickets with exponential backoff for next attempt.
	for _, t := range ready {
		delay := time.Duration(math.Pow(req.BackoffBase, float64(t.Attempts)))
		delay = min(delay, req.MaxBackoffDelay)
		delay += req.TTR
		t.Runat = req.Now.Add(delay)
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
func (m *Store) ExpireTickets(_ context.Context, limit int, now time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for tid, t := range m.data {
		if count == limit {
			break
		}

		if t.Status == status.Pending {
			continue
		}

		if t.Runat.After(now) {
			continue
		}

		delete(m.data, tid)
		count++
	}

	return int64(count), nil
}
