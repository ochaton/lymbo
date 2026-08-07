package memory

import (
	"context"
	"slices"
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
	deps map[lymbo.TicketId]map[lymbo.TicketId]struct{} // deps[ticket_id] = set of blocked_by IDs
}

// Ensure Store implements lymbo.Store interface.
var _ lymbo.Store = (*Store)(nil)

// NewStore creates a new in-memory ticket store.
func NewStore() *Store {
	return &Store{
		data: make(map[lymbo.TicketId]lymbo.Ticket),
		deps: make(map[lymbo.TicketId]map[lymbo.TicketId]struct{}),
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
	m.removeDep(id)
	return nil
}

// removeDep removes id from all dep sets and deletes its own dep set.
// Must be called with m.mu held.
func (m *Store) removeDep(id lymbo.TicketId) {
	for _, depSet := range m.deps {
		delete(depSet, id)
	}
	delete(m.deps, id)
}

// PutAfterGroup atomically inserts the ticket and wires it as a finalizer for groupID.
func (m *Store) PutAfterGroup(_ context.Context, t lymbo.Ticket, groupID string) error {
	if t.ID == "" {
		return lymbo.ErrTicketIDEmpty
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// REQ 12: no-op if ticket already exists
	if _, exists := m.data[t.ID]; exists {
		return nil
	}

	t.Status = status.Pending
	m.data[t.ID] = t

	// REQ 6: scan pending group members, insert dep rows
	for id, member := range m.data {
		if id == t.ID {
			continue
		}
		if member.GroupId != nil && *member.GroupId == groupID && member.Status == status.Pending {
			if m.deps[t.ID] == nil {
				m.deps[t.ID] = make(map[lymbo.TicketId]struct{})
			}
			m.deps[t.ID][id] = struct{}{}
		}
	}

	return nil
}

func (m *Store) DeleteBatch(_ context.Context, ids []lymbo.TicketId) ([]lymbo.TransitionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var infos []lymbo.TransitionInfo
	for _, id := range ids {
		if t, ok := m.data[id]; ok {
			infos = append(infos, lymbo.TransitionInfo{Id: t.ID, Type: t.Type, Tube: t.Tube, Status: t.Status})
			delete(m.data, id)
			m.removeDep(id)
		}
	}
	return infos, nil
}

func updateOne(t *lymbo.Ticket, us lymbo.UpdateSet) {
	if us.Status != nil {
		t.Status = *us.Status
	}
	if us.Nice != nil {
		t.Nice = *us.Nice
	}
	if us.Backoff != nil {
		delay := lymbo.ExpBackoffDelay(us.Backoff.Base, t.Attempts, us.Backoff.MaxDelay)
		delay += us.Backoff.Jitter
		t.Runat = time.Now().Add(delay)
	} else if us.Runat != nil {
		t.Runat = *us.Runat
	}
	if us.Payload != nil {
		t.Payload = us.Payload
	}
	if us.ErrorReason != nil {
		t.ErrorReason = us.ErrorReason
	}
	if us.Tube != nil {
		t.Tube = *us.Tube
	}
	if us.GroupId != nil {
		t.GroupId = us.GroupId
	}
}

func (m *Store) UpdateBatch(ctx context.Context, updates []lymbo.UpdateSet) ([]lymbo.TransitionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	infos := make([]lymbo.TransitionInfo, 0, len(updates))
	for _, us := range updates {
		t, exists := m.data[us.Id]
		if !exists {
			return nil, lymbo.ErrTicketNotFound
		}

		updateOne(&t, us)
		m.data[t.ID] = t
		infos = append(infos, lymbo.TransitionInfo{Id: t.ID, Type: t.Type, Tube: t.Tube, Status: t.Status})

		// REQ 3: remove from all dep sets when ticket reaches terminal state
		if t.Status == status.Done || t.Status == status.Failed || t.Status == status.Cancelled {
			for _, depSet := range m.deps {
				delete(depSet, t.ID)
			}
		}
	}

	return infos, nil
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

// PollPending retrieves pending tickets ready for processing.
// It returns up to limit tickets that are ready to run, sorted by priority.
func (m *Store) PollPending(_ context.Context, req lymbo.PollRequest) (lymbo.PollResult, error) {
	if req.Limit <= 0 {
		return lymbo.PollResult{}, lymbo.ErrLimitInvalid
	}

	if len(req.RequestTubes) == 0 {
		// Empty subscription set: caller wants to poll no tubes.
		// Return early so we don't silently select the default tube
		// and bump attempts on tickets the caller doesn't own.
		return lymbo.PollResult{}, nil
	}

	tubes := make([]string, 0, len(req.RequestTubes))
	for _, t := range req.RequestTubes {
		tubes = append(tubes, t.String())
	}
	slices.Sort(tubes)
	tubes = slices.Compact(tubes)

	m.mu.Lock()
	defer m.mu.Unlock()

	var ready []lymbo.Ticket

	for _, t := range m.data {
		if t.Status != status.Pending {
			continue
		}

		if t.Runat.After(req.Now) {
			continue
		}

		if _, found := slices.BinarySearch(tubes, t.Tube.String()); !found {
			continue
		}

		// REQ 1: blocked tickets must not be returned
		if len(m.deps[t.ID]) > 0 {
			continue
		}

		ready = append(ready, t)
	}

	if len(ready) == 0 {
		return lymbo.PollResult{Tickets: nil}, nil
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
	for i := range ready {
		ready[i].ReadyAt = ready[i].Runat
		t := ready[i]
		delay := lymbo.ExpBackoffDelay(req.BackoffBase, t.Attempts, req.MaxBackoffDelay)
		delay += req.TTR
		t.Runat = req.Now.Add(delay)
		t.Attempts++
		m.data[t.ID] = t
	}

	return lymbo.PollResult{
		Tickets: ready,
	}, nil
}

// CountPendingInGroup returns the number of pending tickets with the given group ID.
func (m *Store) CountPendingInGroup(_ context.Context, groupID string) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, t := range m.data {
		if t.GroupId != nil && *t.GroupId == groupID && t.Status == status.Pending {
			count++
		}
	}
	return count, nil
}

// ExpireTickets removes expired non-pending tickets from the store.
// It deletes up to limit tickets that have expired (runat is before now).
func (m *Store) ExpireTickets(_ context.Context, limit int, now time.Time) ([]lymbo.TransitionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var infos []lymbo.TransitionInfo
	for tid, t := range m.data {
		if len(infos) == limit {
			break
		}

		if t.Status == status.Pending {
			continue
		}

		if t.Runat.After(now) {
			continue
		}

		infos = append(infos, lymbo.TransitionInfo{Id: t.ID, Type: t.Type, Tube: t.Tube, Status: t.Status})
		delete(m.data, tid)
	}

	return infos, nil
}
