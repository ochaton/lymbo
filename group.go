package lymbo

import "context"

// Group is a named set of tickets sharing a common identifier.
// Use kh.Group(id) to create one, submit tickets with kh.Put(ctx, t, WithGroup(g.ID())),
// then track progress via PendingCount or AllTerminal.
type Group struct {
	id string
	k  *Kharon
}

// ID returns the group identifier.
func (g *Group) ID() string { return g.id }

// PendingCount returns the number of tickets in this group that are still pending.
func (g *Group) PendingCount(ctx context.Context) (int, error) {
	return g.k.store.CountPendingInGroup(ctx, g.id)
}

// AllTerminal reports whether all tickets in this group have left the pending state.
func (g *Group) AllTerminal(ctx context.Context) (bool, error) {
	n, err := g.PendingCount(ctx)
	return n == 0, err
}
