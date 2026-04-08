package lymbo

import (
	"context"
	"time"

	"github.com/ochaton/lymbo/stats"
	"github.com/ochaton/lymbo/status"
)

func statusToMetric(s status.Status) stats.Metric {
	switch s {
	case status.Done:
		return stats.Ack
	case status.Failed:
		return stats.Fail
	case status.Cancelled:
		return stats.Cancel
	case status.Pending:
		return stats.Retry
	default:
		return stats.Process
	}
}

// observableStore wraps a Store and feeds per-type/per-tube stats
// from store-level mutations where the ticket type and tube are known.
type observableStore struct {
	Store
	stats *stats.T
}

func newObservableStore(s Store, st *stats.T) Store {
	return &observableStore{Store: s, stats: st}
}

func (o *observableStore) Put(ctx context.Context, t Ticket) error {
	if err := o.Store.Put(ctx, t); err != nil {
		return err
	}
	o.stats.ByType(t.Type).Inc(stats.Add)
	o.stats.ByTube(t.Tube.String()).Inc(stats.Add)
	return nil
}

func (o *observableStore) DeleteBatch(ctx context.Context, ids []TicketId) ([]TransitionInfo, error) {
	infos, err := o.Store.DeleteBatch(ctx, ids)
	if err != nil {
		return infos, err
	}
	for _, info := range infos {
		o.stats.ByType(info.Type).Inc(stats.Delete)
		o.stats.ByTube(string(info.Tube)).Inc(stats.Delete)
	}
	return infos, nil
}

func (o *observableStore) UpdateBatch(ctx context.Context, updates []UpdateSet) ([]TransitionInfo, error) {
	infos, err := o.Store.UpdateBatch(ctx, updates)
	if err != nil {
		return infos, err
	}
	for _, info := range infos {
		m := statusToMetric(info.Status)
		o.stats.ByType(info.Type).Inc(m)
		o.stats.ByTube(string(info.Tube)).Inc(m)
	}
	return infos, nil
}

func (o *observableStore) ExpireTickets(ctx context.Context, limit int, now time.Time) ([]TransitionInfo, error) {
	infos, err := o.Store.ExpireTickets(ctx, limit, now)
	if err != nil {
		return infos, err
	}
	for _, info := range infos {
		o.stats.ByType(info.Type).Inc(stats.Expire)
		o.stats.ByTube(string(info.Tube)).Inc(stats.Expire)
	}
	return infos, nil
}
