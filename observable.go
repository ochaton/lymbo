package lymbo

import (
	"context"
	"time"

	"github.com/ochaton/lymbo/stats"
)

// observableStore wraps a Store and feeds per-(type, tube) stats
// for operations not routed through the pusher (Put, ExpireTickets).
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
	o.stats.ByKey(t.Type, t.Tube.String()).Inc(stats.Add)
	return nil
}

func (o *observableStore) PutAfterGroup(ctx context.Context, t Ticket, groupID string) error {
	if err := o.Store.PutAfterGroup(ctx, t, groupID); err != nil {
		return err
	}
	o.stats.ByKey(t.Type, t.Tube.String()).Inc(stats.Add)
	return nil
}

func (o *observableStore) ExpireTickets(ctx context.Context, limit int, now time.Time) ([]TransitionInfo, error) {
	infos, err := o.Store.ExpireTickets(ctx, limit, now)
	if err != nil {
		return infos, err
	}
	for _, info := range infos {
		o.stats.ByKey(info.Type, string(info.Tube)).Inc(stats.Expire)
	}
	return infos, nil
}
