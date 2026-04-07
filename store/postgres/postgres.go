// Package postgres provides a PostgreSQL implementation of the lymbo.Store interface.
// It uses raw pgx queries without any code generation or migration tools.
//
// Usage:
//
//	pool, _ := pgxpool.New(ctx, dsn)
//	store := postgres.NewTicketsRepository(pool)
//	store.Migrate(ctx)  // Run migrations to set up schema
//
// For custom table names:
//
//	store := postgres.NewTicketsRepositoryWithConfig(postgres.Config{
//		TableName: "my_tickets",
//		Pool:      pool,
//	})
//	store.Migrate(ctx)
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/status"
)

type Config struct {
	TableName string
	Pool      *pgxpool.Pool
}

type Tickets struct {
	db        *pgxpool.Pool
	queries   *Queries
	tableName string
}

var _ lymbo.Store = &Tickets{}

func NewTicketsRepository(pool *pgxpool.Pool) *Tickets {
	t, err := NewTicketsRepositoryWithConfig(Config{
		TableName: "tickets",
		Pool:      pool,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to create tickets repository: %v", err))
	}
	return t
}

func NewTicketsRepositoryWithConfig(cfg Config) (*Tickets, error) {
	if cfg.TableName == "" {
		cfg.TableName = `tickets`
	}

	queries, err := newQueries(cfg.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize query templates: %w", err)
	}

	return &Tickets{
		db:        cfg.Pool,
		tableName: cfg.TableName,
		queries:   queries,
	}, nil
}

// Migrate runs the embedded migrations to set up the database schema
// All migrations are executed in a single transaction to ensure atomicity
func (r *Tickets) Migrate(ctx context.Context) error {
	slog.InfoContext(ctx, "Applying migration", "sql", r.queries.migrate)
	_, err := r.db.Exec(ctx, r.queries.migrate)
	if err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

func (r *Tickets) Get(ctx context.Context, id lymbo.TicketId) (lymbo.Ticket, error) {
	ticketUUID, err := uuid.Parse(id.String())
	if err != nil {
		return lymbo.Ticket{}, lymbo.ErrTicketIDInvalid
	}

	var (
		statusStr   string
		runat       pgtype.Timestamptz
		nice        int16
		ticketType  string
		ctime       pgtype.Timestamptz
		mtime       pgtype.Timestamptz
		attempts    int32
		payload     json.RawMessage
		errorReason json.RawMessage
	)

	err = r.db.QueryRow(ctx, r.queries.get, ticketUUID).Scan(
		&ticketUUID,
		&statusStr,
		&runat,
		&nice,
		&ticketType,
		&ctime,
		&mtime,
		&attempts,
		&payload,
		&errorReason,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return lymbo.Ticket{}, lymbo.ErrTicketNotFound
		}
		return lymbo.Ticket{}, err
	}

	s, err := status.FromString(statusStr)
	if err != nil {
		return lymbo.Ticket{}, err
	}

	var mtimePtr *time.Time
	if mtime.Valid {
		mtimePtr = &mtime.Time
	}

	return lymbo.Ticket{
		ID:          lymbo.TicketId(ticketUUID.String()),
		Status:      s,
		Runat:       runat.Time,
		Nice:        int(nice),
		Type:        ticketType,
		Ctime:       ctime.Time,
		Mtime:       mtimePtr,
		Attempts:    int(attempts),
		Payload:     payload,
		ErrorReason: errorReason,
	}, nil
}

func (r *Tickets) Put(ctx context.Context, ticket lymbo.Ticket) error {
	ticketUUID, err := uuid.Parse(ticket.ID.String())
	if err != nil {
		return lymbo.ErrTicketIDInvalid
	}

	var mtime pgtype.Timestamptz
	if ticket.Mtime != nil {
		mtime = pgtype.Timestamptz{Time: *ticket.Mtime, Valid: true}
	}
	tube := ticket.Tube.String()
	if tube == "" {
		tube = "default"
	}

	_, err = r.db.Exec(ctx, r.queries.put,
		ticketUUID,
		ticket.Status.String(),
		pgtype.Timestamptz{Time: ticket.Runat, Valid: true},
		int16(ticket.Nice),
		ticket.Type,
		pgtype.Timestamptz{Time: ticket.Ctime, Valid: true},
		mtime,
		int32(ticket.Attempts),
		ticket.Payload,
		ticket.ErrorReason,
		tube,
	)
	return err
}

func (r *Tickets) Delete(ctx context.Context, id lymbo.TicketId) error {
	ticketUUID, err := uuid.Parse(id.String())
	if err != nil {
		return lymbo.ErrTicketIDInvalid
	}

	_, err = r.db.Exec(ctx, r.queries.delete, ticketUUID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return err
}

func (r *Tickets) DeleteBatch(ctx context.Context, ids []lymbo.TicketId) error {
	if len(ids) == 0 {
		return nil
	}
	ticketUUIDs := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		ticketUUID, err := uuid.Parse(id.String())
		if err != nil {
			return lymbo.ErrTicketIDInvalid
		}
		ticketUUIDs = append(ticketUUIDs, ticketUUID)
	}

	batch := &pgx.Batch{}
	for _, ticketUUID := range ticketUUIDs {
		batch.Queue(r.queries.delete, ticketUUID)
	}

	br := r.db.SendBatch(ctx, batch)
	defer br.Close()

	if _, err := br.Exec(); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	return nil
}

func (r *Tickets) Update(ctx context.Context, id lymbo.TicketId, fn lymbo.UpdateFunc) error {
	ticketUUID, err := uuid.Parse(id.String())
	if err != nil {
		return lymbo.ErrTicketIDInvalid
	}

	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var (
		statusStr   string
		runat       pgtype.Timestamptz
		nice        int16
		ticketType  string
		ctime       pgtype.Timestamptz
		mtime       pgtype.Timestamptz
		attempts    int32
		payload     json.RawMessage
		errorReason json.RawMessage
	)

	err = tx.QueryRow(ctx, r.queries.get, ticketUUID).Scan(
		&ticketUUID,
		&statusStr,
		&runat,
		&nice,
		&ticketType,
		&ctime,
		&mtime,
		&attempts,
		&payload,
		&errorReason,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return lymbo.ErrTicketNotFound
		}
		return err
	}

	s, err := status.FromString(statusStr)
	if err != nil {
		return err
	}

	var mtimePtr *time.Time
	if mtime.Valid {
		mtimePtr = &mtime.Time
	}

	ticket := lymbo.Ticket{
		ID:          lymbo.TicketId(ticketUUID.String()),
		Status:      s,
		Runat:       runat.Time,
		Nice:        int(nice),
		Type:        ticketType,
		Ctime:       ctime.Time,
		Mtime:       mtimePtr,
		Attempts:    int(attempts),
		Payload:     payload,
		ErrorReason: errorReason,
	}

	if err := fn(ctx, &ticket); err != nil {
		return err
	}

	updatedPayload := ticket.Payload
	updatedErrorReason := ticket.ErrorReason

	var updatedMtime pgtype.Timestamptz
	if ticket.Mtime != nil {
		updatedMtime = pgtype.Timestamptz{Time: *ticket.Mtime, Valid: true}
	}

	_, err = tx.Exec(ctx, r.queries.put,
		ticketUUID,
		ticket.Status.String(),
		pgtype.Timestamptz{Time: ticket.Runat, Valid: true},
		int16(ticket.Nice),
		ticket.Type,
		pgtype.Timestamptz{Time: ticket.Ctime, Valid: true},
		updatedMtime,
		int32(ticket.Attempts),
		updatedPayload,
		updatedErrorReason,
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *Tickets) UpdateBatch(ctx context.Context, updates []lymbo.UpdateSet) error {
	if len(updates) == 0 {
		return nil
	}
	batch := &pgx.Batch{}

	for _, us := range updates {
		ticketUUID, err := uuid.Parse(us.Id.String())
		if err != nil {
			return lymbo.ErrTicketIDInvalid
		}

		var q string
		req := []any{ticketUUID}

		// status as  $2
		switch {
		case us.Status != nil:
			req = append(req, sql.NullString{String: us.Status.String(), Valid: true})
		default:
			req = append(req, sql.NullString{Valid: false})
		}

		// nice as $3
		switch {
		case us.Nice != nil:
			req = append(req, sql.NullInt16{Int16: int16(*us.Nice), Valid: true})
		default:
			req = append(req, sql.NullInt16{Valid: false})
		}

		// runat as $4, ...
		switch {
		case us.Backoff != nil:
			q = r.queries.backoff
			req = append(req,
				us.Backoff.Jitter.Seconds(),
				us.Backoff.Base,
				us.Backoff.MaxDelay.Seconds(),
			)
		case us.Runat != nil:
			q = r.queries.update
			req = append(req, sql.NullTime{Time: *us.Runat, Valid: true})
		default:
			// do nothing, just instant retry
			req = append(req, sql.NullTime{Valid: false})
		}

		// payload as $5
		switch {
		case us.Payload != nil:
			req = append(req, us.Payload)
		default:
			req = append(req, nil)
		}

		// error_reason as $6
		switch {
		case us.ErrorReason != nil:
			req = append(req, us.ErrorReason)
		default:
			req = append(req, nil)
		}

		switch {
		case us.Tube != nil:
			req = append(req, string(*us.Tube))
		default:
			req = append(req, nil)
		}

		batch.Queue(q, req...)
	}

	br := r.db.SendBatch(ctx, batch)
	defer br.Close()

	_, err := br.Exec()
	if err != nil {
		return err
	}

	return nil
}

type pollPendingParams struct {
	now         pgtype.Timestamptz
	ttr         int32
	maxDelay    int32
	backoffBase float64
	maxAttempts int32
	limit       int32
	tubes       []string
}

func clampMaxAttempts(maxDelay int32, backoffBase float64) int32 {
	if backoffBase <= 1 {
		return math.MaxInt32
	}
	return int32(math.Ceil(math.Log(float64(maxDelay)) / math.Log(backoffBase)))
}

func setTubes(tubes []lymbo.Tube) ([]string, error) {
	if len(tubes) == 0 {
		return []string{"default"}, nil
	}
	if len(tubes) > 10 {
		return nil, fmt.Errorf("too many tubes requested: %d (max 10)", len(tubes))
	}
	result := make([]string, len(tubes))
	for i, tube := range tubes {
		if tube == "" {
			return nil, fmt.Errorf("tube name cannot be empty (index %d)", i)
		}
		result[i] = string(tube)
	}
	return result, nil
}

func (r *Tickets) PollPending(ctx context.Context, req lymbo.PollRequest) (lymbo.PollResult, error) {
	maxAttempts := clampMaxAttempts(int32(req.MaxBackoffDelay.Seconds()), req.BackoffBase)

	dto := pollPendingParams{
		now:         pgtype.Timestamptz{Valid: true, Time: req.Now},
		ttr:         int32(req.TTR.Seconds()),
		maxDelay:    int32(req.MaxBackoffDelay.Seconds()),
		backoffBase: req.BackoffBase,
		maxAttempts: maxAttempts,
		limit:       int32(req.Limit),
	}

	var err error
	dto.tubes, err = setTubes(req.RequestTubes)
	if err != nil {
		return lymbo.PollResult{}, err
	}

	rows, err := r.db.Query(ctx, r.queries.poll,
		dto.now,         // $1
		dto.ttr,         // $2
		dto.maxDelay,    // $3
		dto.backoffBase, // $4
		dto.maxAttempts, // $5
		dto.limit,       // $6
		dto.tubes,       // $7
	)
	if err != nil {
		return lymbo.PollResult{}, err
	}
	defer rows.Close()

	tickets := make([]lymbo.Ticket, 0, req.Limit)

	for rows.Next() {
		var (
			id          uuid.UUID
			statusStr   string
			tube        string
			runat       pgtype.Timestamptz
			nice        int16
			ticketType  string
			ctime       pgtype.Timestamptz
			mtime       pgtype.Timestamptz
			attempts    int32
			payload     json.RawMessage
			errorReason json.RawMessage
		)

		err := rows.Scan(
			&id,
			&statusStr,
			&tube,
			&runat,
			&nice,
			&ticketType,
			&ctime,
			&mtime,
			&attempts,
			&payload,
			&errorReason,
		)
		if err != nil {
			return lymbo.PollResult{}, err
		}

		s, err := status.FromString(statusStr)
		if err != nil {
			slog.WarnContext(ctx, "failed to convert ticket in PollPending", "error", err, "ticket_id", id.String())
			continue
		}
		var mtimePtr *time.Time
		if mtime.Valid {
			mtimePtr = &mtime.Time
		}
		tickets = append(tickets, lymbo.Ticket{
			ID:          lymbo.TicketId(id.String()),
			Status:      s,
			Tube:        lymbo.Tube(tube),
			Runat:       runat.Time,
			Nice:        int(nice),
			Type:        ticketType,
			Ctime:       ctime.Time,
			Mtime:       mtimePtr,
			Attempts:    int(attempts),
			Payload:     payload,
			ErrorReason: errorReason,
		})
	}

	if err := rows.Err(); err != nil {
		return lymbo.PollResult{}, err
	}

	return lymbo.PollResult{
		Tickets: tickets,
	}, nil
}

func (r *Tickets) ExpireTickets(ctx context.Context, limit int, now time.Time) (int64, error) {
	res, err := r.db.Exec(ctx, r.queries.expire,
		pgtype.Timestamptz{Time: now, Valid: true},
		int32(limit),
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

// GetStatsByType returns ticket counts grouped by type and status.
func (r *Tickets) GetStatsByType(ctx context.Context) ([]lymbo.TypeStats, error) {
	rows, err := r.db.Query(ctx, r.queries.statsByType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make([]lymbo.TypeStats, 0)
	for rows.Next() {
		var s lymbo.TypeStats
		if err := rows.Scan(&s.Type, &s.Pending, &s.Done, &s.Failed, &s.Cancelled); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, rows.Err()
}
