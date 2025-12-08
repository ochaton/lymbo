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
		payload     []byte
		errorReason []byte
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

	var payload []byte
	if ticket.Payload != nil {
		payload, err = json.Marshal(ticket.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal payload: %w", err)
		}
	}

	var errorReason []byte
	if ticket.ErrorReason != nil {
		errorReason, err = json.Marshal(ticket.ErrorReason)
		if err != nil {
			return fmt.Errorf("failed to marshal error_reason: %w", err)
		}
	}

	var mtime pgtype.Timestamptz
	if ticket.Mtime != nil {
		mtime = pgtype.Timestamptz{Time: *ticket.Mtime, Valid: true}
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
		payload,
		errorReason,
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
		payload     []byte
		errorReason []byte
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

	// Re-marshal payload and error_reason
	var updatedPayload []byte
	if ticket.Payload != nil {
		updatedPayload, err = json.Marshal(ticket.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal payload: %w", err)
		}
	}

	var updatedErrorReason []byte
	if ticket.ErrorReason != nil {
		updatedErrorReason, err = json.Marshal(ticket.ErrorReason)
		if err != nil {
			return fmt.Errorf("failed to marshal error_reason: %w", err)
		}
	}

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

type updateSetParams struct {
	id           uuid.UUID      // $1
	status       sql.NullString // $2
	nice         sql.NullInt16  // $3
	runat        sql.NullTime   // $4
	payload      []byte         // $5
	error_reason []byte         // $6
}

func updateOne(tid uuid.UUID, us lymbo.UpdateSet) (*updateSetParams, error) {
	usp := &updateSetParams{
		id: tid,
	}

	if us.Status != nil {
		usp.status = sql.NullString{String: us.Status.String(), Valid: true}
	}
	if us.Nice != nil {
		usp.nice = sql.NullInt16{Int16: int16(*us.Nice), Valid: true}
	}
	if us.Runat != nil {
		usp.runat = sql.NullTime{Time: *us.Runat, Valid: true}
	}
	if us.Payload != nil {
		payload, err := json.Marshal(us.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %w", err)
		}
		usp.payload = payload
	}
	if us.ErrorReason != nil {
		errorReason, err := json.Marshal(us.ErrorReason)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal error_reason: %w", err)
		}
		usp.error_reason = errorReason
	}
	return usp, nil
}

func (r *Tickets) UpdateSet(ctx context.Context, us lymbo.UpdateSet) error {
	ticketUUID, err := uuid.Parse(us.Id.String())
	if err != nil {
		return lymbo.ErrTicketIDInvalid
	}

	usp, err := updateOne(ticketUUID, us)
	if err != nil {
		return err
	}

	_, err = r.db.Exec(ctx, r.queries.update,
		usp.id,
		usp.status,
		usp.nice,
		usp.runat,
		usp.payload,
		usp.error_reason,
	)
	return err
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

		usp, err := updateOne(ticketUUID, us)
		if err != nil {
			return err
		}

		batch.Queue(r.queries.update,
			usp.id,
			usp.status,
			usp.nice,
			usp.runat,
			usp.payload,
			usp.error_reason,
		)
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
	limit       int32
}

func (r *Tickets) PollPending(ctx context.Context, req lymbo.PollRequest) (lymbo.PollResult, error) {
	dto := pollPendingParams{
		now:         pgtype.Timestamptz{Valid: true, Time: req.Now},
		ttr:         int32(req.TTR.Seconds()),
		maxDelay:    int32(req.MaxBackoffDelay.Seconds()),
		backoffBase: req.BackoffBase,
		limit:       int32(req.Limit),
	}
	rows, err := r.db.Query(ctx, r.queries.poll,
		dto.now,
		dto.ttr,
		dto.maxDelay,
		dto.backoffBase,
		dto.limit,
	)
	if err != nil {
		return lymbo.PollResult{}, err
	}
	defer rows.Close()

	var sleepUntil *time.Time
	tickets := make([]lymbo.Ticket, 0)

	for rows.Next() {
		var (
			rowType     string
			id          uuid.UUID
			statusStr   string
			runat       pgtype.Timestamptz
			nice        int16
			ticketType  string
			ctime       pgtype.Timestamptz
			mtime       pgtype.Timestamptz
			attempts    int32
			payload     []byte
			errorReason []byte
		)

		err := rows.Scan(
			&rowType,
			&id,
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
			return lymbo.PollResult{}, err
		}

		switch rowType {
		case "ticket":
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
				Runat:       runat.Time,
				Nice:        int(nice),
				Type:        ticketType,
				Ctime:       ctime.Time,
				Mtime:       mtimePtr,
				Attempts:    int(attempts),
				Payload:     payload,
				ErrorReason: errorReason,
			})
		case "future_ticket":
			sleepUntil = &runat.Time
		default:
			slog.WarnContext(ctx, "unknown row type PollPending", "row_type", rowType, "ticket_id", id.String())
		}
	}

	if err := rows.Err(); err != nil {
		return lymbo.PollResult{}, err
	}

	return lymbo.PollResult{
		SleepUntil: sleepUntil,
		Tickets:    tickets,
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
