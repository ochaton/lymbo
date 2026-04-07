package postgres

import (
	"bytes"
	"fmt"
	"text/template"
)

var migrate = template.Must(template.New("migrate").Parse(`-- name: Migrate:
BEGIN;
-- Create ticket_status enum if it doesn't exist
DO $$ BEGIN
	CREATE TYPE ticket_status AS ENUM ('pending', 'done', 'failed', 'cancelled');
EXCEPTION
	WHEN duplicate_object THEN null;
END $$;

-- Create table with parameterized name
CREATE TABLE IF NOT EXISTS {{.TableName}} (
	id           UUID PRIMARY KEY,
	status       ticket_status NOT NULL DEFAULT 'pending',
	runat        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
	nice         SMALLINT      NOT NULL DEFAULT 512,
	type         TEXT          NOT NULL,
	ctime        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
	mtime        TIMESTAMPTZ   NULL,
	attempts     INTEGER       NOT NULL DEFAULT 0,
	payload      JSONB         NULL,
	error_reason JSONB         NULL
);

-- Create index
CREATE INDEX IF NOT EXISTS idx_{{.TableName}}_pending_runat_nice ON {{.TableName}} (runat, nice)
WHERE status = 'pending';

-- Create trigger function
CREATE OR REPLACE FUNCTION {{.TableName}}_update_mtime()
RETURNS trigger AS $$
BEGIN
	IF ROW(NEW.status, NEW.runat)
	IS DISTINCT FROM
	ROW(OLD.status, OLD.runat)
	THEN
		NEW.mtime := NOW();
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
DROP TRIGGER IF EXISTS {{.TableName}}_update_mtime_trg ON {{.TableName}};
CREATE TRIGGER {{.TableName}}_update_mtime_trg
	BEFORE UPDATE ON {{.TableName}}
	FOR EACH ROW
	EXECUTE FUNCTION {{.TableName}}_update_mtime();

COMMIT;`))

var get = template.Must(template.New("get").Parse(`-- name: GetTicket:
SELECT id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason
FROM {{.TableName}}
WHERE id = $1;`))

var put = template.Must(template.New("put").Parse(`-- name: PutTicket:
INSERT INTO {{.TableName}} (id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (id) DO UPDATE SET
	status = EXCLUDED.status,
	runat = EXCLUDED.runat,
	nice = EXCLUDED.nice,
	type = EXCLUDED.type,
	ctime = EXCLUDED.ctime,
	mtime = EXCLUDED.mtime,
	attempts = EXCLUDED.attempts,
	payload = EXCLUDED.payload,
	error_reason = EXCLUDED.error_reason;`))

var delete = template.Must(template.New("delete").Parse(`-- name: DeleteTicket:
DELETE FROM {{.TableName}} WHERE id = $1`))

var update = template.Must(template.New("update").Parse(`UPDATE {{.TableName}}
SET
	status = COALESCE($2, status),
	nice = COALESCE($3, nice),
	runat = COALESCE($4, runat),
	payload = COALESCE($5, payload),
	error_reason = COALESCE($6, error_reason)
WHERE id = $1`))

// runat = now() + {jitter} + min(pow({base}, attempt), {max})
var backoff = template.Must(template.New("backoff").Parse(`-- name: BackoffTicket:
UPDATE {{.TableName}}
SET
	status = COALESCE($2, status),
	nice = COALESCE($3, nice),
	runat = now() + (GREATEST($4,0) + LEAST(POWER($5, attempts), $6)) * INTERVAL '1 second',
	payload = COALESCE($7, payload),
	error_reason = COALESCE($8, error_reason)
WHERE id = $1`))

// runat = now() + max(ttr, 0) + min(maxDelay, backoffBase^min(t.attempts, maxAttempts))
var poll = template.Must(template.New("poll").Parse(`-- name: PollTickets:
WITH rescheduled_tickets AS (
	UPDATE {{.TableName}} as t
	SET
		attempts = attempts + 1,
		runat = $1::Timestamptz +
			(GREATEST($2, 0) + LEAST($3, POWER($4, LEAST(t.attempts, $5))))
			* INTERVAL '1 second'
	WHERE id IN (
		SELECT t.id
		FROM {{.TableName}} as t
		WHERE t.status = 'pending' AND t.runat <= $1::Timestamptz
		LIMIT $6
		FOR UPDATE SKIP LOCKED
	)
	RETURNING id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason
),
future_ticket AS (
	SELECT ft.id, ft.status, ft.runat, ft.nice, ft.type, ft.ctime, ft.mtime, ft.attempts, ft.payload, ft.error_reason
	FROM {{.TableName}} as ft
	WHERE status = 'pending'
	ORDER BY ft.runat ASC, ft.nice ASC
	LIMIT 1
	FOR SHARE SKIP LOCKED
)
SELECT
	'ticket' AS ticket,
	rescheduled_tickets.id           AS id,
	rescheduled_tickets.status       AS status,
	rescheduled_tickets.runat        AS runat,
	rescheduled_tickets.nice         AS nice,
	rescheduled_tickets.type         AS type,
	rescheduled_tickets.ctime        AS ctime,
	rescheduled_tickets.mtime        AS mtime,
	rescheduled_tickets.attempts     AS attempts,
	rescheduled_tickets.payload      AS payload,
	rescheduled_tickets.error_reason AS error_reason
FROM rescheduled_tickets
UNION ALL
SELECT
	'future_ticket'            AS ticket,
	future_ticket.id           AS id,
	future_ticket.status       AS status,
	future_ticket.runat        AS runat,
	future_ticket.nice         AS nice,
	future_ticket.type         AS type,
	future_ticket.ctime        AS ctime,
	future_ticket.mtime        AS mtime,
	future_ticket.attempts     AS attempts,
	future_ticket.payload      AS payload,
	future_ticket.error_reason AS error_reason
FROM future_ticket
WHERE NOT EXISTS (SELECT 1 FROM rescheduled_tickets);`))

var expire = template.Must(template.New("expire").Parse(`-- ExpireTickets:
DELETE FROM {{.TableName}}
WHERE id IN (SELECT id FROM {{.TableName}} as t WHERE t.status != 'pending' AND t.runat <= $1 LIMIT $2);`))

type Queries struct {
	migrate string
	get     string
	put     string
	delete  string
	update  string
	backoff string
	poll    string
	expire  string
}

func newQueries(tableName string) (*Queries, error) {
	// tableName = pgx.Identifier([]string{tableName}).Sanitize()
	args := struct{ TableName string }{TableName: tableName}

	exec := func(tmpl *template.Template) (string, error) {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, args); err != nil {
			return "", err
		}
		return buf.String(), nil
	}

	qt := &Queries{}
	var err error

	if qt.migrate, err = exec(migrate); err != nil {
		return nil, fmt.Errorf("failed to execute template `migrate`: %w", err)
	}
	if qt.get, err = exec(get); err != nil {
		return nil, fmt.Errorf("failed to execute template `get`: %w", err)
	}
	if qt.put, err = exec(put); err != nil {
		return nil, fmt.Errorf("failed to execute template `put`: %w", err)
	}
	if qt.delete, err = exec(delete); err != nil {
		return nil, fmt.Errorf("failed to execute template `delete`: %w", err)
	}
	if qt.update, err = exec(update); err != nil {
		return nil, fmt.Errorf("failed to execute template `update`: %w", err)
	}
	if qt.poll, err = exec(poll); err != nil {
		return nil, fmt.Errorf("failed to execute template `poll`: %w", err)
	}
	if qt.expire, err = exec(expire); err != nil {
		return nil, fmt.Errorf("failed to execute template `expire`: %w", err)
	}
	if qt.backoff, err = exec(backoff); err != nil {
		return nil, fmt.Errorf("failed to execute template `backoff`: %w", err)
	}
	return qt, nil
}
