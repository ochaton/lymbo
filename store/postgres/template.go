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

ALTER TABLE {{.TableName}} ADD COLUMN IF NOT EXISTS tube TEXT NOT NULL DEFAULT 'default';
ALTER TABLE {{.TableName}} ADD COLUMN IF NOT EXISTS group_id TEXT DEFAULT NULL;

CREATE INDEX IF NOT EXISTS idx_{{.TableName}}_group_pending ON {{.TableName}} (group_id)
WHERE group_id IS NOT NULL AND status = 'pending';

CREATE TABLE IF NOT EXISTS {{.TableName}}_deps (
    ticket_id  UUID NOT NULL REFERENCES {{.TableName}}(id),
    blocked_by UUID NOT NULL REFERENCES {{.TableName}}(id),
    PRIMARY KEY (ticket_id, blocked_by)
);
CREATE INDEX IF NOT EXISTS idx_{{.TableName}}_deps_blocked_by ON {{.TableName}}_deps (blocked_by);

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
INSERT INTO {{.TableName}} (id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason, tube, group_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (id) DO UPDATE SET
	status = EXCLUDED.status,
	runat = EXCLUDED.runat,
	nice = EXCLUDED.nice,
	type = EXCLUDED.type,
	ctime = EXCLUDED.ctime,
	mtime = EXCLUDED.mtime,
	attempts = EXCLUDED.attempts,
	payload = EXCLUDED.payload,
	error_reason = EXCLUDED.error_reason,
	tube = EXCLUDED.tube;`))

var delete = template.Must(template.New("delete").Parse(`-- name: DeleteTicket:
DELETE FROM {{.TableName}} WHERE id = $1 RETURNING id, type, tube`))

var update = template.Must(template.New("update").Parse(`WITH del_deps AS (
	DELETE FROM {{.TableName}}_deps
	WHERE blocked_by = $1
	  AND $2 IN ('done'::ticket_status, 'failed'::ticket_status, 'cancelled'::ticket_status)
)
UPDATE {{.TableName}}
SET
	status = COALESCE($2, status),
	nice = COALESCE($3, nice),
	runat = COALESCE($4, runat),
	payload = COALESCE($5, payload),
	error_reason = COALESCE($6, error_reason),
	tube = COALESCE($7, tube),
	group_id = COALESCE($8, group_id)
WHERE id = $1
RETURNING id, type, tube, status`))

// runat = now() + {jitter} + min(pow({base}, attempt), {max})
var backoff = template.Must(template.New("backoff").Parse(`-- name: BackoffTicket:
WITH del_deps AS (
	DELETE FROM {{.TableName}}_deps
	WHERE blocked_by = $1
	  AND $2 IN ('done'::ticket_status, 'failed'::ticket_status, 'cancelled'::ticket_status)
)
UPDATE {{.TableName}}
SET
	status = COALESCE($2, status),
	nice = COALESCE($3, nice),
	runat = now() + (GREATEST($4,0) + LEAST(POWER($5, attempts), $6)) * INTERVAL '1 second',
	payload = COALESCE($7, payload),
	error_reason = COALESCE($8, error_reason),
	tube = COALESCE($9, tube),
	group_id = COALESCE($10, group_id)
WHERE id = $1
RETURNING id, type, tube, status`))

// runat = now() + max(ttr, 0) + min(maxDelay, backoffBase^min(t.attempts, maxAttempts))
var poll = template.Must(template.New("poll").Parse(`-- name: PollTickets:
WITH candidates AS (
	SELECT t.id, t.runat AS ready_at
	FROM {{.TableName}} AS t
	WHERE t.status = 'pending'
	  AND t.runat <= $1::Timestamptz
	  AND t.tube = ANY($7::text[])
	  AND NOT EXISTS (
	      SELECT 1 FROM {{.TableName}}_deps d WHERE d.ticket_id = t.id LIMIT 1
	  )
	LIMIT $6
	FOR UPDATE SKIP LOCKED
)
UPDATE {{.TableName}} AS t
SET
	attempts = attempts + 1,
	runat = $1::Timestamptz +
		(GREATEST($2, 0) + LEAST($3, POWER($4, LEAST(t.attempts, $5))))
		* INTERVAL '1 second'
FROM candidates c
WHERE t.id = c.id
RETURNING
	t.id, t.status, t.tube, t.runat, t.nice, t.type,
	t.ctime, t.mtime, t.attempts, t.payload, t.error_reason,
	c.ready_at;`))

var putAfterGroup = template.Must(template.New("putAfterGroup").Parse(`-- name: PutAfterGroup:
WITH inserted AS (
	INSERT INTO {{.TableName}} (id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason, tube, group_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	ON CONFLICT (id) DO NOTHING
	RETURNING id
)
INSERT INTO {{.TableName}}_deps (ticket_id, blocked_by)
SELECT $1, t.id
FROM {{.TableName}} t
WHERE t.group_id = $13 AND t.status = 'pending' AND t.id != $1
  AND EXISTS (SELECT 1 FROM inserted)
ON CONFLICT DO NOTHING;`))

var deleteBatch = template.Must(template.New("deleteBatch").Parse(`-- name: DeleteBatch:
WITH del_deps AS (
	DELETE FROM {{.TableName}}_deps
	WHERE ticket_id = ANY($1::uuid[]) OR blocked_by = ANY($1::uuid[])
)
DELETE FROM {{.TableName}} WHERE id = ANY($1::uuid[]) RETURNING id, type, tube`))

var countPendingInGroup = template.Must(template.New("countPendingInGroup").Parse(`-- name: CountPendingInGroup:
SELECT COUNT(*) FROM {{.TableName}}
WHERE group_id = $1 AND status = 'pending';`))

var expire = template.Must(template.New("expire").Parse(`-- name: ExpireTickets:
DELETE FROM {{.TableName}}
WHERE id IN (SELECT id FROM {{.TableName}} as t WHERE t.status != 'pending' AND t.runat <= $1 LIMIT $2)
RETURNING id, type, tube;`))

type Queries struct {
	migrate             string
	get                 string
	put                 string
	delete              string
	deleteBatch         string
	putAfterGroup       string
	update              string
	backoff             string
	poll                string
	expire              string
	countPendingInGroup string
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
	if qt.deleteBatch, err = exec(deleteBatch); err != nil {
		return nil, fmt.Errorf("failed to execute template `deleteBatch`: %w", err)
	}
	if qt.putAfterGroup, err = exec(putAfterGroup); err != nil {
		return nil, fmt.Errorf("failed to execute template `putAfterGroup`: %w", err)
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
	if qt.countPendingInGroup, err = exec(countPendingInGroup); err != nil {
		return nil, fmt.Errorf("failed to execute template `countPendingInGroup`: %w", err)
	}
	return qt, nil
}
