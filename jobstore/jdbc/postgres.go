package jdbc

import (
	"fmt"
)

// pgColumns is the column list for PostgreSQL SELECT queries.
const pgColumns = `job_id, name, trigger_type, trigger_value, metadata,
		        next_fire_time, state, instance_id, acquired_at, enabled, created_at, updated_at`

// Postgres implements Dialect for PostgreSQL.
type Postgres struct{}

func (Postgres) Placeholder(index int) string { return fmt.Sprintf("$%d", index) }
func (Postgres) Columns() string              { return pgColumns }
func (Postgres) BooleanTrue() string          { return "TRUE" }
func (Postgres) Col(name string) string       { return name }

func (Postgres) SchemaSQL(prefix string) string {
	return `
CREATE TABLE IF NOT EXISTS ` + prefix + `scheduler_jobs (
    job_id         TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    trigger_type   TEXT NOT NULL,
    trigger_value  TEXT NOT NULL,
    metadata       JSONB DEFAULT '{}',
    next_fire_time TIMESTAMPTZ,
    state          TEXT NOT NULL DEFAULT 'WAITING',
    instance_id    TEXT,
    acquired_at    TIMESTAMPTZ,
    enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_` + prefix + `sched_jobs_fire
    ON ` + prefix + `scheduler_jobs (next_fire_time)
    WHERE state = 'WAITING' AND enabled = TRUE;

CREATE TABLE IF NOT EXISTS ` + prefix + `scheduler_locks (
    lock_name TEXT PRIMARY KEY
);

INSERT INTO ` + prefix + `scheduler_locks (lock_name) VALUES ('TRIGGER_ACCESS') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS ` + prefix + `scheduler_executions (
    id          BIGSERIAL PRIMARY KEY,
    job_id      TEXT NOT NULL,
    instance_id TEXT,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    error       TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_` + prefix + `sched_exec_job
    ON ` + prefix + `scheduler_executions (job_id, started_at DESC);
`
}

func (Postgres) UpsertJobSQL(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (job_id, name, trigger_type, trigger_value, metadata,
		                next_fire_time, state, enabled, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (job_id) DO UPDATE SET
			name = EXCLUDED.name,
			trigger_type = EXCLUDED.trigger_type,
			trigger_value = EXCLUDED.trigger_value,
			metadata = EXCLUDED.metadata,
			next_fire_time = EXCLUDED.next_fire_time,
			enabled = EXCLUDED.enabled,
			updated_at = EXCLUDED.updated_at
	`, table)
}

func (Postgres) LockRowSQL(table string) string {
	return fmt.Sprintf("SELECT lock_name FROM %s WHERE lock_name = $1 FOR UPDATE NOWAIT", table)
}

func (Postgres) IsLockNotAvailable(err error) bool {
	return containsString(err, "55P03") ||
		containsString(err, "could not obtain lock")
}
